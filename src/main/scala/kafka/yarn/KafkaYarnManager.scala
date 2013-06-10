package kafka.yarn

import org.apache.hadoop.conf._
import org.apache.hadoop.net._
import org.apache.hadoop.yarn.api._
import org.apache.hadoop.yarn.api.protocolrecords._
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.conf._
import org.apache.hadoop.yarn.ipc._
import org.apache.hadoop.yarn.util._
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.util.Tool
import org.apache.hadoop.util.ToolRunner

class KafkaYarnManager(conf: Configuration = new Configuration) extends Configured(conf) with Tool {

   var containerMemory = 10

  /**
   * @param args
   */
  def run(args: Array[String]) = {
    import KafkaYarnManager._
    val rpc = YarnRPC.create(conf)

    // Get containerId
    val containerId = ConverterUtils.toContainerId(
      sys.env.getOrElse(ApplicationConstants.AM_CONTAINER_ID_ENV,
        throw new IllegalArgumentException("ContainerId not set in the environment")))
    val appAttemptId = containerId.getApplicationAttemptId

    // Connect to the Scheduler of the ResourceManager
    val yarnConf = new YarnConfiguration(conf)
    val rmAddress = NetUtils.createSocketAddr(
      yarnConf.get(YarnConfiguration.RM_SCHEDULER_ADDRESS, YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS))
    val resourceManager = rpc.getProxy(classOf[AMRMProtocol], rmAddress, conf).asInstanceOf[AMRMProtocol]

    // Register the AM with the RM
    val appMasterRequest = Records.newRecord(classOf[RegisterApplicationMasterRequest])
    appMasterRequest.setApplicationAttemptId(appAttemptId)
    //    appMasterRequest.setHost(appMasterHostname)
    //    appMasterRequest.setRpcPort(appMasterRpcPort)
    //    appMasterRequest.setTrackingUrl(appMasterTrackingUrl)
    val response = resourceManager.registerApplicationMaster(appMasterRequest)
        // Dump out information about cluster capability as seen by the resource manager
    val minMem = response.getMinimumResourceCapability().getMemory()
    val maxMem = response.getMaximumResourceCapability().getMemory()
    LOG.info("Min mem capabililty of resources in this cluster " + minMem);
    LOG.info("Max mem capabililty of resources in this cluster " + maxMem);

    // A resource ask has to be atleast the minimum of the capability of the cluster, the value has to be
    // a multiple of the min value and cannot exceed the max.
    // If it is not an exact multiple of min, the RM will allocate to the nearest multiple of min
    if (containerMemory < minMem) {
      LOG.info("Container memory specified below min threshold of cluster. Using min value."
          + ", specified=" + containerMemory
          + ", min=" + minMem);
      containerMemory = minMem;
    }
    else if (containerMemory > maxMem) {
      LOG.info("Container memory specified above max threshold of cluster. Using max value."
          + ", specified=" + containerMemory
          + ", max=" + maxMem);
      containerMemory = maxMem;
    }


    // Finish
    val finishRequest = Records.newRecord(classOf[FinishApplicationMasterRequest])
    finishRequest.setAppAttemptId(appAttemptId)
    finishRequest.setFinishApplicationStatus(FinalApplicationStatus.SUCCEEDED)
    resourceManager.finishApplicationMaster(finishRequest)
    if (finishRequest.getFinalApplicationStatus() == FinalApplicationStatus.SUCCEEDED) 0 else 1

  }
}

object KafkaYarnManager {

  val LOG = LogFactory.getLog(classOf[KafkaYarnManager])

  val ApplicationName = "KafkaYarnManager"

  /**
   * @param args
   */
  def main(args: Array[String]) {
    sys.exit(ToolRunner.run(new KafkaYarnManager, args))
  }

}
