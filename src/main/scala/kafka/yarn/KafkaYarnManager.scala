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
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.JavaConversions._
import java.util.concurrent.CopyOnWriteArrayList
import java.net.URISyntaxException
import java.util.HashMap
import java.net.URI
import org.apache.hadoop.yarn.exceptions.YarnRemoteException
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.yarn.client.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.client.AMRMClientImpl

class KafkaYarnManager(conf: Configuration = new Configuration) extends Configured(conf) with Tool {
  import KafkaYarnManager._
  var appDone = false
  var containerMemory = 1024
  var arg = ""
  val fs = FileSystem.get(conf)
  val commandEnv = Map[String, String]()
  val requestPriority = 0
  val rmRequestID = new AtomicInteger()
  val numCompletedContainers = new AtomicInteger()
  val numAllocatedContainers = new AtomicInteger()
  val numFailedContainers = new AtomicInteger()
  val numRequestedContainers = new AtomicInteger()
  val numTotalContainers = 1
  var launchThreads = collection.mutable.Seq[Thread]()
  var appAttemptID = Records.newRecord(classOf[ApplicationAttemptId])
  val releasedContainers = new CopyOnWriteArrayList[ContainerId]()
  val appMasterHostname = ""
  // Port on which the app master listens for status updates from clients
  val appMasterRpcPort = 0
  // Tracking url to which app master publishes info for clients to monitor
  val appMasterTrackingUrl = ""

  class LaunchContainer(container: Container, var cm: ContainerManager, config: KafkaYarnConfig) extends Runnable {

    @Override 
    /**
     * Connects to CM, sets up container launch context
     * for kafka command and eventually dispatches the container
     * start request to the CM.
     */
    def run() {
      LOG.info("Setting up container launch container for containerid=" + container.getId());
      val ctx = Records.newRecord(classOf[ContainerLaunchContext]);

      ctx.setContainerId(container.getId());
      ctx.setResource(container.getResource());

      val jobUserName = System.getenv(ApplicationConstants.Environment.USER.name());
      ctx.setUser(jobUserName);
      LOG.info("Setting user in ContainerLaunchContext to: " + jobUserName);

      // Set the environment
      ctx.setEnvironment(commandEnv);

      val command: List[String] = List(
      "service",
      "kafka",
      config,
      "1>/users/kamkasravi/commandstdout",
      "2>/users/kamkasravi/commandstderr")
//      "1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + Path.SEPARATOR + ApplicationConstants.STDOUT,
//      "2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + Path.SEPARATOR + ApplicationConstants.STDERR)

      ctx.setCommands(command);

      val startReq = Records.newRecord(classOf[StartContainerRequest]);
      startReq.setContainerLaunchContext(ctx);
      try {
        cm.startContainer(startReq);
      } catch {
        case e: YarnRemoteException =>
          LOG.info("Start container failed for :"
            + ", containerId=" + container.getId());
          e.printStackTrace();
        // TODO do we need to release this container?
      }

    }
  }

  /**
   * Helper function to connect to CM
   */
  def connectToCM(container: Container, rpc: YarnRPC): ContainerManager = {
    LOG.debug("Connecting to ContainerManager for containerid=" + container.getId())
    val cmIpPortStr = container.getNodeId().getHost() + ":" + container.getNodeId().getPort()
    val cmAddress = NetUtils.createSocketAddr(cmIpPortStr)
    LOG.info("Connecting to ContainerManager at " + cmIpPortStr)
    rpc.getProxy(classOf[ContainerManager], cmAddress, conf).asInstanceOf[ContainerManager];
  }

  def setupContainerAskForRM(numContainers: Int): ContainerRequest = {
    // setup requirements for hosts
    // using * as any host will do for the distributed
    // set the priority for the request
    val pri = Records.newRecord(classOf[Priority]);
    // TODO - what is the range for priority? how to decide?
    pri.setPriority(requestPriority);

    // Set up resource type requirements
    // For now, only memory is supported so we set memory requirements
    val capability = Records.newRecord(classOf[Resource]);
    capability.setMemory(containerMemory);

    val request = new ContainerRequest(capability, null, null, pri, numContainers);
    LOG.info("Requested container ask: " + request.toString());
    return request;
  }

  /**
   * Ask RM to allocate given no. of containers to this Application Master
   * @param requestedContainers Containers to ask for from RM
   * @return Response from RM to AM with allocated containers
   * @throws YarnRemoteException
   */
  def sendContainerAskToRM(resourceManager: AMRMClientImpl): AMResponse = {
    val progressIndicator = numCompletedContainers.get() / numTotalContainers;

    LOG.info("Sending request to RM for containers" + ", progress="
        + progressIndicator);

    val resp = resourceManager.allocate(progressIndicator);
    return resp.getAMResponse();
  }

  def addResource(localResources: Map[String, LocalResource], url: String, name: String, rType: LocalResourceType): Unit = {
    val path = new Path(url);
    val status = fs.getFileStatus(path);
    System.out.println(name + " size: " + status.getLen());

    val resource = Records.newRecord(classOf[LocalResource]);
    resource.setType(rType);
    resource.setVisibility(LocalResourceVisibility.APPLICATION);
    resource.setResource(ConverterUtils.getYarnUrlFromPath(path));
    resource.setTimestamp(status.getModificationTime());
    resource.setSize(status.getLen());
    localResources.put(name, resource);
  }

  /**
   * @param args
   */
  def run(args: Array[String]) = {
    var config: KafkaYarnConfig = KafkaYarnConfig(args)
    val zk = KafkaYarnZookeeper(config.zookeeper.get("host").get+":"+config.zookeeper.get("port").get)
    val rpc = YarnRPC.create(conf)
    // Get containerId
    val containerId = ConverterUtils.toContainerId(
      sys.env.getOrElse(ApplicationConstants.AM_CONTAINER_ID_ENV,
        throw new IllegalArgumentException("ContainerId not set in the environment")))
    appAttemptID = containerId.getApplicationAttemptId

    // Connect to the Scheduler of the ResourceManager
    val yarnConf = new YarnConfiguration(conf)
    val rmAddress = NetUtils.createSocketAddr(
      yarnConf.get(YarnConfiguration.RM_SCHEDULER_ADDRESS, YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS))

    val resourceManager = new AMRMClientImpl(appAttemptID);
    resourceManager.init(conf);
    resourceManager.start();
    var isSuccess = 0;
 
    try {
      // Register the AM with the RM
      val appMasterRequest = Records.newRecord(classOf[RegisterApplicationMasterRequest])
      appMasterRequest.setApplicationAttemptId(appAttemptID)
      //    appMasterRequest.setHost(appMasterHostname)
      //    appMasterRequest.setRpcPort(appMasterRpcPort)
      //    appMasterRequest.setTrackingUrl(appMasterTrackingUrl)
      val response = resourceManager.registerApplicationMaster(appMasterHostname, appMasterRpcPort, appMasterTrackingUrl)
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
      } else if (containerMemory > maxMem) {
        LOG.info("Container memory specified above max threshold of cluster. Using max value."
          + ", specified=" + containerMemory
          + ", max=" + maxMem);
        containerMemory = maxMem;
      }

      var loopCounter = 0;
      while (numCompletedContainers.get() < numTotalContainers && !appDone) {
        loopCounter += 1
        LOG.info("Current application state: loop=" + loopCounter
          + ", appDone=" + appDone + ", total=" + numTotalContainers
          + ", requested=" + numRequestedContainers + ", completed="
          + numCompletedContainers + ", failed=" + numFailedContainers
          + ", currentAllocated=" + numAllocatedContainers);

        try {
          Thread.sleep(1000);
        } catch {
          case e => LOG.info("Sleep interrupted " + e.getMessage())
        }

        val askCount = numTotalContainers - numRequestedContainers.get()
        numRequestedContainers.addAndGet(askCount)

        // Setup request to be sent to RM to allocate containers
        val containerAsk = setupContainerAskForRM(askCount)
        resourceManager.addContainerRequest(containerAsk)

        // Send the request to RM
        LOG.info("Asking RM for containers" + ", askCount=" + askCount);
        val amResp = sendContainerAskToRM(resourceManager)

        // Retrieve list of allocated containers from the response
        val allocatedContainers = amResp.getAllocatedContainers()
        LOG.info("Got response from RM for container ask, allocatedCnt=" + allocatedContainers.size())
        numAllocatedContainers.addAndGet(allocatedContainers.size());
        for (allocatedContainer <- allocatedContainers) {
          LOG.info("Launching command on a new container."
            + ", containerId=" + allocatedContainer.getId()
            + ", containerNode=" + allocatedContainer.getNodeId().getHost()
            + ":" + allocatedContainer.getNodeId().getPort()
            + ", containerNodeURI=" + allocatedContainer.getNodeHttpAddress()
            + ", containerState" + allocatedContainer.getState()
            + ", containerResourceMemory" + allocatedContainer.getResource().getMemory());

          val runnableLaunchContainer = new LaunchContainer(allocatedContainer, connectToCM(allocatedContainer, rpc), config);
          val launchThread = new Thread(runnableLaunchContainer);

          // launch and start the container on a separate thread to keep the main thread unblocked
          // as all containers may not be allocated at one go.
          launchThreads = launchThreads :+ launchThread
          launchThread.start();
        }
        
        // Check what the current available resources in the cluster are
        // TODO should we do anything if the available resources are not enough?
        val availableResources = amResp.getAvailableResources();
        LOG.info("Current available resources in the cluster " + availableResources);

        // Check the completed containers
        val completedContainers = amResp.getCompletedContainersStatuses();
        LOG.info("Got response from RM for container ask, completedCnt="+ completedContainers.size());
        for (containerStatus <- completedContainers) {
          LOG.info("Got container status for containerID="
              + containerStatus.getContainerId() + ", state="
              + containerStatus.getState() + ", exitStatus="
              + containerStatus.getExitStatus() + ", diagnostics="
              + containerStatus.getDiagnostics());

          // non complete containers should not be here
          assert (containerStatus.getState() == ContainerState.COMPLETE);

          // increment counters for completed/failed containers
          val exitStatus = containerStatus.getExitStatus();
          if (0 != exitStatus) {
            // container failed
            if (-100 != exitStatus) {
              // container failed
              // counts as completed
              numCompletedContainers.incrementAndGet();
              numFailedContainers.incrementAndGet();
            } else {
              // something else bad happened
              // app job did not complete for some reason
              // we should re-try as the container was lost for some reason
              numAllocatedContainers.decrementAndGet();
              numRequestedContainers.decrementAndGet();
              // we do not need to release the container as it would be done
              // by the RM/CM.
            }
          } else {
            // nothing to do
            // container completed successfully
            numCompletedContainers.incrementAndGet();
            LOG.info("Container completed successfully." + ", containerId="
                + containerStatus.getContainerId());
          }
                  }
        if (numCompletedContainers.get() == numTotalContainers) {
          appDone = true;
        }

        LOG.info("Current application state: loop=" + loopCounter
            + ", appDone=" + appDone + ", total=" + numTotalContainers
            + ", requested=" + numRequestedContainers + ", completed="
            + numCompletedContainers + ", failed=" + numFailedContainers
            + ", currentAllocated=" + numAllocatedContainers);

        // TODO
        // Add a timeout handling layer
        // for misbehaving commands
      }

      // Join all launched threads
      // needed for when we time out
      // and we need to release containers
      for (launchThread <- launchThreads) {
        try {
          launchThread.join(10000);
        } catch  {
          case e =>
          LOG.info("Exception thrown in thread join: " + e.getMessage());
          e.printStackTrace();
        }
      }

      // When the application completes, it should send a finish application
      // signal to the RM
      LOG.info("Application completed. Signalling finish to RM");

      var appStatus = FinalApplicationStatus.SUCCEEDED;
      var appMessage = "";
      if (numFailedContainers.get() == 0) {
        appStatus = FinalApplicationStatus.SUCCEEDED;
      } else {
        appStatus = FinalApplicationStatus.FAILED;
        appMessage = "Diagnostics." + ", total=" + numTotalContainers + ", completed=" + numCompletedContainers.get() + ", allocated="
            + numAllocatedContainers.get() + ", failed="
            + numFailedContainers.get();
        isSuccess = 1;
      }
      resourceManager.unregisterApplicationMaster(appStatus, appMessage, null);
    } finally {
      resourceManager.stop();
    }
    (isSuccess)

  }
}

object KafkaYarnManager extends App {
  val LOG = LogFactory.getLog(classOf[KafkaYarnManager])
  val ApplicationName = "KafkaYarnManager"

  /**
   * @param args
   */
//  def main(args: Array[String]) {
    val rt = ToolRunner.run(new KafkaYarnManager, args)
    sys.exit(rt)
//  }

}
