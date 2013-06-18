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
//import org.specs2.io.fs
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem

class KafkaYarnManager(conf: Configuration = new Configuration) extends Configured(conf) with Tool {
  import KafkaYarnManager._
  var containerMemory = 10
  var shellScriptPath = ""
  val shellCommand = "startkafka"
  val fs = FileSystem.get(conf)
  var ExecShellStringPath = "ExecShellScript.sh"
  val commandEnv = Map[String, String]()
  val requestPriority = 0
  val rmRequestID = new AtomicInteger()
  val numCompletedContainers = new AtomicInteger()
  val numAllocatedContainers = new AtomicInteger()
  val numFailedContainers = new AtomicInteger()
  val numTotalContainers = 1
  val launchThreads = Seq[Thread]()
  val appAttemptID = Records.newRecord(classOf[ApplicationAttemptId])
  val releasedContainers = new CopyOnWriteArrayList[ContainerId]()

  class LaunchContainer(container: Container, var cm: ContainerManager) extends Runnable {

    @Override /**
     * Connects to CM, sets up container launch context
     * for shell command and eventually dispatches the container
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
      // Set the local resources
      val localResources = Map[String, LocalResource]();

      // The container for the eventual shell commands needs its own local resources too.
      // In this scenario, if a shell script is specified, we need to have it copied
      // and made available to the container.
      if (!shellScriptPath.isEmpty()) {
        val shellRsrc = Records.newRecord(classOf[LocalResource]);
        shellRsrc.setType(LocalResourceType.FILE);
        shellRsrc.setVisibility(LocalResourceVisibility.APPLICATION);
        try {
          shellRsrc.setResource(ConverterUtils.getYarnUrlFromURI(new URI(shellScriptPath)));
        } catch {
          case e: URISyntaxException =>
            LOG.error("Error when trying to use shell script path specified in env"
              + ", path=" + shellScriptPath);
            e.printStackTrace();

            // A failure scenario on bad input such as invalid shell script path
            // We know we cannot continue launching the container
            // so we should release it.
            // TODO
            numCompletedContainers.incrementAndGet();
            numFailedContainers.incrementAndGet();
            return ;
        }
        localResources.put(ExecShellStringPath, shellRsrc);
      }
      ctx.setLocalResources(localResources);

      // Set the necessary command to execute on the allocated container
      val vargs = Seq[CharSequence]();

      // Set executable command
      vargs.add(shellCommand);
      // Set shell script path
      if (!shellScriptPath.isEmpty()) {
        vargs.add(ExecShellStringPath);
      }

      // Add log redirect params
      // TODO
      // We should redirect the output to hdfs instead of local logs
      // so as to be able to look at the final output after the containers
      // have been released.
      // Could use a path suffixed with /AppId/AppAttempId/ContainerId/std[out|err]
      vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
      vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");

      // Get final commmand
      val command = new StringBuilder();
      for (str <- vargs) {
        command.append(str).append(" ");
      }

      val commands = Seq[String]();
      commands.add(command.toString());
      ctx.setCommands(commands);

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

  def setupContainerAskForRM(numContainers: Int): ResourceRequest = {
    val request = Records.newRecord(classOf[ResourceRequest]);

    // setup requirements for hosts
    // whether a particular rack/host is needed
    // Refer to apis under org.apache.hadoop.net for more
    // details on how to get figure out rack/host mapping.
    // using * as any host will do for the distributed shell app
    request.setHostName("*");

    // set no. of containers needed
    request.setNumContainers(numContainers);

    // set the priority for the request
    val pri = Records.newRecord(classOf[Priority]);
    pri.setPriority(requestPriority);
    request.setPriority(pri);

    // Set up resource type requirements
    // For now, only memory is supported so we set memory requirements
    val capability = Records.newRecord(classOf[Resource]);
    capability.setMemory(containerMemory);
    request.setCapability(capability);

    return request;
  }

  /**
   * Ask RM to allocate given no. of containers to this Application Master
   * @param requestedContainers Containers to ask for from RM
   * @return Response from RM to AM with allocated containers
   * @throws YarnRemoteException
   */
  def sendContainerAskToRM(requestedContainers: Seq[ResourceRequest], resourceManager: AMRMProtocol): AMResponse = {
    val req = Records.newRecord(classOf[AllocateRequest]);
    req.setResponseId(rmRequestID.incrementAndGet());
    req.setApplicationAttemptId(appAttemptID);
    req.addAllAsks(seqAsJavaList(requestedContainers));
    req.addAllReleases(releasedContainers);
    req.setProgress(numCompletedContainers.get() / numTotalContainers);

    LOG.info("Sending request to RM for containers"
      + ", requestedSet=" + requestedContainers.size()
      + ", releasedSet=" + releasedContainers.size()
      + ", progress=" + req.getProgress());

    for (rsrcReq <- requestedContainers) {
      LOG.info("Requested container ask: " + rsrcReq.toString());
    }
    for (id <- releasedContainers) {
      LOG.info("Released container, id=" + id.getId());
    }

    val resp = resourceManager.allocate(req);
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
    } else if (containerMemory > maxMem) {
      LOG.info("Container memory specified above max threshold of cluster. Using max value."
        + ", specified=" + containerMemory
        + ", max=" + maxMem);
      containerMemory = maxMem;
    }

    // Setup request to be sent to RM to allocate containers
    val resourceReq = Seq[ResourceRequest]();
    val containerAsk = setupContainerAskForRM(1);
    resourceReq :+ containerAsk;

    // Send the request to RM
    val amResp = sendContainerAskToRM(resourceReq, resourceManager);

    // Retrieve list of allocated containers from the response
    val allocatedContainers = amResp.getAllocatedContainers();
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

      val runnableLaunchContainer = new LaunchContainer(allocatedContainer, connectToCM(allocatedContainer, rpc));
      val launchThread = new Thread(runnableLaunchContainer);

      // launch and start the container on a separate thread to keep the main thread unblocked
      // as all containers may not be allocated at one go.
      launchThreads :+ launchThread;
      launchThread.start();
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
    val rt = ToolRunner.run(new KafkaYarnManager, args)
    sys.exit(rt)
  }

}
