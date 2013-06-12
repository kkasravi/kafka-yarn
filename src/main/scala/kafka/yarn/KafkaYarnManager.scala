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

class KafkaYarnManager(conf: Configuration = new Configuration) extends Configured(conf) with Tool {
  import KafkaYarnManager._
  var containerMemory = 10
  var resourceManager = Records.newRecord(classOf[AMRMProtocol])
  var rpc = Records.newRecord(classOf[YarnRPC])
  var shellScriptPath = ""
  val commandEnv = Map[String,String]()
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

    @Override
    /**
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
          return;
        }
        shellRsrc.setTimestamp(shellScriptPathTimestamp);
        shellRsrc.setSize(shellScriptPathLen);
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

      // Set args for the shell command if any
      vargs.add(shellArgs);
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
   * Parse command line options
   * @param args Command line args 
   * @return Whether init successful and run should be invoked 
   * @throws ParseException
   * @throws IOException 
   */
  def init(args: Array[String]): Boolean = {

    Options opts = new Options();
    opts.addOption("app_attempt_id", true, "App Attempt ID. Not to be used unless for testing purposes");
    opts.addOption("shell_command", true, "Shell command to be executed by the Application Master");
    opts.addOption("shell_script", true, "Location of the shell script to be executed");
    opts.addOption("shell_args", true, "Command line args for the shell script");
    opts.addOption("shell_env", true, "Environment for shell script. Specified as env_key=env_val pairs");
    opts.addOption("container_memory", true, "Amount of memory in MB to be requested to run the shell command");
    opts.addOption("num_containers", true, "No. of containers on which the shell command needs to be executed");
    opts.addOption("priority", true, "Application Priority. Default 0");
    opts.addOption("debug", false, "Dump out debug information");

    opts.addOption("help", false, "Print usage");
    CommandLine cliParser = new GnuParser().parse(opts, args);

    if (args.length == 0) {
      printUsage(opts);
      throw new IllegalArgumentException("No args specified for application master to initialize");
    }

    if (cliParser.hasOption("help")) {
      printUsage(opts);
      return false;
    }

    if (cliParser.hasOption("debug")) {
      dumpOutDebugInfo();
    }

    Map<String, String> envs = System.getenv();

    appAttemptID = Records.newRecord(ApplicationAttemptId.class);
    if (envs.containsKey(ApplicationConstants.AM_APP_ATTEMPT_ID_ENV)) {
      appAttemptID = ConverterUtils.toApplicationAttemptId(envs
          .get(ApplicationConstants.AM_APP_ATTEMPT_ID_ENV));
    } else if (!envs.containsKey(ApplicationConstants.AM_CONTAINER_ID_ENV)) {
      if (cliParser.hasOption("app_attempt_id")) {
        String appIdStr = cliParser.getOptionValue("app_attempt_id", "");
        appAttemptID = ConverterUtils.toApplicationAttemptId(appIdStr);
      } 
      else {
        throw new IllegalArgumentException("Application Attempt Id not set in the environment");
      }
    } else {
      ContainerId containerId = ConverterUtils.toContainerId(envs.get(ApplicationConstants.AM_CONTAINER_ID_ENV));
      appAttemptID = containerId.getApplicationAttemptId();
    }

    LOG.info("Application master for app"
        + ", appId=" + appAttemptID.getApplicationId().getId()
        + ", clustertimestamp=" + appAttemptID.getApplicationId().getClusterTimestamp()
        + ", attemptId=" + appAttemptID.getAttemptId());

    if (!cliParser.hasOption("shell_command")) {
      throw new IllegalArgumentException("No shell command specified to be executed by application master");
    }
    shellCommand = cliParser.getOptionValue("shell_command");

    if (cliParser.hasOption("shell_args")) {
      shellArgs = cliParser.getOptionValue("shell_args");
    }
    if (cliParser.hasOption("shell_env")) { 
      String shellEnvs[] = cliParser.getOptionValues("shell_env");
      for (String env : shellEnvs) {
        env = env.trim();
        int index = env.indexOf('=');
        if (index == -1) {
          shellEnv.put(env, "");
          continue;
        }
        String key = env.substring(0, index);
        String val = "";
        if (index < (env.length()-1)) {
          val = env.substring(index+1);
        }
        shellEnv.put(key, val);
      }
    }

    if (envs.containsKey(DSConstants.DISTRIBUTEDSHELLSCRIPTLOCATION)) {
      shellScriptPath = envs.get(DSConstants.DISTRIBUTEDSHELLSCRIPTLOCATION);

      if (envs.containsKey(DSConstants.DISTRIBUTEDSHELLSCRIPTTIMESTAMP)) {
        shellScriptPathTimestamp = Long.valueOf(envs.get(DSConstants.DISTRIBUTEDSHELLSCRIPTTIMESTAMP));
      }
      if (envs.containsKey(DSConstants.DISTRIBUTEDSHELLSCRIPTLEN)) {
        shellScriptPathLen = Long.valueOf(envs.get(DSConstants.DISTRIBUTEDSHELLSCRIPTLEN));
      }

      if (!shellScriptPath.isEmpty()
          && (shellScriptPathTimestamp <= 0 
          || shellScriptPathLen <= 0)) {
        LOG.error("Illegal values in env for shell script path"
            + ", path=" + shellScriptPath
            + ", len=" + shellScriptPathLen
            + ", timestamp=" + shellScriptPathTimestamp);
        throw new IllegalArgumentException("Illegal values in env for shell script path");
      }
    }

    containerMemory = Integer.parseInt(cliParser.getOptionValue("container_memory", "10"));
    numTotalContainers = Integer.parseInt(cliParser.getOptionValue("num_containers", "1"));
    requestPriority = Integer.parseInt(cliParser.getOptionValue("priority", "0"));

    return true;
  }

    /**
     * Helper function to connect to CM
     */
    def connectToCM(container: Container): ContainerManager = {
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
   def sendContainerAskToRM(requestedContainers: Seq[ResourceRequest]): AMResponse = {
    val req = Records.newRecord(classOf[AllocateRequest]);
    req.setResponseId(rmRequestID.incrementAndGet());
    req.setApplicationAttemptId(appAttemptID);
    req.addAllAsks(seqAsJavaList(requestedContainers));
    req.addAllReleases(releasedContainers);
    req.setProgress(numCompletedContainers.get()/numTotalContainers);

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
   
  /**
   * @param args
   */
  def run(args: Array[String]) = {
	rpc = YarnRPC.create(conf)
    // Get containerId
    val containerId = ConverterUtils.toContainerId(
      sys.env.getOrElse(ApplicationConstants.AM_CONTAINER_ID_ENV,
        throw new IllegalArgumentException("ContainerId not set in the environment")))
    val appAttemptId = containerId.getApplicationAttemptId

    // Connect to the Scheduler of the ResourceManager
    val yarnConf = new YarnConfiguration(conf)
    val rmAddress = NetUtils.createSocketAddr(
      yarnConf.get(YarnConfiguration.RM_SCHEDULER_ADDRESS, YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS))
    resourceManager = rpc.getProxy(classOf[AMRMProtocol], rmAddress, conf).asInstanceOf[AMRMProtocol]

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
    
    // Setup request to be sent to RM to allocate containers
    val resourceReq = Seq[ResourceRequest]();
    val containerAsk = setupContainerAskForRM(1);
    resourceReq :+ containerAsk;
    
    // Send the request to RM
    val amResp = sendContainerAskToRM(resourceReq);

    // Retrieve list of allocated containers from the response
    val allocatedContainers = amResp.getAllocatedContainers();
    LOG.info("Got response from RM for container ask, allocatedCnt=" + allocatedContainers.size())
    numAllocatedContainers.addAndGet(allocatedContainers.size());
    for (allocatedContainer <- allocatedContainers) {
        LOG.info("Launching shell command on a new container."
            + ", containerId=" + allocatedContainer.getId()
            + ", containerNode=" + allocatedContainer.getNodeId().getHost()
            + ":" + allocatedContainer.getNodeId().getPort()
            + ", containerNodeURI=" + allocatedContainer.getNodeHttpAddress()
            + ", containerState" + allocatedContainer.getState()
            + ", containerResourceMemory" + allocatedContainer.getResource().getMemory());

        val runnableLaunchContainer = new LaunchContainer(allocatedContainer, connectToCM(allocatedContainer));
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
    sys.exit(ToolRunner.run(new KafkaYarnManager, args))
  }

}
