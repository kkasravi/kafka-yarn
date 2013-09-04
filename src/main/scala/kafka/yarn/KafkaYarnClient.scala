package kafka.yarn

import scala.annotation.tailrec
import scala.collection.JavaConversions._
import org.apache.commons.logging._
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.hadoop.net._
import org.apache.hadoop.util._
import org.apache.hadoop.yarn.api._
import org.apache.hadoop.yarn.api.ApplicationConstants._
import org.apache.hadoop.yarn.api.protocolrecords._
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.api.records.YarnApplicationState._
import org.apache.hadoop.yarn.conf._
import org.apache.hadoop.yarn.ipc._
import org.apache.hadoop.yarn.util._

class KafkaYarnClient(conf: Configuration = new Configuration) extends Configured(conf) with Tool {
  var config: KafkaYarnConfig = null

  def addResource(name: Option[String]): Map[String,LocalResource] = {
    val fs = FileSystem.get(getConf)
    name.map({
      file =>
      val filePath = new Path(file)
      val fileStatus = fs.getFileStatus(filePath)
      val name = filePath.getName
      val amJarRsrc = Records.newRecord(classOf[LocalResource])
      amJarRsrc.setType(LocalResourceType.FILE)
      amJarRsrc.setVisibility(LocalResourceVisibility.PUBLIC)
      amJarRsrc.setResource(ConverterUtils.getYarnUrlFromPath(FileContext.getFileContext.makeQualified(filePath)))
      amJarRsrc.setTimestamp(fileStatus.getModificationTime)
      amJarRsrc.setSize(fileStatus.getLen)
      (name -> amJarRsrc)
    }).toMap
  }
  
  def run(args: Array[String]) = {
    import KafkaYarnClient.{LOG, ApplicationName}
    val rpc = YarnRPC.create(getConf)
    
    config = KafkaYarnConfig(args, ApplicationName)
    val jarName: Option[String] = Some("target/scala-2.9.2/kafka-yarn-assembly-0.0.1-SNAPSHOT.jar")
    // Create a new container launch context for the AM's container
    val amContainer = Records.newRecord(classOf[ContainerLaunchContext])

    // Define the local resources required
    val localResources:Map[String,LocalResource] = addResource(jarName) ++ addResource(config.fileOption)
    amContainer.setLocalResources(localResources)

    // Connect to ApplicationsManager
    val yarnConf = new YarnConfiguration(getConf)
    val rmAddress = NetUtils.createSocketAddr(yarnConf.get(YarnConfiguration.RM_ADDRESS, YarnConfiguration.DEFAULT_RM_ADDRESS))
    val applicationsManager = rpc.getProxy(classOf[ClientRMProtocol], rmAddress, conf).asInstanceOf[ClientRMProtocol]
    LOG.info("Connecting to ResourceManager at " + rmAddress)

    // Get ApplicationId from ApplicationsManager
    val application = applicationsManager.getNewApplication(Records.newRecord(classOf[GetNewApplicationRequest]))
    LOG.info("Got new ApplicationId=" + application.getApplicationId)

    // Create a new ApplicationSubmissionContext
    val appContext = Records.newRecord(classOf[ApplicationSubmissionContext])
    appContext.setApplicationId(application.getApplicationId)
    appContext.setApplicationName(ApplicationName)


    // Set up the environment needed for the launch context
    val environment = Map(Environment.CLASSPATH.name -> List(Environment.CLASSPATH.$,"./*").mkString(System.getProperty("path.separator")))
    amContainer.setEnvironment(environment)
    LOG.info("ApplicationManager environment: " + environment)

    // Construct the command to launch the AppMaster
    val command: List[String] = List(
      Environment.JAVA_HOME.$ + "/bin/java",
      "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=1046",
      "-cp "+"kafka-yarn-assembly-0.0.1-SNAPSHOT.jar",
      "kafka.yarn.KafkaYarnManager",
      config,
      config.fileOption.get,
      "1>/users/kamkasravi/stdout",
      "2>/users/kamkasravi/stderr")
//      "1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + Path.SEPARATOR + ApplicationConstants.STDOUT,
//      "2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + Path.SEPARATOR + ApplicationConstants.STDERR)
    amContainer.setCommands(command)
    LOG.info("ApplicationManager Command: " + command.mkString(" "))

    // Define the resource requirements for the container
    val capability = Records.newRecord(classOf[Resource])
    capability.setMemory(1024)
    amContainer.setResource(capability)

    // Set the container launch content into the ApplicationSubmittionContext
    appContext.setAMContainerSpec(amContainer)

    // Create the request to send to the ApplicationsManager
    val appRequest = Records.newRecord(classOf[SubmitApplicationRequest])
    appRequest.setApplicationSubmissionContext(appContext)
    applicationsManager.submitApplication(appRequest)

    // Monitor the Application
    @tailrec
    def monitor(appId: ApplicationId): Boolean = {
      val reportRequest = Records.newRecord(classOf[GetApplicationReportRequest])
      reportRequest.setApplicationId(appId)
      val reportResponse = applicationsManager.getApplicationReport(reportRequest)
      val report = reportResponse.getApplicationReport

      LOG.info("Application report from ASM: \n" +
        "\t application identifier: " + appId.toString() + "\n" +
        "\t appId: " + appId.getId() + "\n" +
        "\t clientToken: " + report.getClientToken() + "\n" +
        "\t appDiagnostics: " + report.getDiagnostics() + "\n" +
        "\t appMasterHost: " + report.getHost() + "\n" +
        "\t appQueue: " + report.getQueue() + "\n" +
        "\t appMasterRpcPort: " + report.getRpcPort() + "\n" +
        "\t appStartTime: " + report.getStartTime() + "\n" +
        "\t yarnAppState: " + report.getYarnApplicationState() + "\n" +
        "\t distributedFinalState: " + report.getFinalApplicationStatus() + "\n" +
        "\t appTrackingUrl: " + report.getTrackingUrl() + "\n" +
        "\t appUser: " + report.getUser()
      )
      
      val state = report.getYarnApplicationState
      state match {
        case FINISHED =>
          report.getFinalApplicationStatus == FinalApplicationStatus.SUCCEEDED
        case KILLED => false
        case FAILED => false
        case ACCEPTED => Thread.sleep(3); monitor(appId)
        case _      => monitor(appId)
      }
    }

    if (monitor(application.getApplicationId)) 0 else 1
  }
}

object KafkaYarnClient extends App {
  val LOG = LogFactory.getLog(classOf[KafkaYarnClient])
  val ApplicationName = "KafkaYarnClient"
  sys.exit(ToolRunner.run(new KafkaYarnClient, args))
}
