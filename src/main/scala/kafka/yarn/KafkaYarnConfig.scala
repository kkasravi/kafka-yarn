package kafka.yarn

import scala.io.Source
import scala.util.parsing.json._


class KafkaYarnConfig private (val args: Array[String], val command: String){
  val START = Option("--start")
  val STOP = Option("--stop")
  val STATUS = Option("--status")
  val CONFIG = Option("--config")
  var arg: String = null
  var fileOption: Option[String] = None
  var zookeeper:Map[String,String] = null
  parseArgs(args.toList)

  private def parseJsonFile(file:String): Unit = {
    val text = Source.fromFile(file).mkString
    val json = JSON.parseFull(text)
    val map:Map[String,Any] = json.get.asInstanceOf[Map[String, Any]]
    val master:Map[String,Any] = map.get("master").asInstanceOf[Option[Map[String,Any]]].get;
    zookeeper = master.get("zookeeper").asInstanceOf[Option[Map[String,String]]].get
    fileOption = Some(file)    
  }
  
  private def parseArgs(inputArgs: List[String]): Unit = {
    var args = inputArgs 
    if(args.isEmpty) {
    	printUsageAndExit(1,command)
    }
    var skip = false
    args.foreach({ value => {
      Option(value) match {
        case START =>
          arg = value
          args = args.tail
          val file = args.first
          skip = true
          parseJsonFile(file)
        case STOP =>
          arg = value
        case STATUS =>
          arg = value
        case CONFIG =>
          arg = value
          args = args.tail
          val file = args.first
          skip = true
          val text = Source.fromFile(file).mkString
          val json = JSON.parseFull(text)
          val map:Map[String,Any] = json.get.asInstanceOf[Map[String, Any]]
          val master:Map[String,Any] = map.get("master").asInstanceOf[Option[Map[String,Any]]].get;
          zookeeper = master.get("zookeeper").asInstanceOf[Option[Map[String,String]]].get
          val brokers:List[Any] = map.get("brokers").asInstanceOf[Option[List[Any]]].get;
          fileOption = Some(file)
        case _ =>
          if(skip) {
            skip = false
          } else {
        	  printUsageAndExit(1,command)
          }
      }
    }})
  }

    def printUsageAndExit(exitCode: Int, command: String, unknownParam: Any = null) {
	    if (unknownParam != null) {
	      System.err.println("Unknown/unsupported param " + unknownParam)
	    }
	    System.err.println(
	      "Usage: "+command+" [options] input.json\n" +
	      "Options:\n" +
	      "  --config Configure the kafka brokers.\n" +
	      "  --star   Start the kafka brokers. Get zookeeper information from input.json.\n" +
	      "  --stop   Stop the kafka brokers. Get zookeeper information from input.json.\n" +
	      "" +
	      "Zookeeper and broker information used by the various options is read from input.json.")
	    System.exit(exitCode)
	  }
}

object KafkaYarnConfig {
   implicit def convert(v:KafkaYarnConfig): String = v.arg

  def apply(args: Array[String], command: String) = new KafkaYarnConfig(args, command)
}