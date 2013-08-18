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
  var brokers:List[Map[String,Any]] = null
  parseArgs(args.toList)

  private def parseJsonFile(file:String): Unit = {
    val text = Source.fromFile(file).mkString
    val json = JSON.parseFull(text)
    val map:Map[String,Any] = json.get.asInstanceOf[Map[String, Any]]
    val master:Map[String,Any] = map.get("master").asInstanceOf[Option[Map[String,Any]]].get;
    zookeeper = master.get("zookeeper").map[Map[String,String]](value=>value.asInstanceOf[Map[String,String]]).getOrElse(null)      
    brokers = map.get("brokers").map[List[Map[String,Any]]](value=>value.asInstanceOf[List[Map[String,Any]]]).getOrElse(null)
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
          skip = true
          parseJsonFile(args.tail.first)
        case STOP =>
          arg = value
          skip = true
          parseJsonFile(args.tail.first)
        case STATUS =>
          arg = value
          skip = true
          parseJsonFile(args.tail.first)
        case CONFIG =>
          arg = value
          skip = true
          parseJsonFile(args.tail.first)
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
	      "  --config  Configure the kafka brokers. Get zookeeper and broker information from input.json\n" +
	      "  --start   Start the kafka brokers. Get zookeeper information from input.json.\n" +
	      "  --stop    Stop the kafka brokers. Get zookeeper information from input.json.\n" +
	      "" +
	      "Zookeeper and broker information used by the above options is read from input.json.")
	    System.exit(exitCode)
	  }
}

object KafkaYarnConfig {
   implicit def convert(v:KafkaYarnConfig): String = v.arg

  def apply(args: Array[String], command: String) = new KafkaYarnConfig(args, command)
}