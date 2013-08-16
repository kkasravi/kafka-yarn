package kafka.yarn

import scala.io.Source
import scala.util.parsing.json._


class KafkaYarnConfig private (val args: Array[String]){
  val START = Option("--start")
  val STOP = Option("--stop")
  val STATUS = Option("--status")
  val CONFIG = Option("--config")
  var arg: String = null
  var zookeeper:Map[String,String] = null
  parseArgs(args.toList)

  private def parseArgs(inputArgs: List[String]): Unit = {
    var args = inputArgs    
    if(args.isEmpty) {
    	printUsageAndExit(1)
    }
    class Config {
      class Zookeeper(host:String,port:Integer)
      class Master(zookeeper:Zookeeper)
      class Broker(id:Integer,port:Integer,zookeepers:List[Zookeeper])
      var master: Master = null
      var brokers: List[Broker] = null
    }
    var skip = false
    args.foreach({ value => {
      Option(value) match {
        case START =>
          arg = value
        case STOP =>
          arg = value
        case STATUS =>
          arg = value
        case CONFIG =>
          args = args.tail
          val file = args.first
          skip = true
          val text = Source.fromFile(file).mkString//.split ("\n").map (_.trim).mkString
          val json = JSON.parseFull(text)
          val map:Map[String,Any] = json.get.asInstanceOf[Map[String, Any]]
          val master:Map[String,Any] = map.get("master").asInstanceOf[Option[Map[String,Any]]].get;
          zookeeper = master.get("zookeeper").asInstanceOf[Option[Map[String,String]]].get
          val brokers:List[Any] = map.get("brokers").asInstanceOf[Option[List[Any]]].get;
          val c = new Config()
          c.master = new c.Master(new c.Zookeeper(zookeeper.get("host").get,zookeeper.get("port").get.toInt))
        case _ =>
          if(skip) {
            skip = false
          } else {
        	  printUsageAndExit(1)
          }
      }
    }})
  }

    def printUsageAndExit(exitCode: Int, unknownParam: Any = null) {
	    if (unknownParam != null) {
	      System.err.println("Unknown/unsupported param " + unknownParam)
	    }
	    System.err.println(
	      "Usage: KafkaYarnManager [options] \n" +
	      "Options:\n" +
	      "  --start        Start the kafka brokers\n" +
	      "  --stop         Stop the kafka brokers\n" +
	      "  --zookeeper  <add|remove|list>.\n")
	    System.exit(exitCode)
	  }
}

object KafkaYarnConfig {
   implicit def convert(v:KafkaYarnConfig): String = v.arg

  def apply(args: Array[String]) = new KafkaYarnConfig(args)
}