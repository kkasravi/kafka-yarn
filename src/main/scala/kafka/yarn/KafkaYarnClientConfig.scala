package kafka.yarn

import scala.collection.mutable.ArrayBuffer

class KafkaYarnClientConfig private (val args: Array[String]){
  var start: Boolean = false
  var stop: Boolean = false
  var status: Boolean = false
  parseArgs(args.toList)
  
  private def parseArgs(inputArgs: List[String]): Unit = {
    val zookeeperArgs = List[String]()
    var args = inputArgs
    if(args.isEmpty) {
    	printUsageAndExit(1)
    }
    while (! args.isEmpty) {
      args match {
        case ("--start") :: tail =>
          start = true
          args = tail
        case ("--stop") :: tail =>
          stop = true
          args = tail
        case ("--status") :: tail =>
          status = true
          args = tail
        case ("--zookeeper") :: value :: tail =>
          zookeeperArgs +: value
          args = tail
          System.out.println(zookeeperArgs.first)
          zookeeperArgs match {
            case ("add") :: value :: tail =>
              System.out.println(value)
              zookeeperArgs +: value
            case ("remove") :: value :: tail =>
              System.out.println(value)
              zookeeperArgs +: value
            case ("list") :: tail =>
             case Nil =>
             case _ =>
          }
        case Nil =>
          printUsageAndExit(1)
        case _ =>
          printUsageAndExit(1, args)
      }
    }
  }
  
  def printUsageAndExit(exitCode: Int, unknownParam: Any = null) {
    if (unknownParam != null) {
      System.err.println("Unknown/unsupported param " + unknownParam)
    }
    System.err.println(
      "Usage: KafkaYarnClient [options] \n" +
      "Options:\n" +
      "  --start        Start the kafka brokers\n" +
      "  --stop         Stop the kafka brokers\n" +
      "  --zookeeper  <add|remove|list>.\n")
    System.exit(exitCode)
  }
}

object KafkaYarnClientConfig {
  def apply(args: Array[String]) = new KafkaYarnClientConfig(args)
}
