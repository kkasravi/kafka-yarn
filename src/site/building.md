# kafka-yarn-application

## Building Steps:
* sbt assembly
* java -cp target/scala-2.9.2/kafka-yarn-assembly-0.0.1-SNAPSHOT.jar kafka.yarn.KafkaYarnClient
Usage: KafkaYarnClient [options] 
Options:
  --start        Start the kafka brokers
  --stop         Stop the kafka brokers
  --zookeeper  <add|remove|list>.
