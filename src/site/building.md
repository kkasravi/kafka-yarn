# kafka-yarn-application

## Building Steps:
* sbt assembly

## Running Steps:
* java -cp target/scala-2.9.2/kafka-yarn-assembly-0.0.1-SNAPSHOT.jar kafka.yarn.KafkaYarnClient
<pre>
Usage: KafkaYarnClient [options] input.json
Options:
  --config Configure the kafka brokers.
  --start  Start the kafka brokers. Get zookeeper information from input.json.
  --stop   Stop the kafka brokers. Get zookeeper information from input.json.
input.json Zookeeper and broker information used by the various options is read from input.json.
</pre>
