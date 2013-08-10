package kafka.yarn

class KafkaYarnManagerConfig private (val args: Array[String]){

}

object KafkaYarnManagerConfig {
  def apply(args: Array[String]) = new KafkaYarnManagerConfig(args)
}