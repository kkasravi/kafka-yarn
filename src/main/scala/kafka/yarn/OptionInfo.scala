package kafka.yarn

case class OptionInfo(
  val shortName: String,
  val longName: Option[String] = None,
  val isFlag: Boolean = false) {

  require(shortName.length == 1)
}
