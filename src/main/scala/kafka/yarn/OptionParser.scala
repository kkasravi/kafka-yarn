package kafka.yarn

import scala.collection.mutable.ListBuffer

class OptionParser {

  private val optionInfos = new ListBuffer[OptionInfo]

  def addOption(shortName: String, longName: Option[String] = None) =
    optionInfos += OptionInfo(shortName, longName, false)

  def addFlagOption(shortName: String, longName: Option[String] = None) =
    optionInfos += OptionInfo(shortName, longName, true)

  def parseArgs(rawArgs: Seq[String]): (Map[String, String], Seq[String]) = {

    object option {
      def unapply(obj: Any): Option[OptionInfo] =
        (obj match {
          case OptionParser.option(opt) => Some(opt)
          case OptionParser.longOption(opt) => Some(opt)
          case _ => None
        }) flatMap { opt =>
          optionInfos.find {
            x => x.shortName == opt || x.longName == Some(opt)
          }
        }
    }

    def updateOptions(opts: Map[String, String], opt: OptionInfo, v: String) = {
      val updated = opts.updated(opt.shortName, v)
      if (opt.longName.isDefined)
        updated.updated(opt.longName.get, v)
      else
        updated
    }

    val (options, args) =
      rawArgs.reverse.foldLeft((Map.empty[String, String], Nil: List[String])) {
        (results, arg) =>
          val (options, args) = results
          arg match {
            case option(opt) if opt.isFlag =>
              (updateOptions(options, opt, ""), args)
            case option(opt)  =>
              val (x :: xs) = args
              (updateOptions(options, opt, x), xs)
            case arg =>
              (options, arg :: args)
          }
      }
    (options, args)
  }
}

object OptionParser {
  private val option = """-(\w)""".r
  private val longOption = """--([^-\s]\w+)""".r
}
