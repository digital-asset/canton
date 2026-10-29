// Usage: amm splice-date.sc <version 3.x> <devnet|testnet|mainnet>
//    or: amm splice-date.sc --version 3.x --network testnet   (-v and -n are aliases of --version and --network)
//
// Converts a Canton version to the matching Splice version (3.6 -> 0.9.0, 3.7 -> 0.10.0, ...)
// and looks up the date on which that Splice version reaches the given network.

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.io.Source
import scala.util.Try
import scala.annotation.tailrec

val calendarUrl = "https://sv-cal.canton.foundation/schedule-dev.ics"
val firstCantonMinor = 6 // Canton 3.6 <-> Splice 0.9.0
val firstSpliceMinor = 9
val Networks = Seq("devnet", "testnet", "mainnet")

def fail(msg: String): Nothing = {
  Console.err.println(s"Error: $msg")
  sys.exit(1)
}

// Get splice version from Canton version
def spliceVersion(cantonVersion: String): String = {
  val CantonRegex = """3\.(\d+)""".r
  cantonVersion match {
    case CantonRegex(x) =>
      val minor = x.toInt
      if (minor < firstCantonMinor)
        fail(s"Canton version must be at least 3.$firstCantonMinor, got $cantonVersion")
      s"0.${minor - firstCantonMinor + firstSpliceMinor}.0"
    case _ => fail(s"Canton version must be of the form 3.x with x an integer, got '$cantonVersion'")
  }
}

/** Returns the (summary, date) of every event of the calendar. */
def parseEvents(ics: String): Seq[(String, LocalDate)] = {
  // Unfold continuation lines (RFC 5545: a line starting with a space or tab continues the previous one)
  val lines = ics.replace("\r\n", "\n").replaceAll("\n[ \t]", "").split("\n").toSeq
  val dateFormat = DateTimeFormatter.BASIC_ISO_DATE

  def property(event: Seq[String], name: String): Option[String] =
    event.collectFirst {
      case l if l.startsWith(name + ":") || l.startsWith(name + ";") => l.substring(l.indexOf(':') + 1).trim
    }

  @tailrec
  // Parse the lines and return a list of events (outer list). Each event consists of a set of lines (inner list).
  def eventBlocks(remaining: List[String], acc: List[List[String]]): List[List[String]] = {
    // Drop non-events lines
    remaining.dropWhile(_ != "BEGIN:VEVENT") match {
      case Nil => acc
      case _ :: rest =>
        val (event, tail) = rest.span(_ != "END:VEVENT")
        eventBlocks(tail, event :: acc)
    }
  }

  eventBlocks(lines.toList, Nil).flatMap { event =>
    for {
      summary <- property(event, "SUMMARY") // This contains the text we want to inspect
      start <- property(event, "DTSTART") // The date
      date <- Try(LocalDate.parse(start.take(8), dateFormat)).toOption
    } yield (summary.replace("\\,", ","), date)
  }
}

@main
def main(
    @arg(short = 'v', doc = "Canton version, of the form 3.x") version: String,
    @arg(short = 'n', doc = "Network: devnet, testnet or mainnet") network: String,
    @arg(short = 'f', doc = "Local ICS file to use instead of downloading the calendar") file: Option[String] = None
): Unit = {
  val net = network.toLowerCase
  if (!Networks.contains(net)) fail(s"Network must be one of ${Networks.mkString(", ")}, got '$network'")

  val splice = spliceVersion(version)

  val ics = file match {
    case Some(path) => Try(Source.fromFile(path, "UTF-8").mkString).getOrElse(fail(s"Unable to read $path"))
    case None => Try(Source.fromURL(calendarUrl, "UTF-8").mkString).getOrElse(fail(s"Unable to download the calendar at $calendarUrl"))
  }

  val SummaryRegex = """(?i)\s*(devnet|testnet|mainnet)\s+upgrades\s+to\s+splice\s+(\d+\.\d+\.\d+)\s*""".r
  val dates = parseEvents(ics).collect {
    case (SummaryRegex(n, v), date) if n.toLowerCase == net && v == splice => date
  }

  dates.sorted.headOption match {
    case Some(date) =>
      println(s"Splice version: $splice")
      println(s"Date: $date")
    case None => fail(s"No entry found for Splice $splice on $net in $calendarUrl")
  }
}
