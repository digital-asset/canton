/*
Compare two daml versions.
Return: an integer (negative if the first version is smaller than the second,
zero in case of equivalent versions, positive if the second version is bigger
than the second).

Two examples of such versions are:
* 2.5.0
* 2.5.0-snapshot.20221208.11077.0.53b7db71

In the second case, 20221208 corresponds to a date and 11077 to a number of commits.

Comparison is done as follows:
* Comparison of x.y.z (version core) using SEMVER
* Given two identical cores, then
   - A snapshot version is considered smaller than a non-snapshot version.
   - Date is used.
   - Number of commits is used.
 */

import java.time.LocalDate
import java.time.format.DateTimeFormatter
@main
def compare(v1Raw: String, v2Raw: String): Int = {
  val v1 = parseSemver(v1Raw)
  val v2 = parseSemver(v2Raw)

  v1.compare(v2)
}

@main
def tests(): Unit = {
  assert(compare("2.1.3", "2.1.3") == 0)

  assert(compare("2.1.9", "2.2.1") == -1)

  assert(compare("2.5.0", "2.5.0-snapshot.20221209.11077.0.53b7db71") == 1)
  assert(compare("2.5.0-snapshot.20221209.11077.0.53b7db71", "2.5.0") == -1)

  // Version takes precedence over date
  assert(compare("2.5.0-snapshot.20221209.11077.0.53b7db71", "2.5.1-snapshot.20001212.1.0.53b7db71") == -1)

  // Date takes precedence over commits
  assert(compare("2.5.0-snapshot.20221210.1.0.53b7db71", "2.5.0-snapshot.20221208.2.0.53b7db71") == 2)

  // Commits are taken into account
  assert(compare("2.5.0-snapshot.20221209.1.0.53b7db71", "2.5.0-snapshot.20221209.2.0.53b7db71") == -1)

  // snapshot smaller than corresponding version
  assert(compare("2.7.1-rc2", "2.7.1") == -1)
}

/** Encodes a daml version
  *
  * @param snapshot a tuple date and Int, where the Int represents the number of commit
  */
case class Version(
    major: String,
    minor: String,
    patch: String,
    snapshot: Option[Version.Suffix],
  ) extends Ordered[Version] {
  override def compare(that: Version): Int = {
    if (major != that.major) major.compare(that.major)
    else if (minor != that.minor) minor.compare(that.minor)
    else if (patch != that.patch) patch.compare(that.patch)
    else { // suffix comparison
      (snapshot, that.snapshot) match {
        case (None, None) => 0
        case (None, Some(_)) => 1
        case (Some(_), None) => -1

        case (Some(suffix1), Some(suffix2)) =>
          (suffix1, suffix2) match {
            case (Version.Snapshot(date1, commits1), Version.Snapshot(date2, commits2)) =>
              val dateComp = date1.compareTo(date2)

              if (dateComp != 0)
                dateComp
              else
                commits1.compareTo(commits2)

            case (Version.RC(number1), Version.RC(number2)) =>
              number1.compareTo(number2)

            case _ => 0 // incomparable but I don't want to throw here
          }
      }
    }
  }
}

object Version {
  sealed trait Suffix extends Product with Serializable
  final case class Snapshot(date: LocalDate, commits: Int) extends Suffix
  final case class RC(number: Int) extends Suffix
}

private def parseSemver(version: String): Version = {
  // `?:` removes the capturing group, so we get a cleaner pattern-match statement
  val regex = raw"([0-9]{1,3})\.([0-9]{1,3})\.([0-9]{1,4})(?:-(.*))?".r

  version match {
    case regex(major, minor, patch, suffix) =>
      val rawSuffixO = Option(suffix) // `suffix` is `null` if no suffix is given

      // TODO(i27546) Remove adhoc handling once adhoc versions are no longer used
      val suffixRegex = raw"(adhoc|snapshot)\.([0-9]+)\.([0-9]+)\.([0-9]+)\.([0-9a-z]+)".r
      val rcRegex = "rc([0-9]+)".r

      val suffixO = rawSuffixO.map {
        case suffixRegex(_, date, commits, _, _) =>
          Version.Snapshot(LocalDate.parse(date, DateTimeFormatter.BASIC_ISO_DATE), commits.toInt)
        case rcRegex(rcNumber) =>
          Version.RC(rcNumber.toInt)

        case _ =>
          throw new IllegalArgumentException(
            s"Unable to convert string `$rawSuffixO` to a valid daml version suffix."
          )
      }

      Version(major, minor, patch, suffixO)

    case _ =>
      throw new IllegalArgumentException(
        s"Unable to convert string `$version` to a valid release version."
      )
  }
}
