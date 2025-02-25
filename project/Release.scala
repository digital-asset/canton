import sbt.Keys.streams
import sbt.internal.util.ManagedLogger
import sbt.{IO, Setting, complete, inputKey}

import cats.syntax.either._

object Release {
  private val SemVerDigitsRegex = """(0|[1-9]\d*)"""

  // Utility class to hold args of release fix functions
  final case class ParsedVersion(
      major: String,
      minor: String,
      patch: String,
      suffix: Option[String],
  )(val fileToCheck: java.io.File)
      extends Ordered[ParsedVersion] {
    def majorMinor: (String, String) = (major, minor)

    override def compare(that: ParsedVersion): Int =
      if (this.major != that.major) this.major compare that.major
      else if (this.minor != that.minor) this.minor compare that.minor
      else if (this.patch != that.patch) this.patch compare that.patch
      else suffixComparison(this.suffix, that.suffix)

    // Simplified version of CantonVersion suffix comparison, we just need to know if suffixes are equal or not for the
    // purpose of this code
    private def suffixComparison(
        maybeSuffix1: Option[String],
        maybeSuffix2: Option[String],
    ): Int =
      (maybeSuffix1, maybeSuffix2) match {
        case (None, None) => 0
        case (None, Some(_)) => 1
        case (Some(_), None) => -1
        case (Some(suffix1), Some(suffix2)) => suffix1 compare suffix2
      }
  }

  private case class SplitFile(
      before: List[String],
      matching: Option[String],
      after: Vector[String],
  )

  /** Split lines in 3 parts:
    *   - Everything up until the line that either matches releaseVersion or is strictly lower than
    *     it
    *   - Possibly the line that matches releaseVersion if it exists
    *   - Everything the matching line, or strictly higher than releaseVersion
    */
  private def splitFile(
      lines: List[String],
      releaseVersion: ParsedVersion,
      regex: String,
      parseLine: String => ParsedVersion,
  ): SplitFile = {
    // Find the first line matching the regex, and split there
    val (beforeReleaseLines, fromFirstReleaseLine) = lines.span(!_.matches(regex))
    // From there, get all the release lines, and split at the first non release line
    val (releaseLines, afterLastReleaseLine) = fromFirstReleaseLine.span(_.matches(regex))

    // Split the version lines according to their order from the current release
    val (lower, matching, higher) =
      releaseLines.foldLeft((Vector.empty[String], Option.empty[String], Vector.empty[String])) {
        case ((lower, _, higher), line)
            if parseLine(line).majorMinor == releaseVersion.majorMinor =>
          (lower, Some(line), higher)
        case ((lower, matching, higher), line) if parseLine(line) < releaseVersion =>
          (lower :+ line, matching, higher)
        case ((lower, matching, higher), line) if parseLine(line) > releaseVersion =>
          (lower, matching, higher :+ line)
      }

    SplitFile(beforeReleaseLines ++ lower, matching, higher ++ afterLastReleaseLine)
  }

  private def checkOrUpdateVersionFile(
      releaseVersion: ParsedVersion,
      versionRegexString: String,
      // parse a line into a version
      parseLine: String => ParsedVersion,
      makeNewLine: (ParsedVersion, String) => String,
  )(implicit log: ManagedLogger): Unit = {
    val versionRegex = versionRegexString.r
    val lines = IO.readLines(releaseVersion.fileToCheck)

    val SplitFile(before, matching, after) =
      splitFile(lines, releaseVersion, versionRegexString, parseLine)

    matching
      .map { m =>
        log.info(s"Version already there: $m")
      }
      .getOrElse {
        val lastLowerVersion = before.last
        // Check that we could parse at least one version
        if (!lastLowerVersion.matches(versionRegexString)) {
          log.error(
            s"Did not find an existing release version in that file: ${releaseVersion.fileToCheck.getPath}"
          )
          sys.exit(1)
        }
        val newLine = makeNewLine(releaseVersion, lastLowerVersion)
        val newContent = (before :+ newLine) ++ after
        IO.writeLines(releaseVersion.fileToCheck, newContent)
        log.info("Release version added:")
        log.info(newLine)
      }
  }

  /** Checks if the release defined in releaseArgs is defined in the file. If not, add a line with
    * that version
    */
  def fixReleaseVersion(releaseArgs: ParsedVersion)(implicit log: ManagedLogger): Unit = {
    val versionRegexString =
      s""".*ReleaseVersion\\($SemVerDigitsRegex, $SemVerDigitsRegex, $SemVerDigitsRegex(?:, Some\\("(.*)"\\))?\\)$$"""
    val versionRegex = versionRegexString.r

    val lines = IO.readLines(releaseArgs.fileToCheck)

    def parseLine(s: String) = s match {
      case versionRegex(ma, mi, p, su) =>
        ParsedVersion(ma, mi, p, Option(su))(releaseArgs.fileToCheck)
    }

    checkOrUpdateVersionFile(
      releaseArgs,
      versionRegexString,
      parseLine,
      { case (args, lastLowerVersion) =>
        val suffixVarName = releaseArgs.suffix.map("_" + _.toLowerCase).getOrElse("")
        val versionString =
          s"""${releaseArgs.major}_${releaseArgs.minor}_${releaseArgs.patch}$suffixVarName"""
        val suffixString = releaseArgs.suffix.map(s => s""", Some("$s")""").getOrElse("")
        val prefixTab = lastLowerVersion.takeWhile(_.isWhitespace)
        s"""${prefixTab}lazy val v$versionString: ReleaseVersion = ReleaseVersion(${releaseArgs.major}, ${releaseArgs.minor}, ${releaseArgs.patch}$suffixString)"""
      },
    )
  }

  /** Checks if the release defined in releaseArgs has supported protocol versions defined in the
    * file. If not, add a line with that version, and a mapping using the protocol versions of to
    * the closest lower version found in the file
    */
  def fixReleaseToProtocolVersion(releaseArgs: ParsedVersion)(implicit log: ManagedLogger): Unit = {
    val semVerRegex =
      s"""${SemVerDigitsRegex}_${SemVerDigitsRegex}_$SemVerDigitsRegex(?:_(.*))?"""
    val versionRegexString = s""".*ReleaseVersions\\.v$semVerRegex -> (List\\(.*\\)).*"""
    val versionRegex = versionRegexString.r

    val parseLine: String => (ParsedVersion, String) = {
      case versionRegex(major, minor, patch, suffix, protocolVersions) =>
        (
          ParsedVersion(major, minor, patch, Option(suffix))(releaseArgs.fileToCheck),
          protocolVersions,
        )
    }

    checkOrUpdateVersionFile(
      releaseArgs,
      versionRegexString,
      parseLine.andThen { case (parsedVersion, _) => parsedVersion },
      { case (args, lastLowerVersion) =>
        val (_, protocolVersions) = parseLine(lastLowerVersion)
        val prefixTab = lastLowerVersion.takeWhile(_.isWhitespace)
        val suffixString = releaseArgs.suffix.map(s => s"_${s.toLowerCase}").getOrElse("")
        s"""${prefixTab}ReleaseVersions.v${releaseArgs.major}_${releaseArgs.minor}_${releaseArgs.patch}$suffixString -> $protocolVersions,"""
      },
    )
  }

  private def parseSemver(
      version: String
  ): Either[String, (String, String, String, Option[String])] = {
    // `?:` removes the capturing group, so we get a cleaner pattern-match statement
    val regex = raw"([0-9]{1,3})\.([0-9]{1,3})\.([0-9]{1,4})(?:-(.*))?".r

    version match {
      case regex(rawMajor, rawMinor, rawPatch, suffix) =>
        val parsedDigits = List(rawMajor, rawMinor, rawPatch)
        parsedDigits match {
          case List(major, minor, patch) =>
            // `suffix` is `null` if no suffix is given
            Right((major, minor, patch, Option(suffix)))
          case _ => Left(s"Unexpected error while parsing version `$version`")
        }

      case _ =>
        Left(
          s"Unable to convert string `$version` to a valid release version."
        )
    }
  }

  lazy val releaseChecksSettings: Seq[Setting[_]] = {
    lazy val fixReleaseVersions =
      inputKey[Unit]("Ensure release version and protocol version mappings are defined")
    Seq(
      fixReleaseVersions := {
        import complete.DefaultParsers._
        implicit val log: ManagedLogger = streams.value.log

        val args: Seq[String] = spaceDelimited("<arg>").parsed

        val (major, minor, patch, suffix) = parseSemver(args.head).valueOr { err =>
          throw new IllegalArgumentException(err)
        }

        val releaseVersionArgs =
          ParsedVersion(major = major, minor = minor, patch = patch, suffix = suffix)(
            fileToCheck = new java.io.File(args(1))
          )

        val releaseToProtocolVersionArgs =
          ParsedVersion(major = major, minor = minor, patch = patch, suffix = suffix)(
            new java.io.File(args(2))
          )

        fixReleaseVersion(releaseVersionArgs)
        fixReleaseToProtocolVersion(releaseToProtocolVersionArgs)
      }
    )
  }
}
