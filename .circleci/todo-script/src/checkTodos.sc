import java.io.PrintWriter

import scala.collection.mutable.ListBuffer
import scala.collection.{Map, SortedMap}
import scala.sys.process._
import scala.util.matching.Regex

/**  This script depends on `gh` (github cli) and awk
  *
  *  Intentionally not using the .ci/nix-exec script because some commands in this script do not work properly in a pure
  *  nix environment (i.e. nix-shell using the `--pure` flag). Use [[nixify]]
  *
  *  Current limitations of the TODO checker:
  *    - For `.rst` files, TODO blocks are considered to be the range from each instance of `todo::` to the next blank
  *      line. This is different to the actual TODO block in the `.rst` file---for example, a `rst` TODO block may contain
  *      blank lines followed by more content of the block.
  */

def nixify(command: String): String = {
  s"""nix-shell -I nixpkgs=./nix/nixpkgs.nix --run "$command""""
}

// check if we should run at all
val disableCheckerFile = "disabled-todo-checker"
if (new java.io.File(disableCheckerFile).exists()) {
  println(
    s"todo-checker has been disabled by the presence of the following file: `$disableCheckerFile`"
  )
  sys.exit(0)
}

val prNumO: Option[String] = {
  val pullRequestEnv = sys.env.getOrElse("CIRCLE_PULL_REQUEST", "")

  "([0-9]+)".r
    .findFirstMatchIn(pullRequestEnv)
    .map(_.matched)
}

val runningInCI = sys.env.contains("CI")

// This is done early because we also want to do this check on release lines
ensureNoTodosInSqlFiles()

// The following checks rely on information retrieved from CircleCI and GitHub on the branch
if (runningInCI) {
  val branch = sys.env.getOrElse("CIRCLE_BRANCH", "").strip()
  val baseBranch = prNumO.map { prNum => nixify(s"gh pr view $prNum --json baseRefName --jq .baseRefName").!!.strip() }
  val releaseLineStr = "release-line"
  val onReleaseLine = baseBranch.exists(_.contains(releaseLineStr)) || branch.contains(releaseLineStr)
  val debugInfo = s"[debug info: branch '$branch', base branch '$baseBranch']"

 // On a release line we disable the todo-checker
  if (onReleaseLine) {
    if (baseBranch.isDefined) {
      println(
        s"todo-checker has been disabled, because this CI run is for a PR based on release line branch $baseBranch $debugInfo"
      )
    } else {
      println(
        s"todo-checker has been disabled, because this CI run is for a commit on release line branch $branch $debugInfo"
      )
    }
    sys.exit(0)
  }

  // The baseBranch can be empty when CI is running but the corresponding PR has not been created yet to obtain the base branch from the PR
  if (baseBranch.isEmpty) {
    println(s"NOTE: todo-checker is missing information on the base branch for branch $branch (PR not available).")
    println("If the following rebase to main step fails then re-run this job after the PR was created.\n")
  }

  // if we are not on main, rebase to main to avoid race condition with todos already closed on main
  if (branch != "main") {
    println("rebasing to main to avoid flagging already closed issues")
    nixify("git config user.email canton@digitalasset.com").!!.strip()
    nixify("git fetch origin main").!!.strip()
    nixify("git merge -X theirs origin/main -m Merged --commit").!!.strip()
  }
}

sealed trait Bucket extends Ordered[Bucket] {

  /** The name of the bucket
    */
  val name: String

  /** Provides an ordering between different [[Bucket]] classes
    */
  val classPosition: Int

  /** Provides an ordering between instances of the same [[Bucket]] class
    */
  val withinClassPosition: Int

  override def compare(that: Bucket): Int = {
    val compareCategories = classPosition.compareTo(that.classPosition)
    if (compareCategories == 0)
      withinClassPosition.compareTo(that.withinClassPosition)
    else compareCategories
  }
}

final case class TagBucket(tag: String) extends Bucket {
  override val name = tag
  override val classPosition = 1
  override val withinClassPosition = tag.hashCode
}

final case class MilestoneBucket(number: Int) extends Bucket {
  override val name = "Milestone " + number.toString
  override val classPosition = 2
  override val withinClassPosition = number
}

final case class IssueBucket(number: Int) extends Bucket {
  override val name = "Issue " + number.toString
  override val classPosition = 3
  override val withinClassPosition = number
}

object UnknownBucket extends Bucket {
  override val name = "Unknown category"
  override val classPosition = 4
  override val withinClassPosition = 0
}

object UnassignedBucket extends Bucket {
  override val name = "No category assigned"
  override val classPosition = 5
  override val withinClassPosition = 0
}

sealed trait RegexCategory {
  val regex: Regex
  def getBucket(name: String): Bucket
}

final case class Tag(tags: List[String]) extends RegexCategory {

  override val regex = {
    require(tags.nonEmpty)
    val anyTag = "(?i)" + tags.map(t => s"($t)").mkString("|")
    anyTag.r
  }

  override def getBucket(name: String) = TagBucket(tags.head)
}

object Issue extends RegexCategory {
  override val regex = "[i#][0-9]+".r

  override def getBucket(str: String) = numsToIssue(str)
}

object Milestone extends RegexCategory {
  // Only allow for specific milestones because TODOs referring to milestones are hard to manage.
  override val regex = "M41|M99|M98".r

  override def getBucket(str: String) = MilestoneBucket(str.drop(1).toInt)
}

object GithubIssueLink extends RegexCategory {
  override val regex: Regex = "canton/issues/[0-9]+".r

  override def getBucket(str: String): Bucket = numsToIssue(str)
}

def numsToIssue(str: String): IssueBucket = {
  "[0-9]+".r.findFirstMatchIn(str) match {
    case None => throw new RuntimeException("The given string isn't an issue")
    case Some(m) => IssueBucket(m.matched.toInt)
  }
}

val tags: List[RegexCategory] = List(
  Tag(List("test-coverage")),
  Tag(List("GA", "1.0.0", "1.0")),
)

val allRegexps: List[RegexCategory] = tags ++ List(Issue, Milestone)

val todoPatterns = Seq("TODO", "XXX", "FIXME")
val todoPatternRegexpStr = todoPatterns.map(str => s"($str)").mkString("|")

def addToBucket(
    acc: Map[Bucket, List[String]],
    bucket: Bucket,
    line: String,
): Map[Bucket, List[String]] =
  acc.concat(Seq(bucket -> (line :: acc.getOrElse(bucket, List()))))

def matchTODOWithBuckets(line: String, bucketsForLine: String): List[(Bucket, String)] = {

  val allAttempts = allRegexps
    .map(rgx =>
      rgx.regex.findFirstMatchIn(bucketsForLine).map(m => rgx.getBucket(m.matched) -> line)
    )
    .collect { case Some(x) => x }

  if (allAttempts.isEmpty) List(UnknownBucket -> line) else allAttempts
}

def processScalaStyleTodo(line: String): List[(Bucket, String)] = {
  val identifierRegex = ("(" + todoPatternRegexpStr + """)(.*?)\((.+?)\)""").r
  identifierRegex.findFirstMatchIn(line) match {
    case Some(contents) => matchTODOWithBuckets(line, contents.matched)
    case None => List(UnassignedBucket -> line)
  }
}

def processRstStyleTodo(line: String): List[(Bucket, String)] = {
  val ppLine = line.replace("<SEP>", "\n")

  val allIssues = line
    .split("<SEP>")
    .toList
    .map { issue =>
      GithubIssueLink.regex
        .findFirstMatchIn(issue)
        .map(m => GithubIssueLink.getBucket(m.matched) -> ppLine)
    }
    .collect { case Some(x) => x }

  if (allIssues.isEmpty) List(UnassignedBucket -> ppLine)
  else allIssues
}

def tableToString(table: Map[Bucket, List[String]]): String = {
  table.toList
    .map{ case (bucket, todoList) => bucket.name + "\n\n" + todoList.mkString("\n") }
    .mkString("\n\n")
}

def writeToFile(content: String, filename: String): Unit = {
  new PrintWriter(filename) {
    write(content)
    close()
  }
}

val fixedIssuesCurrentPR: Set[Int] = {
  prNumO.fold(Set.empty[Int])({ prNum =>
    val prInfo = nixify(s"gh pr view $prNum --json body --jq .body").!!
    val issueCloseKeywords = "((fixes)|(closes))"
    val fixedIssueRegexStr = "(?i)" + issueCloseKeywords + "(\\s+)(#)([0-9]+)"
    val fixedIssueRegex = fixedIssueRegexStr.r
    fixedIssueRegex
      .findAllMatchIn(prInfo)
      .map(m => "([0-9]+)".r.findFirstMatchIn(m.matched).get.matched.toInt)
      .toSet
  })
}

if (!sys.env.contains("GITHUB_TOKEN")) {
  println("Hub tool needs GitHub credentials in order to work")
  println(
    "Since env var `GITHUB_TOKEN` is not defined, ensure the credentials are found in ~/.config/hub"
  )
}

val openIssues: Set[Int] = nixify("gh issue list --limit 2500 --json number --jq '.[].number'").!!.split("\n")
  .map(_.toInt)
  .toSet

// Issues that have dangling TODOs (e.g., in sql files)
val ignoredIssues: Set[Int] = Set(282923)

println(s"Found ${openIssues.size} open issues: ${openIssues.mkString(", ")}")

val projectRoot = "." // CI scripts are called from the project root

val scalaStyleExcludeDirectories =
  Seq(
    "TODO-script",
    "lib",
    "log",
    "todo-out",
    "theory",
    "docs",
    "docs-open",
    ".git",
    ".idea",
    "target",
    "3rdparty",
    "build",
    "contributing",
    "daml",
  )

// Different versions of grep (e.g. on Mac and Ubuntu) behave differently. This grep call should be tested for both
// platforms if it is to be run locally. Right now it's only tested for Ubuntu because the CI uses Ubuntu.
def grepForPattern(pattern: String): Seq[String] = {
  val excludeDirectories = scalaStyleExcludeDirectories ++ Seq("release-notes")

  Seq("grep", "-r") ++ excludeDirectories.map(dir => s"--exclude-dir=$dir") ++ Seq(
    "-I", // Ignore binary files
    "--exclude=*.png",
    "--exclude=*checkTodos.sc",
    "--exclude=*UNRELEASED.md",
    pattern,
    projectRoot,
  )
}

object ErrorCollector extends ProcessLogger {
  private val allErrors = ListBuffer[String]()
  def errors() = allErrors.mkString("\n\n")
  def anyError(): Boolean = allErrors.nonEmpty

  // Do not use ProcessLogger.apply(_) to work around:
  // https://github.com/lihaoyi/Ammonite/issues/1120
  override def out(s: => String): Unit = println(s)
  override def err(s: => String): Unit = allErrors += s
  override def buffer[T](f: => T) = f
}

val grepCommands = todoPatterns.map(grepForPattern)
val grepLines = grepCommands.flatMap({ command =>
  nixify(command.mkString(" ")).lazyLines_!(ErrorCollector)
})

def checkGreppingRstFilesWorks(awkGrep: String): Unit = {
  println("Grepping for todos in RST files ...")
  require(awkGrep.! == 0, s"Grepping for todos in RST files failed; executed command: $awkGrep")
}

val awkGrep = nixify(s"grep --include='*.rst' -rl '.. todo::' docs-open")
checkGreppingRstFilesWorks(awkGrep)
val awkRun = nixify("xargs awk -f .circleci/todo-script/rst_script.awk")
val awkCommand = awkGrep #| awkRun
val awkLines = awkCommand.lazyLines_!(ErrorCollector)

if (ErrorCollector.anyError())
  throw new RuntimeException("grep or awk failed with errors:\n\n" + ErrorCollector.errors())

def initialMapping(): Map[Bucket, List[String]] = SortedMap()
def pairsToMap(pairs: List[(Bucket, String)]): Map[Bucket, List[String]] =
  pairs.foldLeft(initialMapping()) { case (acc, (bucket, string)) =>
    addToBucket(acc, bucket, string)
  }

// These are matched against the result obtained from grep
val scalaStyleAllowedPrefixes = Seq(
  "./community/ledger/ledger-api-tests/tool/src/main/resources/logback.xml:            <pattern>%date{\"yyyy-MM-dd'T'HH:mm:ss.SSSXXX\", UTC} %-5level %logger{5}@[%-4.30thread] - %msg%n</pattern>",
  "./community/ledger-api-bench-tool/src/main/resources/logback.xml:            <pattern>%date{\"yyyy-MM-dd'T'HH:mm:ss.SSSXXX\", UTC} %-5level %logger{5}@[%-4.30thread] - %msg%n</pattern>",
  "./community/ledger/error/src/main/resources/logback.xml:            <pattern>%date{\"yyyy-MM-dd'T'HH:mm:ss.SSSXXX\", UTC} %-5level %logger{5}@[%-4.30thread] - %msg%n</pattern>",
  "./community/ledger/ledger-common/src/test/resources/test-certificates/ocsp.crt:n6zpdBWthvmazUr/06UcQFrsiX1qBrFTatFIt8lmUoGy6kdXXXysmO70dLDUF2Px",
)
val scalaStyleIssuesTable: List[(Bucket, String)] =
  grepLines.toList
    .filterNot(line => scalaStyleAllowedPrefixes.exists(line.startsWith))
    .flatMap(line => processScalaStyleTodo(line))
val rstStyleIssuesTable: List[(Bucket, String)] =
  awkLines.toList.flatMap(line => processRstStyleTodo(line))
val table = pairsToMap(scalaStyleIssuesTable ++ rstStyleIssuesTable)

val issuesNotOpen = table.filter {
  case (IssueBucket(i), _) => ((!openIssues.contains(i)) || fixedIssuesCurrentPR.contains(i)) && !ignoredIssues.contains(i)
  case _ => false
}

val todosWithoutReference = table.filter { case (bucket, _) =>
  bucket match {
    case UnknownBucket | UnassignedBucket => true
    case _ => false
  }
}

def purple(str: String): String = {
  val ANSI_PURPLE = "\u001B[35m"
  val ANSI_RESET = "\u001B[0m"
  ANSI_PURPLE + str + ANSI_RESET
}

val strTable = tableToString(table)
val strNoOwner = purple("TODOs without owners:\n\n") + tableToString(todosWithoutReference)
val strNotOpen = purple("TODOs for closed issues:\n\n") + tableToString(issuesNotOpen)

new java.io.File("todo-out").mkdirs
writeToFile(strTable, "todo-out/todos")

val allTodos = table.values.toList.flatten
// There may be duplicates in allTodos when a TODO occurs in multiple categories
// For example:
// TODO(ratko/matthias): this would show up twice

val uniqueTodos = Set.apply(allTodos: _*)
val todoCount = uniqueTodos.size
writeToFile(todoCount.toString, "todo-out/count")

if (todosWithoutReference.nonEmpty && issuesNotOpen.nonEmpty) {
  val output = List(strNoOwner, strNotOpen).mkString("\n\n")
  Console.err.println(output)
  sys.exit(1)
}

if (todosWithoutReference.nonEmpty) {
  Console.err.println(strNoOwner)
  sys.exit(1)
}

if (issuesNotOpen.nonEmpty) {
  Console.err.println(strNotOpen)
  sys.exit(1)
}

/*
Check that sql files outside in /stable/ don't contain any TODO.
The reason is that comments are part of Flyway checksum, so we can never remove them.
*/
def ensureNoTodosInSqlFiles(): Unit = {
  val todosInSqlFiles = nixify("grep -rn 'TODO(' --include='*.sql' || true").!!.split("\n").toList
    .filter(_.contains("/stable/"))
    .filterNot(_.contains("/target/"))
    .filterNot(_.contains("TODO(#282923)")) // we need to live with this one

  if(todosInSqlFiles.nonEmpty) {
    val message = "The following TODOs are found in SQL files; please remove them"
    Console.err.println((message +: todosInSqlFiles).mkString("\n\n"))
    sys.exit(1)
  }
}
