#!/usr/bin/env amm

import scala.sys.process._
import java.io.File
import scala.util.matching.Regex

val repoPathsPath = "canton_repo_paths"

@main
def run(params: String*) = {
  if (params.isEmpty || params.contains("help") || params.contains("--help")) {
    printHelp()
    sys.exit(0)
  }

  val commit = params(0)
  val branches = params.drop(1)

  val repoPaths = getRepoPaths(branches)

  repoPaths.foreach { case (baseBranch, repoPath) =>
    println(s"Backporting $commit to $baseBranch using repo in $repoPath")

    new BackPort(commit, baseBranch, repoPath).run() match {
      case Left(error) => println(s"Failure: $error")
      case Right(()) => ()
    }

    println("--------------------")
  }
}

private def printHelp() = println(
  s"""The goal of this tool is to port a commit to several branches.
     |
     |The following parameters need to be passed to this script:
     |- hash of the commit
     |- branches to which the commit should be cherry-picked (separated by a space)
     |
     |Example:
     |scripts/backport.scala da28cf79f714fad9899900d2fe8928e6bb35f1d3 main release-line-3.2 release-line-2.9
     |
     |Prerequisite:
     |File `canton_repo_paths` should exist in the root of the repo.
     |It should contain one row per repo with the branch and the local path, e.g.:
     |  release-line-2.9 /home/rgugliel/github/2.9-canton
     |  release-line-2.10 /home/rgugliel/github/2.10-canton
     |  main /home/rgugliel/github/canton
     |  release-line-3.2 /home/rgugliel/github/3.2-canton
     |
     |In case of error, the script will try to proceed with the next port.
     |""".stripMargin
)

/** @param commitHash Hash of the commit to port
 * @param baseBranch Base branch (e.g., `release-line-2.9`, `main`, `release-line-3.2`)
 * @param repoPath Path to the local repository
 */
class BackPort(commitHash: String, baseBranch: String, repoPath: String) {
  private lazy val commitHashShort = commitHash.take(8)
  private lazy val directory = new File(repoPath)
  private lazy val isRepoClean = Process("git status --porcelain", directory).!!.isEmpty
  private lazy val targetBranch = s"port-$commitHashShort-$baseBranch"

  private lazy val currentBranch = Process("git branch --show-current", directory).!!
    // There is a new line at the end of the result
    .replace("\n", "")

  private val prUrl =
    s"https://github.com/DACH-NY/canton/compare/$baseBranch...$targetBranch?expand=1"

  /*
   Some commands are chatty and --quiet does not exist and/or is not efficient.
   */
  private lazy val noopProcessLogger = new ProcessLogger {
    override def out(s: => String): Unit = ()
    override def err(s: => String): Unit = ()
    override def buffer[T](f: => T): T = f
  }

  def run(): Either[String, Unit] =
    if (currentBranch == targetBranch) {
      if (isRepoClean) {
        println("Nothing to do")
        Right(())
      } else
        completeCherryPick()
    } else performCherryPick()

  /** Attempt to finish the cherry pick (provided that conflicts have been resolved)
   * @return
   */
  private def completeCherryPick(): Either[String, Unit] = {
    println("Attempting to finish the cherrypick")
    for {
      _ <- Either.cond(
        Process(s"git cherry-pick --continue", directory).!<(noopProcessLogger) == 0,
        (),
        s"Unable to continue the cherry pick. Ensure conflicts have been marked as resolved.",
      )
      _ <- Either.cond(
        Process(s"git push --quiet", directory).!<(noopProcessLogger) == 0,
        (),
        s"Unable to push branch $targetBranch",
      )
      _ = println(s"Success. Visit $prUrl")
    } yield ()
  }

  /** Performs the cherry pick
   * - Checkout the new branch
   * - Cherry pick
   * - Push
   * @return
   */
  private def performCherryPick(): Either[String, Unit] = for {
    _ <- Either.cond(isRepoClean, (), s"Repo $repoPath is not clean")
    _ <- Either.cond(
      Process(s"git checkout -q $baseBranch", directory).! == 0,
      (),
      s"Unable to checkout $baseBranch",
    )
    _ <- Either.cond(
      Process(s"git pull --quiet", directory).! == 0,
      (),
      s"Unable to pull on $baseBranch",
    )
    _ <- Either.cond(
      Process(s"git checkout --quiet -b $targetBranch", directory).! == 0,
      (),
      s"Unable to create new branch $targetBranch. Try do delete it.",
    )
    _ <- Either.cond(
      Process(s"git cherry-pick $commitHash", directory).!<(noopProcessLogger) == 0,
      (),
      s"""
         |Unable to cherry-pick $commitHash. Conflicts in $repoPath need to be solved manually.
         |Run the script once again after fixing the conflicts and marking them as resolved.
         |""".stripMargin,
    )
    _ <- Either.cond(
      Process(s"git push --quiet", directory).!<(noopProcessLogger) == 0,
      (),
      s"Unable to push branch $targetBranch",
    )
    _ = println(s"Success. Visit $prUrl")
  } yield ()
}

private def getRepoPaths(branches: Seq[String]): Seq[(String, String)] = {
  val file = new File(repoPathsPath)
  if (!file.exists())
    throw new IllegalArgumentException(s"File $repoPathsPath does not exist")

  val source = scala.io.Source.fromFile(repoPathsPath)
  val lines = source.getLines()

  val versionToRepo: Map[String, String] = lines.flatMap { line =>
    val versionRepoPath = "([a-z0-9\\-\\.]+) ([^\n]+)".r

    line match {
      case versionRepoPath(branch, repo) => List((branch, repo.trim))
      case _ =>
        println(s"Unable to read `branch pathToRepo` from: $line. Discarding.")
        List()
    }
  }.toMap

  branches.map { version =>
    (
      version,
      versionToRepo.getOrElse(
        version,
        throw new IllegalArgumentException(s"Unable to find repo path for $version")
      )
    )
  }
}
