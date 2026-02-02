import os.{Path, /}
import java.time.LocalDate

/**
  A script for docs replication.
  It creates a PR in the docs repository with the sphinx docs.
  It assumes the following:
  - The docs have been generated already (the preprocessed-sphinx folder)
  - The auth for Github CLI is already done
  */

def makeDocsRepoName(env: String): String = env match {
  case "prod" => "DACH-NY/docs-website"
  case _ => "DACH-NY/docs-website-test"
}

def makeDocsRepoUrl(env: String, githubAuth: String): String = {
  s"https://$githubAuth@github.com/${makeDocsRepoName(env)}.git"
}

@main
def run(env: String, docsOpenDir: Path, githubAuth: String, shortVersion: String, fullVersion: String, dryRun: Boolean = false) = {
  val preprocessedSphinxDocsDir = docsOpenDir / "target" / "preprocessed-sphinx"
  val workDir = docsOpenDir / "src" / "replication" / "workdir"
  val docsRepoDir = workDir / "docs-website"
  val docsRepoCantonDocsDir = docsRepoDir / "docs" / "replicated" / "canton" / shortVersion

  val docsRepoUrl = makeDocsRepoUrl(env, githubAuth)

  val newBranchName = s"automatic-update-canton-$fullVersion"
  val commitMessage = s"Automatic update (Canton): $fullVersion"

  if (!os.exists(workDir)) {
    os.makeDir(workDir)
  }

  if (os.exists(docsRepoDir)) {
    os.proc("git", "checkout", "main").call(cwd = docsRepoDir)
    os.proc("git", "pull").call(cwd = docsRepoDir)
  } else {
    os.proc("git", "clone", docsRepoUrl, "docs-website").call(cwd = workDir)
    os.proc("git", "remote", "set-url", "origin", docsRepoUrl).call(cwd = docsRepoDir)
  }

  os.remove.all(docsRepoCantonDocsDir)
  os.copy(preprocessedSphinxDocsDir, docsRepoCantonDocsDir)

  val gitStatus = os.proc("git", "status").call(cwd = docsRepoDir).out.text()
  val hasChanges = !gitStatus.contains("nothing to commit")

  if (hasChanges) {
    os.proc("git", "checkout", "-b", newBranchName).call(cwd = docsRepoDir)
    os.proc("git", "add", ".").call(cwd = docsRepoDir)
    os.proc("git", "commit", "-m", commitMessage).call(cwd = docsRepoDir)

    os.proc("git", "push", "--set-upstream", "origin", newBranchName).call(cwd = docsRepoDir)
    os.proc(Seq("gh", "pr", "create", "--title", commitMessage, "--repo", docsRepoUrl, "--body", "") ++ (if (dryRun) Seq("--dry-run") else Seq.empty)).call(cwd = docsRepoDir)
  }
}
