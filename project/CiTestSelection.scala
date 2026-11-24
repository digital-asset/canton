import sbt.*
import sbt.Keys.*

/** Lets CI hand a test shard's class list to `testOnly` in a file instead of on the command line.
  *
  * sbt parses `testOnly <names>` once per aggregated project, with tab completion over that
  * project's test names. With a shard's 100+ class names and about 100 projects this takes about a
  * minute of single-threaded CPU before any test starts. With [[EnvVar]] set, a `testOnly` without
  * names selects the classes listed in the file instead, so the command line stays short.
  *
  * The names are turned into filters by sbt's own `Defaults.selectedFilter`, the same function a
  * named `testOnly` uses, so globs work. A `testOnly` that does name classes (for example the
  * failed-only rerun) ignores the file.
  *
  * If this filter is not in effect for a project (a more specific `testOnly / testFilter` wins, or
  * [[EnvVar]] does not reach sbt), the bare `testOnly` runs every test in that project. CI fails
  * the shard when a class outside the file ran (`.github/actions/test/run_tests/action.yml`).
  */
object CiTestSelection {
  val EnvVar = "CANTON_TEST_SELECTION_FILE"

  /** Whitespace-separated test class names (or globs) from the file [[EnvVar]] names; `None` when
    * it is unset. A set variable that points at a missing file is an error rather than running
    * every test.
    */
  private lazy val selection: Option[Seq[String]] =
    sys.env.get(EnvVar).filter(_.trim.nonEmpty).map { path =>
      val file = new File(path)
      if (!file.isFile) sys.error(s"$EnvVar is set, but $path is not a file")
      IO.read(file).split("\\s+").toSeq.filter(_.nonEmpty)
    }

  private[this] def testOnlyFilter(selected: Seq[String]): Seq[String => Boolean] =
    selection match {
      // An empty list must select nothing: sbt treats `testOnly` without names as "all tests".
      case Some(names) if selected.isEmpty && names.isEmpty => Seq(_ => false)
      case Some(names) if selected.isEmpty => Defaults.selectedFilter(names)
      case _ => Defaults.selectedFilter(selected)
    }

  lazy val settings: Seq[Setting[_]] = Seq(
    Global / testOnly / testFilter := testOnlyFilter _
  )
}
