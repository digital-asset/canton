import sbt.*
import sbt.Keys.*
import wartremover.WartRemover
import wartremover.WartRemover.autoImport.*

// WartRemover's sbt plugin emits relative excluded paths (for sbt#6027), which don't work
// under Bloop. This plugin supplements them with absolute paths that Bloop's compiler can match.
// It requires WartRemover, so it's automatically disabled for projects that use
// .disablePlugins(WartRemover).
object WartRemoverBloopFix extends AutoPlugin {
  override def requires = WartRemover
  override def trigger = allRequirements

  override def projectSettings: Seq[Def.Setting[?]] = Seq(
    scalacOptions ++= {
      wartremoverExcluded.value.distinct.map(c => s"-P:wartremover:excluded:$c")
    }
  )
}
