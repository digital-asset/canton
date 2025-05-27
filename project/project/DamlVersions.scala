package metabuild

/** Versions defined herere required in both the metabuild project (our plugins) and our main build.
  * https://www.scala-sbt.org/1.x/docs/Organizing-Build.html
  */
object DamlVersions {

  /** The version of the daml compiler (and in most cases of the daml libraries as well).
    */
  // after changing version, run `sbt updateDamlProjectVersions` to update the `daml.yaml` project files.
  val version: String = "2.10.1-snapshot.20250523.13179.0.v7cd2f5cf"

  /** Custom Daml artifacts override version.
    */
  private val customDamlVersion: String = sys.env.getOrElse("DAML_HEAD_VERSION", "0.0.0")

  /** Whether we want to use the customDamlVersion.
    */
  val useCustomDamlVersion: Boolean = sys.env.contains("DAML_HEAD_VERSION")

  /** The version to use when sdk jvm libraries published to maven repositories.
    */
  val libraries_version: String = if (useCustomDamlVersion) customDamlVersion else version

  /** The daml-lf language versions supported by canton.
    * (needed to load the corresponding daml-libs dependencies when building packaged dars)
    */
  val daml_language_versions = Seq("1.14", "1.15", "1.17", "1.dev")

}
