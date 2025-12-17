package metabuild

/** Versions defined here are required in both the metabuild project (our plugins) and our main
  * build. https://www.scala-sbt.org/1.x/docs/Organizing-Build.html
  */
object DamlVersions {

  /** The version of the daml compiler (and in most cases of the daml libraries as well).
    */
  // after changing version, run `sbt updateDamlProjectVersions` to update the `daml.yaml` project files.
  val version: String = "3.5.0-snapshot.20251215.14512.0.v98a3bf71"

  /** The dpm registry to install daml components from.
    */
  val dpm_registry: String = "europe-docker.pkg.dev/da-images/public-unstable"

  /** Custom Daml artifacts override version.
    */
  private val customDamlVersion: String = sys.env.getOrElse("DAML_HEAD_VERSION", "0.0.0")

  /** Whether we want to use the customDamlVersion.
    */
  val useCustomDamlVersion: Boolean = sys.env.contains("DAML_HEAD_VERSION")

  /** The version to use when sdk jvm libraries published to maven repositories.
    */
  val libraries_version: String = if (useCustomDamlVersion) customDamlVersion else version

  /** The daml-lf language versions supported by canton. (needed to load the corresponding daml-libs
    * dependencies when building packaged dars)
    */
  val daml_language_versions = Seq("2.1", "2.2", "2.dev")

}
