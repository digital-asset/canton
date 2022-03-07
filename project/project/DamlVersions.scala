package metabuild

/** Versions defined here are required in both the metabuild project (our plugins) and our main build.
  * https://www.scala-sbt.org/1.x/docs/Organizing-Build.html
  */
object DamlVersions {

  /** The version of the daml compiler (and in most cases of the daml libraries as well).
    */
  val version: String = "2.1.0-snapshot.20220302.9431.0.96029b7c"

  /** Custom Daml artifacts override version.
    */
  private val customDamlVersion: String = "0.0.0"

  /** The version to use when sdk jvm libraries published to maven repositories.
    */
  val libraries_version: String =
    if (!sys.env.contains("DAML_SNAPSHOT")) version else customDamlVersion

  /** The daml-lf language versions supported by canton.
    * (needed to load the corresponding daml-libs dependencies when building packaged dars)
    */
  val daml_language_versions = Seq("1.14", "1.dev")
}
