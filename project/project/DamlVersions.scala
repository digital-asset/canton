package metabuild

/** Versions defined here are required in both the metabuild project (our plugins) and our main build.
  * https://www.scala-sbt.org/1.x/docs/Organizing-Build.html
  */
object DamlVersions {

  /** The version of the daml compiler (and in most cases of the daml libraries as well).
    */
  val version: String = "2.4.0-snapshot.20220814.10394.0.660e9f4b"

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
  val daml_language_versions = Seq("1.14", "1.dev")

  /** The version of the VMBC driver libraries in use.
    */
  val vmbc_driver_libraries_base_version: String = "2.3.0-snapshot.20220611.10066.0.458cfc43-0.2"

  /** Custom VMBC driver libraries override version.
    */
  val customVmbcDriverLibrariesVersion = "0.0.0-0.0"

  val vmbc_driver_libraries_version: String =
    if (!sys.env.contains("VMBC_DRIVER_LIBRARIES_SNAPSHOT")) vmbc_driver_libraries_base_version
    else customVmbcDriverLibrariesVersion

}
