/** uses the sbt-buildinfo plugin to share data from the meta-build with the main build
  */
enablePlugins(BuildInfoPlugin)

buildInfoKeys := Seq[BuildInfoKey](
  "daml_libraries_version" -> metabuild.DamlVersions.libraries_version,
  "use_custom_daml_version" -> metabuild.DamlVersions.useCustomDamlVersion,
  "daml_compiler_version" -> metabuild.DamlVersions.libraries_version,
  "dpm_registry" -> metabuild.DamlVersions.dpm_registry,
)
buildInfoPackage := "metabuild"

ThisBuild / scalacOptions += "-feature" // to show warnings in project scala files such as BuildCommon
