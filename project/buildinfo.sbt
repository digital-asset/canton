/** uses the sbt-buildinfo plugin to share data from the meta-build with the main build
  */
enablePlugins(BuildInfoPlugin)

buildInfoKeys := Seq[BuildInfoKey](
  "daml_libraries_version" -> metabuild.DamlVersions.libraries_version,
  "daml_language_versions" -> metabuild.DamlVersions.daml_language_versions,
  "daml_compiler_version" -> metabuild.DamlVersions.version,
)
buildInfoPackage := "metabuild"

ThisBuild / scalacOptions += "-feature" // to show warnings in project scala files such as BuildCommon
