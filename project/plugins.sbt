// Linting plugins
addSbtPlugin("org.wartremover" % "sbt-wartremover" % "3.3.0")
addSbtPlugin("org.wartremover" % "sbt-wartremover-contrib" % "2.2.1")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.4.6")
addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.14.0")
addSbtPlugin("com.lightbend.sbt" % "sbt-java-formatter" % "0.7.0")

// Code coverage
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.2.2")

// documentation site creation
addSbtPlugin("com.github.sbt" % "sbt-site-sphinx" % "1.7.0")

// Ensure we have license headers in all relevant source files
addSbtPlugin("de.heikoseeberger" % "sbt-header" % "5.10.0")

// Required to "reStart" command which improves working with server applications in sbt. Not spray specific.
addSbtPlugin("io.spray" % "sbt-revolver" % "0.10.0")

addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.6")
libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.11.14"

// helps scaladoc resolve links for common scala libraries
addSbtPlugin("com.thoughtworks.sbt-api-mappings" % "sbt-api-mappings" % "3.0.0")

// unifies scaladoc from all modules into a single artifact
addSbtPlugin("com.github.sbt" % "sbt-unidoc" % "0.5.0")
addSbtPlugin("com.eed3si9n" %% "sbt-buildinfo" % "0.13.1")

// Our DamlPlugin needs to read and write values from daml.yaml files
// This is a _very_ simple yaml library as we only need to look at two simple keys
libraryDependencies += "com.esotericsoftware.yamlbeans" % "yamlbeans" % "1.13"

// Assembly plugin to build fat-jars
// We can not upgrade to 2.x due to shapeless: https://github.com/sbt/sbt-assembly/issues/496
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "1.2.0")

// Need better files for building the config file when doing the 'did we break Protobuf compatibility?' check
libraryDependencies += "com.github.pathikrit" %% "better-files" % "3.8.0"

// enable dependency tree plugin (now native as of sbt 1.4.x)
addDependencyTreePlugin

addSbtPlugin("com.github.cb372" % "sbt-explicit-dependencies" % "0.3.1")

// Allows using some convenient Cats syntax in build files
libraryDependencies += "org.typelevel" %% "cats-core" % "2.9.0"

libraryDependencies ++= Seq(
  "io.circe" %% "circe-core" % "0.14.2",
  "io.circe" %% "circe-generic" % "0.14.2",
  "io.circe" %% "circe-parser" % "0.14.2",
)

// JMH for benchmarking purposes
addSbtPlugin("pl.project13.scala" % "sbt-jmh" % "0.4.3")

addSbtPlugin("org.jetbrains.scala" % "sbt-ide-settings" % "1.1.2")

//For easy tar.gz archiving
libraryDependencies += "org.apache.commons" % "commons-compress" % "1.27.1"
