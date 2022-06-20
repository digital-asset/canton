// Linting plugins
addSbtPlugin("org.wartremover" % "sbt-wartremover" % "2.4.16")
addSbtPlugin("org.wartremover" % "sbt-wartremover-contrib" % "1.3.13")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.4.3")
addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.10.0")

// Code coverage
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.9.2")

// documentation site creation
addSbtPlugin("com.typesafe.sbt" % "sbt-site" % "1.4.0")

// Ensurewe have license headeres in all source
addSbtPlugin("de.heikoseeberger" % "sbt-header" % "5.6.0")

// Required to "reStart" command which improves working with server applications in sbt. Not spray specific.
addSbtPlugin("io.spray" % "sbt-revolver" % "0.9.1")

addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.6")
libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.11.8"

// Required to for packaging into a tarball and zip archive
addSbtPlugin("org.xerial.sbt" % "sbt-pack" % "0.13")

// helps scaladoc resolve links for common scala libraries
addSbtPlugin("com.thoughtworks.sbt-api-mappings" % "sbt-api-mappings" % "3.0.0")

// unifies scaladoc from all modules into a single artifact
addSbtPlugin("com.eed3si9n" % "sbt-unidoc" % "0.4.3")

addSbtPlugin("com.eed3si9n" %% "sbt-buildinfo" % "0.9.0")

// Our DamlPlugin needs to read and write values from daml.yaml files
// This is a _very_ simple yaml library as we only need to look at two simple keys
libraryDependencies += "com.esotericsoftware.yamlbeans" % "yamlbeans" % "1.13"

// Assembly plugin to build fat-jars
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.15.0")

// Need better files for building the config file when doing the 'did we break Protobuf compatibility?' check
libraryDependencies += "com.github.pathikrit" %% "better-files" % "3.8.0"

// enable dependency tree plugin (now native as of sbt 1.4.x)
addDependencyTreePlugin

addSbtPlugin("com.github.cb372" % "sbt-explicit-dependencies" % "0.2.16")

// Allows using some convenient Cats syntax in build files
libraryDependencies += "org.typelevel" %% "cats-core" % "2.6.1"
