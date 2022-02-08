/** Allows us to expose build configuration of the metaproject to our main build project.
  * Particularly to allow us to share the same version of Daml libraries between build plugins and dependencies.
  */
addSbtPlugin("com.eed3si9n" %% "sbt-buildinfo" % "0.9.0")
