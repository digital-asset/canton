import better.files.File

object Artifactory {
  private val Host = "digitalasset.jfrog.io"

  private val EnvKeyArtifactoryUser = "ARTIFACTORY_USER"
  private val EnvKeyArtifactoryPassword = "ARTIFACTORY_PASSWORD"

  lazy val credentials: NetRc.LoginAndPassword =
    (for {
      netrc <- NetRc.parse((File(System.getProperty("user.home")) / ".netrc").toJava)
      artifactoryNetRc <- netrc
        .get(Host)
        .fold[Either[String, NetRc.LoginAndPassword]](Left("Artifactory credentials not found"))(
          Right(_)
        )
    } yield artifactoryNetRc).getOrElse(
      NetRc.LoginAndPassword(
        sys.env.get(EnvKeyArtifactoryUser),
        sys.env.get(EnvKeyArtifactoryPassword),
      )
    )

  lazy val login: String =
    credentials.login.getOrElse(sys.error(missingCredentialError(EnvKeyArtifactoryUser)))

  lazy val password: String =
    credentials.password.getOrElse(sys.error(missingCredentialError(EnvKeyArtifactoryPassword)))

  private def missingCredentialError(environmentVariableName: String): String =
    s"Either a '<user home>/.netrc' login entry for '$Host' or the '$environmentVariableName' environment variable must be defined"
}
