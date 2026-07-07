val mavenHost = sys.env.getOrElse("MAVEN_HOST", "pkgs.dev.azure.com")
val mavenUser = sys.env.getOrElse("MAVEN_USERNAME", "digitalasset")
// Fail loud: an empty password silently produces broken credentials that cascade
// into hundreds of generic download failures with no obvious root cause.
val mavenPass = sys.env.getOrElse(
  "MAVEN_PASSWORD",
  sys.error("MAVEN_PASSWORD must be set when USE_MAVEN_MIRROR=true"),
)

// Coursier uses host-only matching (null realm). This file is read by Coursier
// via -Dsbt.coursier.credentials. It is NOT parsed by Ivy (sbt-license-report).
// Ivy credentials are provided separately as a Java .properties file generated
// at runtime in sbt-ci-wrapper.sh via -Dsbt.credentials.file.
credentials += Credentials(null, mavenHost, mavenUser, mavenPass)
