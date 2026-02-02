credentials += Credentials(
  null,
  sys.env.getOrElse("MAVEN_HOST", "pkgs.dev.azure.com"),
  sys.env.getOrElse("MAVEN_USERNAME", "digitalasset"),
  sys.env.getOrElse("MAVEN_PASSWORD", ""),
)
