import BuildCommon.{CommunityProjects, DamlProjects}

BuildCommon.sbtSettings

lazy val `community-app` = CommunityProjects.`community-app`
lazy val `community-common` = CommunityProjects.`community-common`
lazy val `community-domain` = CommunityProjects.`community-domain`
lazy val `community-participant` = CommunityProjects.`community-participant`
lazy val demo = CommunityProjects.demo
lazy val blake2b = CommunityProjects.blake2b
lazy val functionmeta = CommunityProjects.functionmeta
lazy val `slick-fork` = CommunityProjects.`slick-fork`
lazy val `akka-fork` = CommunityProjects.`akka-fork`
lazy val `util-external` = CommunityProjects.`util-external`
lazy val `util-internal` = CommunityProjects.`util-internal`
lazy val `sequencer-driver-api` = CommunityProjects.`sequencer-driver-api`
lazy val `sequencer-driver-lib` = CommunityProjects.`sequencer-driver-lib`
lazy val `wartremover-extension` = CommunityProjects.`wartremover-extension`
lazy val `integration-testing-toolkit-junit` = CommunityProjects.`integration-testing-toolkit-junit`
lazy val `daml-fork` = DamlProjects.`daml-fork`
lazy val `daml-copy-macro` = DamlProjects.`daml-copy-macro`
lazy val `daml-copy-protobuf` = DamlProjects.`daml-copy-protobuf`
lazy val `daml-copy-protobuf-java` = DamlProjects.`daml-copy-protobuf-java`
lazy val `google-common-protos-scala` = DamlProjects.`google-common-protos-scala`
lazy val `daml-copy-common` = DamlProjects.`daml-copy-common`
lazy val `daml-copy-testing` = DamlProjects.`daml-copy-testing`
lazy val `daml-copy-participant` = DamlProjects.`daml-copy-participant`

lazy val root = (project in file("."))
  .disablePlugins(WartRemover)
  .aggregate((CommunityProjects.allProjects ++ DamlProjects.allProjects).toSeq.map(_.project): _*)
