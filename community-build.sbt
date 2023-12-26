import BuildCommon.{CommunityProjects, DamlProjects}

BuildCommon.sbtSettings

lazy val `community-app` = CommunityProjects.`community-app`
lazy val `community-app-base` = CommunityProjects.`community-app-base`
lazy val `community-base` = CommunityProjects.`community-base`
lazy val `community-common` = CommunityProjects.`community-common`
lazy val `community-domain` = CommunityProjects.`community-domain`
lazy val `community-participant` = CommunityProjects.`community-participant`
lazy val `community-admin-api` = CommunityProjects.`community-admin-api`
lazy val `community-testing` = CommunityProjects.`community-testing`
lazy val `community-integration-testing` = CommunityProjects.`community-integration-testing`
lazy val demo = CommunityProjects.demo
lazy val blake2b = CommunityProjects.blake2b
lazy val `slick-fork` = CommunityProjects.`slick-fork`
lazy val `pekko-fork` = CommunityProjects.`pekko-fork`
lazy val `util-external` = CommunityProjects.`util-external`
lazy val `util-logging` = CommunityProjects.`util-logging`
lazy val `sequencer-driver-api` = CommunityProjects.`sequencer-driver-api`
lazy val `sequencer-driver-lib` = CommunityProjects.`sequencer-driver-lib`
lazy val `wartremover-extension` = CommunityProjects.`wartremover-extension`
lazy val `daml-copy-macro` = DamlProjects.`daml-copy-macro`
lazy val `daml-copy-protobuf-java` = DamlProjects.`daml-copy-protobuf-java`
lazy val `google-common-protos-scala` = DamlProjects.`google-common-protos-scala`
lazy val `ledger-api` = DamlProjects.`ledger-api`
lazy val `bindings-java` = DamlProjects.`bindings-java`
lazy val `daml-copy-common` = DamlProjects.`daml-copy-common`
lazy val `daml-copy-common-0` = DamlProjects.`daml-copy-common-0`
lazy val `daml-copy-common-1` = DamlProjects.`daml-copy-common-1`
lazy val `daml-copy-common-2` = DamlProjects.`daml-copy-common-2`
lazy val `daml-copy-common-3` = DamlProjects.`daml-copy-common-3`
lazy val `daml-copy-common-4` = DamlProjects.`daml-copy-common-4`
lazy val `daml-copy-common-5` = DamlProjects.`daml-copy-common-5`
lazy val `ledger-common` = CommunityProjects.`ledger-common`
lazy val `ledger-common-dars-lf-v2-dev` = CommunityProjects.`ledger-common-dars-lf-v2-dev`
lazy val `ledger-common-dars-lf-v2-1` = CommunityProjects.`ledger-common-dars-lf-v2-1`
lazy val `daml-errors` = CommunityProjects.`daml-errors`
lazy val `daml-copy-testing` = DamlProjects.`daml-copy-testing`
lazy val `daml-copy-testing-0` = DamlProjects.`daml-copy-testing-0`
lazy val `daml-copy-testing-1` = DamlProjects.`daml-copy-testing-1`
lazy val `ledger-api-core` = CommunityProjects.`ledger-api-core`
lazy val `ledger-json-api` = CommunityProjects.`ledger-json-api`
lazy val `ledger-api-tools` = CommunityProjects.`ledger-api-tools`
lazy val `ledger-api-string-interning-benchmark` =
  CommunityProjects.`ledger-api-string-interning-benchmark`

lazy val root = (project in file("."))
  .disablePlugins(WartRemover)
  .aggregate((CommunityProjects.allProjects ++ DamlProjects.allProjects).toSeq.map(_.project): _*)
