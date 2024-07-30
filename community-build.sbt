import BuildCommon.CommunityProjects.`daml-tls`
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
lazy val `magnolify-addon` = CommunityProjects.`magnolify-addon`
lazy val `util-external` = CommunityProjects.`util-external`
lazy val `util-logging` = CommunityProjects.`util-logging`
lazy val `sequencer-driver-api` = CommunityProjects.`sequencer-driver-api`
lazy val `sequencer-driver-api-conformance-tests` =
  CommunityProjects.`sequencer-driver-api-conformance-tests`
lazy val `sequencer-driver-lib` = CommunityProjects.`sequencer-driver-lib`
lazy val `community-reference-driver` = CommunityProjects.`community-reference-driver`
lazy val `wartremover-extension` = CommunityProjects.`wartremover-extension`
lazy val `google-common-protos-scala` = DamlProjects.`google-common-protos-scala`
lazy val `ledger-api-value` = DamlProjects.`ledger-api-value`
lazy val `ledger-api` = DamlProjects.`ledger-api`
lazy val `bindings-java` = DamlProjects.`bindings-java`
lazy val `ledger-common` = CommunityProjects.`ledger-common`
lazy val `ledger-common-dars-lf-v2-dev` = CommunityProjects.`ledger-common-dars-lf-v2-dev`
lazy val `ledger-common-dars-lf-v2-1` = CommunityProjects.`ledger-common-dars-lf-v2-1`
lazy val `daml-errors` = CommunityProjects.`daml-errors`
lazy val `daml-tls` = CommunityProjects.`daml-tls`
lazy val `dam-grpc-utils` = CommunityProjects.`daml-grpc-utils`
lazy val `daml-adjustable-clock` = CommunityProjects.`daml-adjustable-clock`
lazy val `ledger-api-core` = CommunityProjects.`ledger-api-core`
lazy val `ledger-json-api` = CommunityProjects.`ledger-json-api`
lazy val `ledger-api-tools` = CommunityProjects.`ledger-api-tools`
lazy val `ledger-api-string-interning-benchmark` =
  CommunityProjects.`ledger-api-string-interning-benchmark`

lazy val root = (project in file("."))
  .disablePlugins(WartRemover)
  .aggregate((CommunityProjects.allProjects ++ DamlProjects.allProjects).toSeq.map(_.project)*)
