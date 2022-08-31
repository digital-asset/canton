import BuildCommon.CommunityProjects

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
lazy val `daml-fork` = CommunityProjects.`daml-fork`
lazy val `wartremover-extension` = CommunityProjects.`wartremover-extension`

lazy val root = (project in file("."))
  .disablePlugins(WartRemover)
  .aggregate(
    `community-app`,
    `community-common`,
    `community-domain`,
    `community-participant`,
    demo,
    blake2b,
    functionmeta,
    `slick-fork`,
    `akka-fork`,
    `daml-fork`,
    `wartremover-extension`,
  )
