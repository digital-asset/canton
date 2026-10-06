// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import better.files.File
import cats.data.NonEmptyList
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.BufferedProcessLogger
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.version.{
  ProtocolVersion,
  ProtocolVersionCompatibility,
  ReleaseVersion,
}
import com.digitalasset.nonempty.NonEmpty

import java.nio.file.{Files, Paths}
import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

/** A collection of small utilities for tests that have no obvious home */
@SuppressWarnings(Array("com.digitalasset.canton.RequireBlocking"))
object ReleaseUtils {
  final case class TestedRelease(
      releaseVersion: ReleaseVersion,
      protocolVersions: NonEmpty[List[ProtocolVersion]],
  )

  /** Given a list of [[TestedRelease]] returns a new list of [[TestedRelease]] where each element
    * is a release version together with one of its supported protocol version.
    *
    * Example:
    *   - Given [(2.7.9, [3, 4, 5]), (2.9.0, [5, 6])
    *   - Returns [(2.7.9, [3]), (2.7.9, [4]), (2.7.9, [5]), (2.9.0, [5]), (2.9.0, [6])]
    */
  def zipReleasesWithProtocolVersions(releases: List[TestedRelease]): List[TestedRelease] =
    releases.flatMap { release =>
      release.protocolVersions.map(e => release.copy(protocolVersions = NonEmpty.mk(List, e)))
    }

  /** Spreads the elements of `list` across exactly `numberOfShards` shards in round-robin order, so
    * the element at 0-based position `i` lands in shard `i % numberOfShards`.
    *
    * This keeps the shards balanced by count and, crucially, keeps any run of heavy elements from
    * all landing in the same shard. The data continuity dumps are ordered by release, so contiguous
    * chunking used to pile the heavier dumps into one class and make it the long pole. Dealing the
    * dumps out in turn instead evens the classes out. The set of elements is unchanged, only their
    * grouping.
    *
    * The result always has exactly `numberOfShards` sub-lists. When there are fewer elements than
    * shards, the trailing shards are empty.
    *
    * Examples:
    *   - List(1, 2, 3, 4) and n=2 => List(List(1, 3), List(2, 4))
    *   - List(1, 2, 3) and n=4 => List(List(1), List(2), List(3), List())
    */
  def shard[E](list: NonEmptyList[E], numberOfShards: PositiveInt): List[List[E]] = {
    val n = numberOfShards.value
    val byShard = list.toList.zipWithIndex.groupBy { case (_, index) => index % n }
    (0 until n)
      .map(shardIndex => byShard.getOrElse(shardIndex, Nil).map { case (element, _) => element })
      .toList
  }

  /** Matches the `<version>` part of a `releases/canton-open-source-<version>.tar.gz` object that
    * is an actual Canton release, namely:
    *   - stable: `3.5.2`
    *   - release candidate: `3.5.3-rc1`
    *   - snapshot: `3.6.0-snapshot.20260929.20331.0.v07b3f95b` (older ones carry an extra leading
    *     number, e.g. `3.0.0-snapshot.100000000.20240214.12557.0.v052dc804`, and/or a git hash
    *     without the `v` prefix, e.g. `2.7.0-snapshot.20230522.10377.0.6e5cea28`)
    *   - ad-hoc: `3.4.12-ad-hoc.100000000.20240215.12560.0.vb11aba02`, also spelled `adhoc` and
    *     `ad-hoc-snapshot`
    *
    * This deliberately ignores artifacts with names ending with -api, -proto etc.
    */
  private val releaseName =
    raw"""\d+\.\d+\.\d+(?:-rc\d+|-(?:ad-?hoc-)?(?:snapshot|ad-?hoc)(?:\.\d+)+\.v?[0-9a-f]+)?""".r

  private[util] def isReleaseName(version: String): Boolean = releaseName.matches(version)

  /** Lists `canton-open-source-*` releases published in the public S3 bucket.
    *
    * Returned versions are deduplicated and parsed as [[ReleaseVersion]]; entries that are not
    * release names (see [[releaseName]]) or that fail to parse are silently ignored.
    *
    * @param includeAdHoc:
    *   default to `false`. If false, ad-hoc releases are excluded
    * @param includeSnapshot:
    *   default true. If `false`, snapshot releases are excluded
    */
  def listAllReleases(
      includeAdHoc: Boolean = false,
      includeSnapshot: Boolean = true,
  ): Seq[ReleaseVersion] = {
    import scala.sys.process.*
    val output =
      """aws s3api list-objects-v2 --bucket canton-public-releases --prefix releases/canton-open-source- --query 'Contents[].Key' --output text --no-sign-request""".!!
    output
      .split("\\s+")
      .toSeq
      .filter(_.endsWith(".tar.gz"))
      .map(_.stripPrefix("releases/canton-open-source-").stripSuffix(".tar.gz"))
      .filter(isReleaseName)
      .flatMap(ReleaseVersion.create(_).toOption)
      .filter(r => (includeAdHoc || !r.isAdHoc) && (includeSnapshot || !r.isSnapshot))
      .distinct
  }

  // All previous stable releases minus releases that support only deleted protocol versions
  lazy val previousSupportedStableReleases: List[ReleaseVersion] =
    File("release-notes/")
      .list(file => file.name.startsWith(ReleaseVersion.current.major.toString))
      .map(_.name.replace(".md", ""))
      .map(ReleaseVersion.tryCreate)
      .filter { releaseVersion =>
        val protocolVersions =
          ProtocolVersionCompatibility.supportedProtocols(
            includeDevVersion = false,
            includeAlphaVersions = false,
            includeBetaVersions = true,
            release = releaseVersion,
          )
        releaseVersion.isStable && protocolVersions.exists(pv => !pv.isDeleted)
      }
      .toList
      .sorted

  /** The first time we attempt to get a release, a future is inserted into the map. This allows to
    * synchronize between different requests for the same release.
    */
  private val releasesRetrieval: TrieMap[ReleaseVersion, Future[String]] = TrieMap.empty
  private val lock = new Mutex()

  /** If the .tar.gz corresponding to release is not found locally, attempts to download it from
    * artifactory. Then, extract the .tar.gz file.
    * @param release
    *   Version that needs to be retrieved
    * @return
    *   Directory containing the downloaded release
    */
  @SuppressWarnings(Array("com.digitalasset.canton.SynchronizedFuture"))
  def retrieve(
      release: ReleaseVersion
  )(implicit elc: ErrorLoggingContext, ec: ExecutionContext): Future[String] =
    lock.exclusive {
      releasesRetrieval.get(release) match {
        case Some(releaseRetrieval) => releaseRetrieval
        case None =>
          val releaseRetrieval = Future(downloadAndExtract(release))
          releasesRetrieval.put(release, releaseRetrieval).discard
          releaseRetrieval
      }
    }

  /** This method should not be called concurrently for the same release. Use [[retrieve]] method
    * above instead.
    *
    * @param release
    *   Release version th
    * @return
    *   Directory containing the downloaded release
    */
  private def downloadAndExtract(
      release: ReleaseVersion
  )(implicit elc: ErrorLoggingContext): String = {
    import scala.sys.process.*
    val cantonDir = s"tmp/canton-open-source-$release/bin/canton"
    if (Files.exists(Paths.get(cantonDir))) {
      elc.info(s"Release $release already downloaded.")
      cantonDir
    } else {
      val processLogger = new BufferedProcessLogger
      elc.info(s"Beginning to download release $release. This may take a while. ")
      val exitCode = s"scripts/testing/get-release.sh $release".!(processLogger)
      ErrorUtil.requireArgument(
        exitCode == 0,
        s"getting release $release failed with exit code $exitCode. Download script output: \n ${processLogger.output()}",
      )
      elc.info(
        s"Finished downloading release $release. Download script output: \n ${processLogger.output()}"
      )

      cantonDir
    }
  }

}
