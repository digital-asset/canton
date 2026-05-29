// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.nightly.topology

import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.plugins.UsePostgres
import com.digitalasset.canton.integration.tests.upgrade.CantonNetworkTopologyIntegrationTestBase
import com.digitalasset.canton.integration.{EnvironmentDefinition, SharedEnvironment}
import com.digitalasset.canton.logging.NodeLoggingUtil
import com.digitalasset.canton.synchronizer.sequencer.OnboardingStateForSequencerV2
import com.digitalasset.canton.topology.PhysicalSynchronizerId
import com.digitalasset.canton.topology.store.StoredTopologyTransactions
import com.digitalasset.canton.topology.store.StoredTopologyTransactions.GenericStoredTopologyTransactions
import com.digitalasset.canton.util.{FutureUtil, GrpcStreamingUtils}
import com.google.cloud.storage.Storage.BlobListOption
import com.google.cloud.storage.StorageOptions
import io.circe.parser as circeParser
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorInputStream
import org.apache.commons.io.FilenameUtils

import java.io.{InputStream, PipedInputStream, PipedOutputStream}
import java.util.concurrent.atomic.AtomicReference
import java.util.zip.GZIPInputStream
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.*

/** Try to deserialize and validate the topology state from CN as periodically exported on GCP.
  */
trait CantonNetworkRecentTopologyIntegrationTest
    extends CantonNetworkTopologyIntegrationTestBase
    with SharedEnvironment {

  // We need very high timeouts here as the mainnet snapshot especially is huge
  // and takes very long to validate
  private val timeout = 2.hours

  /** Name of the GCP bucket that contains the CN topology snapshots */
  private val bucketName = "cn-topology-snapshots"
  private val storage = StorageOptions.getDefaultInstance.getService

  registerPlugin(new UsePostgres(loggerFactory))

  /** The environment folder name in the GCP bucket (e.g. "cilr", "mainzrh"). */
  protected def environmentName: String

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1S1M1_Manual
      .withSetup { env =>
        import env.*
        participant1.start()
      }

  /** Download the latest onboarding-state file from the GCP bucket for [[environmentName]].
    *
    * Bucket structure:
    * {{{
    *   cn-topology-snapshots/
    *     <environment>/          (e.g. cilr, mainzrh)
    *       sv-1/
    *         topology_snapshot_<timestamp>/
    *           onboarding-state  (possibly with an extension if compressed, e.g. ".zst")
    * }}}
    *
    * We list the snapshot folders under `<environment>/sv-1/`, pick the latest one (lexicographic
    * max on the ISO timestamp), and download the `onboarding-state` blob into a local temporary
    * file.
    */
  private def downloadLatestOnboardingState()(implicit
      ec: ExecutionContext
  ): (InputStream, String) = {
    val prefix = s"$environmentName/sv-1/"

    // List all blobs under the environment's sv-1/ prefix to discover snapshot folders.
    val blobs = storage
      .list(bucketName, BlobListOption.prefix(prefix))
      .iterateAll()
      .asScala
      .toSeq

    // Find the latest topology_snapshot folder by extracting folder names and sorting.
    // Blob names look like: mainzrh/sv-1/topology_snapshot_2026-04-15T08:12:31.642741Z/onboarding-state
    // If the file is compressed, it will be e.g. "onboarding-state.zst"
    val onboardingStatePattern = s"$prefix(topology_snapshot_[^/]+)/onboarding-state.*".r
    val onboardingStateBlobName = blobs
      .flatMap(blob => onboardingStatePattern.findFirstMatchIn(blob.getName).map(_.matched))
      .distinct
      .sorted
      .maxOption
      .getOrElse(
        fail(s"No topology snapshot folders found in bucket $bucketName under prefix $prefix")
      )

    val snapshotFolder =
      onboardingStateBlobName.substring(0, onboardingStateBlobName.lastIndexOf('/'))

    logger.info(s"Downloading $onboardingStateBlobName from bucket $bucketName")

    val blob = Option(storage.get(bucketName, onboardingStateBlobName))
      .getOrElse(
        fail(s"Blob $onboardingStateBlobName not found in bucket $bucketName")
      )

    val raw = new PipedInputStream()
    val out = new PipedOutputStream(raw)

    // Download the snapshot in a background thread, otherwise we deadlock
    FutureUtil.doNotAwait(
      Future {
        try {
          blob.downloadTo(out)
        } finally out.close()
      },
      "Failed to download file from bucket",
    )

    val stream: InputStream = FilenameUtils.getExtension(onboardingStateBlobName) match {
      case "" => raw
      case "gz" => new GZIPInputStream(raw)
      case "zst" => new ZstdCompressorInputStream(raw)
      case other => fail(s"Unsupported file extension: $other")
    }
    (stream, snapshotFolder)
  }

  /** Try to download and parse the metadata file from the same snapshot folder. Returns the parsed
    * [[PhysicalSynchronizerId]] if the file exists and contains valid JSON with a
    * "physicalSynchronizerId" field, or [[None]] otherwise.
    */
  private def downloadAndParseMetadata(snapshotFolder: String): PhysicalSynchronizerId = {
    val storage = StorageOptions.getDefaultInstance.getService
    val metadataBlobName = s"$snapshotFolder/metadata"

    val blob = Option(storage.get(bucketName, metadataBlobName))
    blob
      .flatMap { b =>
        val content = new String(b.getContent(), "UTF-8")
        logger.info(s"Downloaded metadata file: $content")
        circeParser
          .parse(content)
          .toOption
          .flatMap(_.hcursor.get[String]("physicalSynchronizerId").toOption)
          .flatMap { psidStr =>
            PhysicalSynchronizerId.fromString(psidStr) match {
              case Right(psid) =>
                logger.info(s"Parsed physicalSynchronizerId from metadata: $psid")
                Some(psid)
              case Left(err) =>
                logger.warn(s"Failed to parse physicalSynchronizerId '$psidStr': $err")
                None
            }
          }
      }
      .getOrElse(
        fail(
          s"Unable to extract physical synchronizer id from topology snapshot $snapshotFolder"
        )
      )
  }

  private val snapshot = new AtomicReference[Option[GenericStoredTopologyTransactions]](None)
  private var physicalSynchronizerId: PhysicalSynchronizerId = _

  "Canton node " can {
    "successfully deserialize the topology snapshot" in { env =>
      import env.*

      val (in, snapshotFolder) = downloadLatestOnboardingState()
      physicalSynchronizerId = downloadAndParseMetadata(snapshotFolder)
      val transactions =
        try {
          GrpcStreamingUtils
            .parseDelimitedFromTrusted(in, OnboardingStateForSequencerV2)
            .value
            .flatMap(_.topologySnapshot)
        } finally in.close()
      logger.info(s"loaded ${transactions.size} transactions")
      snapshot.set(Some(StoredTopologyTransactions(transactions)))
    }

    "successfully validate the topology snapshot" in { implicit env =>
      // To avoid flooding the logs
      NodeLoggingUtil.setLevel(level = "INFO")
      // runValidation will fail if the validation fails. We discard the returned value because we don't need
      // it here and only assert that validation passed
      runValidation(
        // Index value is not important, it just needs to be an index that isn't used by default by the node
        topoStoreIdx = 11,
        snapshot.get().value,
        cleanupTopologyState = false,
        timeout,
        physicalSynchronizerIdOverride = Some(physicalSynchronizerId),
      ).discard
    }
  }
}

final class CantonNetworkRecentCilrTopologyIntegrationTest
    extends CantonNetworkRecentTopologyIntegrationTest {
  override protected def environmentName: String = "cilr"
}

final class CantonNetworkRecentMainNetTopologyIntegrationTest
    extends CantonNetworkRecentTopologyIntegrationTest {
  override protected def environmentName: String = "mainzrh"
}

final class CantonNetworkRecentTestNetTopologyIntegrationTest
    extends CantonNetworkRecentTopologyIntegrationTest {
  override protected def environmentName: String = "testzrh"
}

final class CantonNetworkRecentDevNetTopologyIntegrationTest
    extends CantonNetworkRecentTopologyIntegrationTest {
  override protected def environmentName: String = "dev"
}
