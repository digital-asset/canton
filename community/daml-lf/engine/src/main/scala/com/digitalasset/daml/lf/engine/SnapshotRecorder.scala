// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine

import cats.instances.either.*
import cats.instances.vector.*
import cats.syntax.traverse.*
import com.digitalasset.daml.lf.data.Ref.{PackageId, PackageName, Party}
import com.digitalasset.daml.lf.data.Time
import com.digitalasset.daml.lf.interpretation.InterpretationConfig
import com.digitalasset.daml.lf.testing.snapshot.Snapshot
import com.digitalasset.daml.lf.transaction.{
  ContractInstanceCoder,
  FatContractInstance,
  GlobalKey,
  VersionedTransaction,
}
import com.digitalasset.daml.lf.value.Value.ContractId
import com.digitalasset.daml.lf.value.{ContractIdVersion, ValueCoder}

import java.nio.file.{Files, Path, StandardOpenOption}
import scala.jdk.CollectionConverters.*
import scala.util.Using

/** Writes the entries of transaction snapshots, as read by the snapshot benchmark. */
private[lf] object SnapshotRecorder {

  def transactionEntry(
      tx: VersionedTransaction,
      participantId: String,
      submitters: Set[Party],
      ledgerTime: Time.Timestamp,
      preparationTime: Time.Timestamp,
      submissionSeed: Option[crypto.Hash],
      transactionSeed: crypto.Hash,
      contractIdVersion: ContractIdVersion,
      packageResolution: Map[PackageName, PackageId],
      inputContracts: Map[ContractId, FatContractInstance],
      keyLookups: Map[GlobalKey, Vector[ContractId]],
      usedPackages: Set[PackageId],
      interpretationConfig: InterpretationConfig,
      metricPlugins: Seq[Snapshot.MetricPlugin],
  ): Either[ValueCoder.EncodeError, Snapshot.TransactionEntry] =
    for {
      encoded <- TransactionCoder.encodeTransaction(tx)
      encodedInputs <- inputContracts.toVector
        .sortBy(_._1.coid)
        .traverse { case (_, contract) =>
          ContractInstanceCoder.encodeFatContractInstance(contract)
        }
    } yield {
      val builder = Snapshot.TransactionEntry
        .newBuilder()
        .setRawTransaction(encoded.toByteString)
        .setParticipantId(participantId)
        .addAllSubmitters(submitters.map(_.toString).asJava)
        .setLedgerTime(ledgerTime.micros)
        .setPreparationTime(preparationTime.micros)
        .setTransactionSeed(transactionSeed.bytes.toByteString)
        .setContractIdVersion(contractIdVersion match {
          case ContractIdVersion.V1 => Snapshot.ContractIdVersion.CONTRACT_ID_VERSION_V1
          case ContractIdVersion.V2 => Snapshot.ContractIdVersion.CONTRACT_ID_VERSION_V2
        })
        .putAllPackageResolution(
          packageResolution.map { case (name, id) => (name: String) -> (id: String) }.asJava
        )
        .addAllInputContracts(encodedInputs.asJava)
        .addAllKeyLookups(
          keyLookups.values
            .filter(_.nonEmpty)
            .toSeq
            .sortBy(_.head.coid)
            .map(cids =>
              Snapshot.KeyLookup
                .newBuilder()
                .addAllContractIds(cids.map(_.toBytes.toByteString).asJava)
                .build()
            )
            .asJava
        )
        .addAllUsedPackages(usedPackages.toSeq.map(id => id: String).sorted.asJava)
        .setInterpretationConfig(
          Snapshot.InterpretationConfig
            .newBuilder()
            .addAllAllowedLanguageVersions(
              interpretationConfig.allowedLanguageVersions.map(_.pretty).asJava
            )
        )
        .addAllMetricPlugins(metricPlugins.asJava)
      submissionSeed.foreach(seed => builder.setSubmissionSeed(seed.bytes.toByteString))
      builder.build()
    }

  def append(file: Path, entry: Snapshot.TransactionEntry): Unit =
    append(file, Snapshot.SubmissionEntry.newBuilder().setTransaction(entry).build())

  def append(file: Path, entry: Snapshot.SubmissionEntry): Unit =
    Using.resource(
      Files.newOutputStream(
        file,
        StandardOpenOption.CREATE,
        StandardOpenOption.WRITE,
        StandardOpenOption.APPEND,
      )
    )(entry.writeDelimitedTo)
}
