// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import com.daml.daml_lf_dev.DamlLf
import com.daml.lf.data.Ref.PackageId
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.LengthLimitedString.DarName
import com.digitalasset.canton.crypto.Hash
import com.digitalasset.canton.lifecycle.FlagCloseable
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.MetricHandle.GaugeM
import com.digitalasset.canton.metrics.TimedLoadGauge
import com.digitalasset.canton.participant.admin.PackageService
import com.digitalasset.canton.participant.admin.PackageService.{Dar, DarDescriptor}
import com.digitalasset.canton.participant.store.DamlPackageStore
import com.digitalasset.canton.participant.store.db.DbDamlPackageStore.{DamlPackage, DarRecord}
import com.digitalasset.canton.protocol.PackageDescription
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.resource.DbStorage.DbAction
import com.digitalasset.canton.resource.DbStorage.DbAction.WriteOnly
import com.digitalasset.canton.tracing.TraceContext
import io.functionmeta.functionFullName

import scala.concurrent.{ExecutionContext, Future}

class DbDamlPackageStore(
    storage: DbStorage,
    override protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends DamlPackageStore
    with FlagCloseable
    with NamedLogging {

  import DamlPackageStore._
  import storage.api._
  import storage.converters._

  private val processingTime: GaugeM[TimedLoadGauge, Double] =
    storage.metrics.loadGaugeM("daml-packages-dars-store")

  private def exists(packageId: PackageId): DbAction.ReadOnly[Option[DamlLf.Archive]] =
    sql"select data from daml_packages where package_id = $packageId"
      .as[Array[Byte]]
      .headOption
      .map { mbData =>
        mbData.map(DamlLf.Archive.parseFrom)
      }

  private def insertOrUpdatePackage(
      pkg: DamlPackage,
      sourceDescription: String,
  ): DbAction.WriteOnly[Int] =
    // only update the description if the given one is not empty
    storage.profile match {
      case _: DbStorage.Profile.H2 =>
        if (sourceDescription.nonEmpty)
          sqlu"merge into daml_packages (package_id, data, source_description) values (${pkg.packageId}, ${pkg.data}, ${sourceDescription})"
        else
          sqlu"merge into daml_packages (package_id, data) values (${pkg.packageId}, ${pkg.data})"
      case _: DbStorage.Profile.Postgres =>
        if (sourceDescription.nonEmpty)
          sqlu"""insert into daml_packages (package_id, data, source_description) values (${pkg.packageId}, ${pkg.data}, ${sourceDescription})
               on conflict (package_id) do update set data = ${pkg.data}, source_description = ${sourceDescription}"""
        else
          sqlu"""insert into daml_packages (package_id, data) values (${pkg.packageId}, ${pkg.data})
               on conflict (package_id) do update set data = ${pkg.data}"""
      case _: DbStorage.Profile.Oracle =>
        if (sourceDescription.nonEmpty)
          sqlu"""insert
              /*+  IGNORE_ROW_ON_DUPKEY_INDEX ( daml_packages ( package_id ) ) */
              into daml_packages (package_id, data, source_description)
              values (${pkg.packageId}, ${pkg.data}, ${sourceDescription})
          """
        else
          sqlu"""insert
          /*+  IGNORE_ROW_ON_DUPKEY_INDEX ( daml_packages ( package_id ) ) */
          into daml_packages (package_id, data)
          values (${pkg.packageId}, ${pkg.data})
          """
    }

  override def append(
      pkgs: List[DamlLf.Archive],
      sourceDescription: String,
      dar: Option[PackageService.Dar],
  )(implicit
      traceContext: TraceContext
  ): Future[Unit] = processingTime.metric.event {

    //TODO(i8432): Perform a batch insert for the packages
    val insertPkgs: List[WriteOnly[Int]] = pkgs.map { archive =>
      val pkg = DamlPackage(readPackageId(archive), archive.toByteArray)
      insertOrUpdatePackage(pkg, sourceDescription)
    }

    val writeDar: List[WriteOnly[Int]] = dar.map(dar => (appendToDarStore(dar))).toList

    storage
      .update_(
        action = DBIO.seq(writeDar ++ insertPkgs: _*),
        operationName = s"${this.getClass}: append Daml LF archive",
      )
  }

  private def appendToDarStore(dar: Dar) = {
    val hash = dar.descriptor.hash
    val record = DarRecord(hash, dar.bytes, dar.descriptor.name)
    insertOrUpdateDar(record)
  }

  override def getPackage(
      packageId: PackageId
  )(implicit traceContext: TraceContext): Future[Option[DamlLf.Archive]] =
    processingTime.metric.event {
      storage.querySingle(exists(packageId), functionFullName).value
    }

  override def getPackageDescription(packageId: PackageId)(implicit
      traceContext: TraceContext
  ): Future[Option[PackageDescription]] = processingTime.metric.event {
    storage
      .querySingle(
        sql"select package_id, source_description from daml_packages where package_id = $packageId"
          .as[PackageDescription]
          .headOption,
        functionFullName,
      )
      .value
  }

  override def listPackages(
      limit: Option[Int]
  )(implicit traceContext: TraceContext): Future[Seq[PackageDescription]] =
    processingTime.metric.event {
      storage.query(
        sql"select package_id, source_description from daml_packages #${limit.fold("")(storage.limit(_))}"
          .as[PackageDescription],
        functionFullName,
      )
    }

  override def removePackage(
      packageId: PackageId
  )(implicit traceContext: TraceContext): Future[Unit] = {
    logger.debug(s"Removing package $packageId")
    val query = sqlu"delete from daml_packages where package_id = $packageId"
    storage.update_(query, functionFullName)
  }

  override def getDar(
      hash: Hash
  )(implicit traceContext: TraceContext): Future[Option[Dar]] =
    processingTime.metric.event {
      storage.querySingle(existing(hash), functionFullName).value
    }

  override def listDars(
      limit: Option[Int]
  )(implicit traceContext: TraceContext): Future[Seq[DarDescriptor]] =
    processingTime.metric.event {
      val query = limit match {
        case None => sql"select hash, name from dars".as[DarDescriptor]
        case Some(amount) =>
          sql"select hash, name from dars #${storage.limit(amount)}".as[DarDescriptor]
      }
      storage.query(query, functionFullName)
    }

  private def existing(hash: Hash): DbAction.ReadOnly[Option[Dar]] =
    sql"select hash, name, data from dars where hash_hex = ${hash.toHexString}".as[Dar].headOption

  private def insertOrUpdateDar(dar: DarRecord): DbAction.WriteOnly[Int] =
    storage.profile match {
      case _: DbStorage.Profile.H2 =>
        sqlu"merge into dars (hash_hex, hash, data, name) values (${dar.hash.toLengthLimitedHexString}, ${dar.hash}, ${dar.data}, ${dar.name})"
      case _: DbStorage.Profile.Postgres =>
        sqlu"""insert into dars (hash_hex, hash, data, name) values (${dar.hash.toLengthLimitedHexString},${dar.hash}, ${dar.data}, ${dar.name})
               on conflict (hash_hex) do nothing"""
      case _: DbStorage.Profile.Oracle =>
        sqlu"""insert
                /*+  IGNORE_ROW_ON_DUPKEY_INDEX ( dars ( hash_hex ) ) */
                into dars (hash_hex, hash, data, name) 
                values (${dar.hash.toLengthLimitedHexString}, ${dar.hash}, ${dar.data}, ${dar.name})
              """
    }

}

object DbDamlPackageStore {
  private case class DamlPackage(packageId: PackageId, data: Array[Byte])

  private case class DarRecord(hash: Hash, data: Array[Byte], name: DarName)
}
