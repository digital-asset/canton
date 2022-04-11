// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import cats.data.OptionT
import com.daml.daml_lf_dev.DamlLf
import com.daml.lf.data.Ref.PackageId
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.LfPackageId
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.LengthLimitedString.DarName
import com.digitalasset.canton.config.RequireTypes.{PositiveNumeric, String256M}
import com.digitalasset.canton.crypto.Hash
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.MetricHandle.GaugeM
import com.digitalasset.canton.metrics.TimedLoadGauge
import com.digitalasset.canton.participant.admin.PackageService
import com.digitalasset.canton.participant.admin.PackageService.{Dar, DarDescriptor}
import com.digitalasset.canton.participant.store.DamlPackageStore
import com.digitalasset.canton.participant.store.db.DbDamlPackageStore.{DamlPackage, DarRecord}
import com.digitalasset.canton.protocol.PackageDescription
import com.digitalasset.canton.resource.DbStorage.DbAction
import com.digitalasset.canton.resource.DbStorage.DbAction.WriteOnly
import com.digitalasset.canton.resource.{DbStorage, DbStore, IdempotentInsert}
import com.digitalasset.canton.tracing.TraceContext
import io.functionmeta.functionFullName
import slick.jdbc.TransactionIsolation.Serializable

import scala.concurrent.{ExecutionContext, Future}

class DbDamlPackageStore(
    maxContractIdSqlInListSize: PositiveNumeric[Int],
    override protected val storage: DbStorage,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends DamlPackageStore
    with DbStore {

  import DamlPackageStore._
  import storage.api._
  import storage.converters._
  import DbStorage.Implicits._

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
      darO: Option[DarDescriptor],
      sourceDescription: String256M,
  ): DbAction.WriteTransactional[Unit] = {

    val insertToPackageStore = (storage.profile match {
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
    }).map((_int: Int) => ())

    val insertToDarPackages = darO
      .map { dar =>
        val hash = dar.hash.toLengthLimitedHexString

        val sql = IdempotentInsert.insertIgnoringConflicts(
          storage,
          oracleIgnoreIndex = "dar_packages ( dar_hash_hex, package_id )",
          insertBuilder =
            sql"dar_packages(dar_hash_hex, package_id) values ($hash, ${pkg.packageId})",
        )

        sql.map((_int: Int) => ())
      }

    insertToDarPackages
      .fold[DBIOAction[Unit, NoStream, Effect.Transactional with Effect.Write]](
        ifEmpty = insertToPackageStore
      )(darPkgsInsert =>
        insertToPackageStore
          .andThen(darPkgsInsert)
      )
  }

  override def append(
      pkgs: List[DamlLf.Archive],
      sourceDescription: String256M,
      dar: Option[PackageService.Dar],
  )(implicit
      traceContext: TraceContext
  ): Future[Unit] = processingTime.metric.event {

    //TODO(i8432): Perform a batch insert for the packages
    val insertPkgs = pkgs.map { archive =>
      val pkg = DamlPackage(readPackageId(archive), archive.toByteArray)
      insertOrUpdatePackage(pkg, dar.map(_.descriptor), sourceDescription)
    }

    val writeDar: List[WriteOnly[Int]] = dar.map(dar => (appendToDarStore(dar))).toList

    // Combine all the operations into a single transaction.
    // Use Serializable isolation to protect against concurrent deletions from the `dars` or `daml_packages` tables,
    // which might otherwise cause a constraint violation exception.
    // This is not a performance-critical operation, and happens rarely, so the isolation level should not be a
    // performance concern.
    val writeDarAndPackages = DBIO
      .seq(writeDar ++ insertPkgs: _*)
      .transactionally
      .withTransactionIsolation(Serializable)

    storage
      .update_(
        action = writeDarAndPackages,
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

    storage.update_(
      sqlu"""delete from daml_packages where package_id = $packageId """,
      functionFullName,
    )
  }

  override def anyPackagePreventsDarRemoval(packages: Seq[PackageId], removeDar: DarDescriptor)(
      implicit tc: TraceContext
  ): OptionT[Future, PackageId] = {

    import DbStorage.Implicits.BuilderChain._
    import com.digitalasset.canton.resource.DbStorage.Implicits.getResultPackageId

    val darHex = removeDar.hash.toLengthLimitedHexString

    def packagesWithoutDar(
        nonEmptyPackages: NonEmpty[Seq[PackageId]]
    ) = {
      val queryActions = DbStorage
        .toInClauses(
          field = "package_id",
          values = nonEmptyPackages,
          maxContractIdSqlInListSize,
        )
        .map({ case (_value, inStatement) =>
          (sql"""
                  select package_id
                  from dar_packages result
                  where
                  """ ++ inStatement ++ sql"""
                  and not exists (
                    select package_id
                    from dar_packages counterexample
                    where
                      result.package_id = counterexample.package_id
                      and dar_hash_hex != $darHex
                  )
                  #${storage.limit(1)}
                  """).as[LfPackageId]
        })

      val resultF = for {
        packages <- storage.sequentialQueryAndCombine(queryActions, functionFullName)
      } yield {
        packages.headOption
      }

      OptionT(resultF)
    }

    NonEmpty
      .from(packages)
      .fold(OptionT.none[Future, PackageId])(packagesWithoutDar)
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
    sql"select hash, name, data from dars where hash_hex = ${hash.toLengthLimitedHexString}"
      .as[Dar]
      .headOption

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

  override def removeDar(hash: Hash)(implicit traceContext: TraceContext): Future[Unit] = {
    storage.update_(
      sqlu"""delete from dars where hash_hex = ${hash.toLengthLimitedHexString}""",
      functionFullName,
    )
  }

}

object DbDamlPackageStore {
  private case class DamlPackage(packageId: LfPackageId, data: Array[Byte])

  private case class DarRecord(hash: Hash, data: Array[Byte], name: DarName)
}
