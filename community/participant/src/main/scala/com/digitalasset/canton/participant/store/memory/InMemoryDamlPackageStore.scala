// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import cats.Monoid
import cats.data.OptionT
import cats.implicits._
import com.daml.daml_lf_dev.DamlLf
import com.daml.lf.data.Ref.PackageId
import com.digitalasset.canton.LfPackageId
import com.digitalasset.canton.config.RequireTypes.{String256M, String255}
import com.digitalasset.canton.crypto.Hash
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.PackageService
import com.digitalasset.canton.participant.admin.PackageService.{Dar, DarDescriptor}
import com.digitalasset.canton.participant.store.DamlPackageStore
import com.digitalasset.canton.participant.store.memory.InMemoryDamlPackageStore.defaultPackageDescription
import com.digitalasset.canton.protocol.PackageDescription
import com.digitalasset.canton.tracing.TraceContext

import java.util.concurrent.ConcurrentHashMap
import scala.collection.concurrent
import scala.collection.immutable.Seq
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

class InMemoryDamlPackageStore(override protected val loggerFactory: NamedLoggerFactory)(implicit
    ec: ExecutionContext
) extends DamlPackageStore
    with NamedLogging {
  import DamlPackageStore._

  private val pkgData: concurrent.Map[LfPackageId, (DamlLf.Archive, String256M)] =
    new ConcurrentHashMap[LfPackageId, (DamlLf.Archive, String256M)].asScala

  private val darData: concurrent.Map[Hash, (Array[Byte], String255)] =
    new ConcurrentHashMap[Hash, (Array[Byte], String255)].asScala

  private val darPackages: concurrent.Map[Hash, Set[LfPackageId]] =
    new ConcurrentHashMap[Hash, Set[LfPackageId]].asScala

  override def append(
      pkgs: List[DamlLf.Archive],
      sourceDescription: String256M,
      dar: Option[PackageService.Dar],
  )(implicit
      traceContext: TraceContext
  ): Future[Unit] = {

    val pkgIds = pkgs.map(readPackageId)

    pkgs.foreach { pkgArchive =>
      val packageId = readPackageId(pkgArchive)
      // only update the description if the given one is not empty
      if (sourceDescription.nonEmpty)
        pkgData.put(packageId, (pkgArchive, sourceDescription))
      else
        // TODO(andreas) This is not thread-safe
        pkgData.put(
          packageId,
          (pkgArchive, pkgData.get(packageId).map(_._2).getOrElse(defaultPackageDescription)),
        )
    }

    dar.foreach { dar =>
      darData.put(dar.descriptor.hash, (dar.bytes.clone(), dar.descriptor.name))
      val hash = dar.descriptor.hash
      val pkgS = pkgIds.toSet
      darPackages.updateWith(hash)(optSet => Some(optSet.fold(pkgS)(_.union(pkgS))))
    }

    Future.unit
  }

  override def getPackage(packageId: LfPackageId)(implicit
      traceContext: TraceContext
  ): Future[Option[DamlLf.Archive]] =
    Future.successful(pkgData.get(packageId).map(_._1))

  override def getPackageDescription(
      packageId: LfPackageId
  )(implicit traceContext: TraceContext): Future[Option[PackageDescription]] =
    Future.successful(pkgData.get(packageId).map { case (_, sourceDescription) =>
      PackageDescription(packageId, sourceDescription)
    })

  override def listPackages(
      limit: Option[Int]
  )(implicit traceContext: TraceContext): Future[Seq[PackageDescription]] =
    Future.successful(
      pkgData
        .take(limit.getOrElse(Int.MaxValue))
        .map { case (pid, (_, sourceDescription)) =>
          PackageDescription(pid, sourceDescription)
        }
        .to(Seq)
    )

  override def removePackage(
      packageId: PackageId
  )(implicit traceContext: TraceContext): Future[Unit] = {
    darPackages
      .mapValuesInPlace({ case (_hash, packages) => packages - packageId })
      .filterInPlace({ case (_hash, packages) => packages.nonEmpty })

    pkgData.remove(packageId)

    Future.unit
  }

  override def getDar(
      hash: Hash
  )(implicit traceContext: TraceContext): Future[Option[Dar]] =
    Future.successful(darData.get(hash).map { case (bytes, name) =>
      Dar(DarDescriptor(hash, name), bytes.clone())
    })

  override def listDars(
      limit: Option[Int]
  )(implicit traceContext: TraceContext): Future[Seq[DarDescriptor]] =
    Future.successful(
      darData
        .take(limit.getOrElse(Int.MaxValue))
        .map { case (hash, (_, name)) =>
          DarDescriptor(hash, name)
        }
        .to(Seq)
    )

  override def anyPackagePreventsDarRemoval(packages: List[PackageId], removeDar: DarDescriptor)(
      implicit tc: TraceContext
  ): OptionT[Future, PackageId] = {
    val known = packages.toSet.intersect(Monoid.combineAll(darPackages.toMap.values))
    val fromAllOtherDars = Monoid.combineAll(darPackages.toMap.removed(removeDar.hash).values)
    val withoutDar = known.diff(fromAllOtherDars).headOption
    OptionT.fromOption(withoutDar)
  }

  override def removeDar(hash: Hash)(implicit traceContext: TraceContext): Future[Unit] = {
    darPackages.remove(hash)
    darData.remove(hash)
    Future.unit
  }

  override def close(): Unit = ()
}

object InMemoryDamlPackageStore {
  val defaultPackageDescription = String256M.tryCreate("default")
}
