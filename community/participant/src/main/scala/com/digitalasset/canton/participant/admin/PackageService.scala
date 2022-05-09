// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import cats.data.{EitherT, OptionT}
import cats.syntax.either._
import cats.syntax.functorFilter._
import cats.syntax.traverse._
import com.daml.daml_lf_dev.DamlLf
import com.daml.error.definitions.{DamlError, PackageServiceError}
import com.daml.lf.archive
import com.daml.lf.archive.{DarParser, Decode, Error => LfArchiveError}
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.engine.Engine
import com.daml.lf.language.Ast.Package
import com.digitalasset.canton.LedgerSubmissionId
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.LengthLimitedString.DarName
import com.digitalasset.canton.config.RequireTypes.{String255, String256M}
import com.digitalasset.canton.crypto.{Hash, HashOps, HashPurpose}
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, Lifecycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.LedgerSyncEvent
import com.digitalasset.canton.participant.admin.CantonPackageServiceError.PackageRemovalErrorCode
import com.digitalasset.canton.participant.admin.CantonPackageServiceError.PackageRemovalErrorCode.{
  CannotRemoveOnlyDarForPackage,
  DarUnvettingError,
  MainPackageInUse,
  PackageRemovalError,
}
import com.digitalasset.canton.participant.admin.PackageService._
import com.digitalasset.canton.participant.store.DamlPackageStore
import com.digitalasset.canton.participant.store.DamlPackageStore.readPackageId
import com.digitalasset.canton.participant.sync.ParticipantEventPublisher
import com.digitalasset.canton.participant.topology.ParticipantTopologyManagerError
import com.digitalasset.canton.protocol.{PackageDescription, PackageInfoService}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.PathUtils
import com.github.blemale.scaffeine.Scaffeine
import com.google.protobuf.ByteString
import io.functionmeta.functionFullName
import slick.jdbc.GetResult

import java.io._
import java.nio.file.Paths
import java.util.UUID
import java.util.zip.ZipInputStream
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

trait DarService {
  def appendDarFromByteString(
      payload: ByteString,
      filename: String,
      vetAllPackages: Boolean,
      synchronizeVetting: Boolean,
  )(implicit traceContext: TraceContext): EitherT[Future, DamlError, Hash]
  def getDar(hash: Hash)(implicit traceContext: TraceContext): Future[Option[PackageService.Dar]]
  def listDars(limit: Option[Int])(implicit
      traceContext: TraceContext
  ): Future[Seq[PackageService.DarDescriptor]]
}

class PackageService(
    engine: Engine,
    private[admin] val packagesDarsStore: DamlPackageStore,
    eventPublisher: ParticipantEventPublisher,
    hashOps: HashOps,
    vetPackages: Traced[(Seq[PackageId], Boolean)] => EitherT[
      Future,
      ParticipantTopologyManagerError,
      Unit,
    ],
    inspectionOps: PackageInspectionOps,
    override protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends DarService
    with PackageInfoService
    with NamedLogging
    with FlagCloseable {

  def getLfArchive(packageId: PackageId)(implicit
      traceContext: TraceContext
  ): Future[Option[DamlLf.Archive]] =
    packagesDarsStore.getPackage(packageId)

  def listPackages(limit: Option[Int] = None)(implicit
      traceContext: TraceContext
  ): Future[Seq[PackageDescription]] =
    packagesDarsStore.listPackages(limit)

  def getDescription(packageId: PackageId)(implicit
      traceContext: TraceContext
  ): Future[Option[PackageDescription]] =
    packagesDarsStore.getPackageDescription(packageId)

  def getPackage(packageId: PackageId)(implicit
      traceContext: TraceContext
  ): Future[Option[Package]] =
    getLfArchive(packageId).map(_.map(Decode.assertDecodeArchive(_)._2))

  def removePackage(
      packageId: PackageId,
      force: Boolean,
  )(implicit traceContext: TraceContext): EitherT[Future, PackageRemovalError, Unit] = {
    if (force) {
      logger.info(s"Forced removal of package $packageId")
      EitherT.liftF(packagesDarsStore.removePackage(packageId))
    } else {
      val isUsed = inspectionOps.packageUnused(packageId)
      val isVetted = inspectionOps.packageVetted(packageId)

      for {
        _used <- isUsed
        vetted <- isVetted
        removed <- {
          logger.debug(s"Removing package $packageId")
          EitherT.liftF[Future, PackageRemovalError, Unit](
            packagesDarsStore.removePackage(packageId)
          )
        }
      } yield ()
    }
  }

  def removeDar(darHash: Hash)(implicit
      tc: TraceContext
  ): EitherT[Future, PackageRemovalError, Unit] = {

    for {
      darOpt <- EitherT.liftF(packagesDarsStore.getDar(darHash))

      _unit <- darOpt.fold({
        logger.info(s"Trying to remove a DAR that isn't stored. Operation is trivially successful.")
        EitherT.pure[Future, PackageRemovalError](())
      })({ dar =>
        val darLfE = PackageService.darToLf(dar)
        val (descriptor, lfArchive) =
          darLfE.left.map(msg => throw new IllegalStateException(msg)).merge
        removeDarLf(lfArchive, descriptor)
      })

    } yield { () }
  }

  private def removeDarLf(
      dar: archive.Dar[DamlLf.Archive],
      darDescriptor: DarDescriptor,
  )(implicit
      tc: TraceContext
  ): EitherT[Future, PackageRemovalError, Unit] = {

    // Can remove the DAR if:
    // 1. The main package of the dar is unused
    // 2. For all dependencies of the DAR, either:
    //     - They are unused
    //     - Or they are contained in another vetted DAR
    // 3. The main package of the dar is either:
    //     - Already un-vetted
    //     - Or can be automatically un-vetted, by revoking a vetting transaction corresponding to all packages in the DAR

    val packages =
      dar.all.map { d =>
        d.getHash
        readPackageId(d)
      }

    val mainPkg = readPackageId(dar.main)
    for {
      _mainUnused <- inspectionOps
        .packageUnused(mainPkg)
        .leftMap(err => new MainPackageInUse(err.pkg, darDescriptor, err.contract, err.domain))

      packageUsed <- EitherT.liftF(packages.traverse(p => inspectionOps.packageUnused(p).value))

      usedPackages = packageUsed.mapFilter {
        case Left(packageInUse: PackageRemovalErrorCode.PackageInUse) => Some(packageInUse.pkg)
        case Right(()) => None
      }

      _unit <- packagesDarsStore
        .anyPackagePreventsDarRemoval(usedPackages, darDescriptor)
        .toLeft(())
        .leftMap(p => new CannotRemoveOnlyDarForPackage(p, darDescriptor): PackageRemovalError)

      isVetted <- EitherT.liftF(inspectionOps.packageVetted(mainPkg).value.map(e => e.isLeft))

      _unit <-
        if (isVetted) {
          revokeVettingForDar(mainPkg, packages, darDescriptor)
        } else {
          EitherT.pure[Future, PackageRemovalError](())
        }

      _unit <-
        EitherT.liftF(packagesDarsStore.removePackage(mainPkg))

      _removed <- {
        logger.info(s"Removing dar ${darDescriptor.hash}")
        EitherT.liftF[Future, PackageRemovalError, Unit](
          packagesDarsStore.removeDar(darDescriptor.hash)
        )
      }

    } yield {
      ()
    }
  }

  private def revokeVettingForDar(
      mainPkg: PackageId,
      packages: List[PackageId],
      darDescriptor: DarDescriptor,
  )(implicit
      tc: TraceContext
  ): EitherT[Future, PackageRemovalError, Unit] = {
    for {
      tx <- EitherT.liftF(inspectionOps.genRevokePackagesTx(packages).value)

      _unvetted <- tx match {
        case Left(err) =>
          logger.info(
            s"Unable to automatically revoke the vetting the dar $darDescriptor."
          )

          val removalError = new DarUnvettingError(err, darDescriptor, mainPkg): PackageRemovalError
          EitherT.leftT[Future, Unit](removalError)

        case Right(op) =>
          logger.debug(s"Revoking vetting for dar $darDescriptor")
          inspectionOps
            .runTx(op, true)
            .leftMap(err => new DarUnvettingError(err, darDescriptor, mainPkg): PackageRemovalError)
      }
    } yield ()
  }

  /** Stores DAR file from given byte string with the provided filename.
    * All the Daml packages inside the DAR file are also stored.
    * @param payload ByteString containing the data of the DAR file
    * @param filename String the filename of the DAR
    * @return Future with the hash of the DAR file
    */
  def appendDarFromByteString(
      payload: ByteString,
      filename: String,
      vetAllPackages: Boolean,
      synchronizeVetting: Boolean,
  )(implicit traceContext: TraceContext): EitherT[Future, DamlError, Hash] =
    appendDar(
      payload,
      PathUtils.getFilenameWithoutExtension(Paths.get(filename).getFileName),
      vetAllPackages,
      synchronizeVetting,
    )

  private def catchUpstreamErrors[E](
      attempt: Either[LfArchiveError, E]
  )(implicit traceContext: TraceContext): EitherT[Future, DamlError, E] =
    EitherT.fromEither(attempt match {
      case Right(value) => Right(value)
      case Left(LfArchiveError.InvalidDar(entries, cause)) =>
        Left(PackageServiceError.Reading.InvalidDar.Error(entries.entries.keys.toSeq, cause))
      case Left(LfArchiveError.InvalidZipEntry(name, entries)) =>
        Left(
          PackageServiceError.Reading.InvalidZipEntry.Error(name, entries.entries.keys.toSeq)
        )
      case Left(LfArchiveError.InvalidLegacyDar(entries)) =>
        Left(PackageServiceError.Reading.InvalidLegacyDar.Error(entries.entries.keys.toSeq))
      case Left(LfArchiveError.ZipBomb) =>
        Left(PackageServiceError.Reading.ZipBomb.Error(LfArchiveError.ZipBomb.getMessage))
      case Left(e: LfArchiveError) =>
        Left(PackageServiceError.Reading.ParseError.Error(e.msg))
      case Left(e) =>
        Left(PackageServiceError.InternalError.Unhandled(e))
    })

  private def appendDar(
      payload: ByteString,
      darName: String,
      vetAllPackages: Boolean,
      synchronizeVetting: Boolean,
  )(implicit traceContext: TraceContext): EitherT[Future, DamlError, Hash] = {
    val hash = hashOps.digest(HashPurpose.DarIdentifier, payload)
    val stream = new ZipInputStream(payload.newInput())
    val ret: EitherT[Future, DamlError, Hash] = for {
      lengthValidatedName <- EitherT
        .fromEither[Future](
          String255
            .create(darName, Some("DAR file name"))
        )
        .leftMap(PackageServiceError.Reading.InvalidDarFileName.Error(_))
      dar <- catchUpstreamErrors(DarParser.readArchive(darName, stream))
      // Validate the packages before storing them in the DAR store or the package store
      _ <- validateArchives(dar.all)
      _ <- storeValidatedPackagesAndSyncEvent(
        dar.all,
        lengthValidatedName.asString1GB,
        LedgerSubmissionId.assertFromString(UUID.randomUUID().toString),
        Some(
          PackageService.Dar(DarDescriptor(hash, lengthValidatedName), payload.toByteArray)
        ),
        vetAllPackages = vetAllPackages,
        synchronizeVetting = synchronizeVetting,
      )

    } yield hash
    ret.transform { res =>
      stream.close()
      res
    }
  }

  override def getDar(hash: Hash)(implicit
      traceContext: TraceContext
  ): Future[Option[PackageService.Dar]] =
    packagesDarsStore.getDar(hash)

  override def listDars(limit: Option[Int])(implicit
      traceContext: TraceContext
  ): Future[Seq[PackageService.DarDescriptor]] = packagesDarsStore.listDars(limit)

  private def validateArchives(archives: List[DamlLf.Archive])(implicit
      traceContext: TraceContext
  ): EitherT[Future, DamlError, Unit] =
    for {
      packages <- archives
        .traverse(archive => catchUpstreamErrors(Decode.decodeArchive(archive)))
        .map(_.toMap)
      _ <- EitherT.fromEither[Future](
        engine
          .validatePackages(packages)
          .leftMap(
            PackageServiceError.Validation.handleLfEnginePackageError(_): DamlError
          )
      )
    } yield ()

  private def vetPackages(archives: List[DamlLf.Archive], synchronizeVetting: Boolean)(implicit
      traceContext: TraceContext
  ): EitherT[Future, DamlError, Unit] =
    vetPackages(Traced((archives.map(DamlPackageStore.readPackageId), synchronizeVetting)))
      .leftMap[DamlError] { err =>
        implicit val code = err.code
        CantonPackageServiceError.IdentityManagerParentError(err)
      }

  private val dependencyCache = Scaffeine()
    .maximumSize(10000)
    .expireAfterAccess(15.minutes)
    .buildAsyncFuture[PackageId, Option[Set[PackageId]]] { packageId =>
      loadPackageDependencies(packageId)(TraceContext.empty).value.map(_.toOption).onShutdown(None)
    }

  def packageDependencies(packages: List[PackageId]): EitherT[Future, PackageId, Set[PackageId]] =
    packages
      .traverse(pkgId => OptionT(dependencyCache.get(pkgId)).toRight(pkgId))
      .map(_.flatten.toSet -- packages)

  private def loadPackageDependencies(packageId: PackageId)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, PackageId, Set[PackageId]] = {
    def computeDirectDependencies(
        packageIds: List[PackageId]
    ): EitherT[FutureUnlessShutdown, PackageId, Set[PackageId]] =
      for {
        directDependenciesByPackage <- packageIds.traverse { packageId =>
          for {
            pckg <- OptionT(
              performUnlessClosingF(functionFullName)(packagesDarsStore.getPackage(packageId))
            )
              .toRight(packageId)
            directDependencies <- EitherT(
              performUnlessClosingF(functionFullName)(
                Future(
                  Either
                    .catchOnly[Exception](
                      com.daml.lf.archive.Decode.assertDecodeArchive(pckg)._2.directDeps
                    )
                    .leftMap { e =>
                      logger.error(
                        s"Failed to decode package with id $packageId while trying to determine dependencies",
                        e,
                      )
                      packageId
                    }
                )
              )
            )
          } yield directDependencies
        }
      } yield directDependenciesByPackage.reduceLeftOption(_ ++ _).getOrElse(Set.empty)

    def go(
        packageIds: List[PackageId],
        knownDependencies: Set[PackageId],
    ): EitherT[FutureUnlessShutdown, PackageId, Set[PackageId]] =
      if (packageIds.isEmpty) EitherT.rightT(knownDependencies)
      else {
        for {
          directDependencies <- computeDirectDependencies(packageIds)
          newlyDiscovered = directDependencies -- knownDependencies - packageId
          allDependencies <- go(newlyDiscovered.toList, knownDependencies ++ newlyDiscovered)
        } yield allDependencies
      }
    go(List(packageId), Set())

  }

  /** Stores archives in the store and sends package upload event to participant event log for inclusion in ledger
    * sync event stream. This allows the ledger api server to update its package store accordingly and unblock
    * synchronous upload request if the request originated in the ledger api.
    * @param archives The archives to store. They must have been decoded and package-validated before.
    * @param sourceDescription description of the source of the package
    * @param submissionId upstream submissionId for ledger api server to recognize previous package upload requests
    * @param vetAllPackages if true, then the packages will be vetted automatically
    * @param synchronizeVetting if true, the future will terminate once the participant observed the package vetting on all connected domains
    * @return future holding whether storing and/or event sending failed (relevant to upstream caller)
    */
  def storeValidatedPackagesAndSyncEvent(
      archives: List[DamlLf.Archive],
      sourceDescription: String256M,
      submissionId: LedgerSubmissionId,
      dar: Option[Dar],
      vetAllPackages: Boolean,
      synchronizeVetting: Boolean,
  )(implicit traceContext: TraceContext): EitherT[Future, DamlError, Unit] = {

    EitherT
      .right(
        packagesDarsStore
          .append(archives, sourceDescription, dar)
          .map { _ =>
            // update our dependency cache
            // we need to do this due to an issue we can hit if we have pre-populated the cache
            // with the information about the package not being present (with a None)
            // now, that the package is loaded, we need to get rid of this None.
            dependencyCache.synchronous().asMap().filterInPlace { case (_, deps) =>
              deps.isDefined
            }
          }
          .transformWith {
            case Success(_) =>
              eventPublisher.publish(
                LedgerSyncEvent.PublicPackageUpload(
                  archives = archives,
                  sourceDescription = Some(sourceDescription.unwrap),
                  recordTime = ParticipantEventPublisher.now.toLf,
                  submissionId = Some(submissionId),
                )
              )
            case Failure(e) =>
              logger.warn(
                s"Failed to upload one or more archives in submissionId $submissionId and sourceDescription $sourceDescription",
                e,
              )
              eventPublisher.publish(
                LedgerSyncEvent.PublicPackageUploadRejected(
                  rejectionReason = e.getMessage,
                  recordTime = ParticipantEventPublisher.now.toLf,
                  submissionId = submissionId,
                )
              )
          }
      )
      .flatMap { _ =>
        if (vetAllPackages)
          vetPackages(archives, synchronizeVetting)
        else
          EitherT.rightT(())
      }
  }

  override def onClosed(): Unit = Lifecycle.close(packagesDarsStore)(logger)

}
object PackageService {

  def getArchives(filename: String): Either[Throwable, Seq[DamlLf.Archive]] =
    DarParser.readArchiveFromFile(new File(filename)).map(_.all)

  case class DarDescriptor(hash: Hash, name: DarName)

  object DarDescriptor {
    implicit val getResult: GetResult[DarDescriptor] =
      GetResult(r => DarDescriptor(r.<<, String255.tryCreate(r.<<)))
  }

  case class Dar(descriptor: DarDescriptor, bytes: Array[Byte]) {
    override def equals(other: Any): Boolean = other match {
      case that: Dar =>
        // Array equality only returns true when both are the same instance.
        // So by using sameElements to compare the bytes, we ensure that we compare the data, not the instance.
        (bytes sameElements that.bytes) && descriptor == that.descriptor
      case _ => false
    }
  }

  object Dar {
    implicit def getResult(implicit getResultByteArray: GetResult[Array[Byte]]): GetResult[Dar] =
      GetResult(r => Dar(r.<<, r.<<))
  }

  def darToLf(
      dar: Dar
  ): Either[String, (DarDescriptor, archive.Dar[DamlLf.Archive])] = {
    val bytes = dar.bytes
    val payload = ByteString.copyFrom(bytes)
    val stream = new ZipInputStream(payload.newInput())
    DarParser
      .readArchive(dar.descriptor.name.str, stream)
      .fold(
        { _ =>
          Left(s"Cannot parse stored dar $dar")
        },
        x => Right(dar.descriptor -> x),
      )
  }

}
