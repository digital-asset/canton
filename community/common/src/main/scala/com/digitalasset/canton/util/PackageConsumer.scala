// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.data.EitherT
import com.digitalasset.canton.LfPackageId
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.PackageConsumer.{ContinueOnInterruption, PackageResolver}
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.engine.*
import com.digitalasset.daml.lf.language.Ast.Package

import scala.concurrent.ExecutionContext

object PackageConsumer {
  type ContinueOnInterruption = () => Boolean

  trait PackageResolver {
    def resolve(packageId: PackageId, onMissingPackage: PackageId => FutureUnlessShutdown[Unit])(
        implicit
        traceContext: TraceContext,
        executionContext: ExecutionContext,
    ): FutureUnlessShutdown[Option[Package]] = for {
      packageO <- resolveInternal(packageId)
      _ <- packageO match {
        case Some(_) => FutureUnlessShutdown.unit
        case None => onMissingPackage(packageId)
      }
    } yield packageO

    protected def resolveInternal(packageId: PackageId)(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Option[Package]]
  }

  object PackageResolver {

    val ignoreMissingPackage: PackageId => FutureUnlessShutdown[Unit] = _ =>
      FutureUnlessShutdown.unit

    /** Yields a failed future, if:
      *   - The package is missing in the store.
      *   - The participant `participantId` has vetted the package nevertheless at ledgerTime in
      *     `topologySnapshot`.
      */
    def crashOnMissingPackage(
        topologySnapshot: TopologySnapshot,
        participantId: ParticipantId,
        ledgerTime: CantonTimestamp,
    )(
        packageId: PackageId
    )(implicit
        errorLoggingContext: ErrorLoggingContext,
        executionContext: ExecutionContext,
    ): FutureUnlessShutdown[Unit] = {
      implicit val traceContext: TraceContext = errorLoggingContext.traceContext

      topologySnapshot.vettedPackages(participantId).map { vettedPackages =>
        vettedPackages
          .find(vp => vp.packageId == packageId && vp.validAt(ledgerTime))
          .foreach { vettedPackage =>
            ErrorUtil.internalError(
              new IllegalStateException(
                s"""Unable to load package $packageId even though the package has been vetted: $vettedPackage. Crashing...
                   |To recover from this error upload the package with id $packageId to the participant and then reconnect the participant to the synchronizer.""".stripMargin
              )
            )
          }
      }
    }
  }
}

abstract class PackageConsumer(
    packageResolver: PackageResolver,
    continueOnInterruption: ContinueOnInterruption,
) {

  private def resolve(
      packageId: LfPackageId,
      onMissingPackage: PackageId => FutureUnlessShutdown[Unit],
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, String, Option[Package]] =
    EitherT.right[String].apply(packageResolver.resolve(packageId, onMissingPackage))

  def consume[V](result: Result[V], onMissingPackage: PackageId => FutureUnlessShutdown[Unit])(
      implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, String, V] =
    result match {
      case ResultError(e) =>
        EitherT.leftT[FutureUnlessShutdown, V](e.toString)

      case ResultNeedPackage(packageId, resume) =>
        for {
          p <- resolve(packageId, onMissingPackage)
          r <- consume(resume(p), onMissingPackage)
        } yield r

      case ResultInterruption(continue, abort) =>
        if (continueOnInterruption()) {
          consume(continue(), onMissingPackage)
        } else {
          val reason = abort()
          EitherT.leftT[FutureUnlessShutdown, V](
            s"Aborted engine: ${reason.getOrElse("no context provided")}"
          )
        }

      case ResultDone(result) =>
        EitherT.rightT[FutureUnlessShutdown, String](result)

      case other =>
        EitherT.leftT[FutureUnlessShutdown, V](s"PackageConsumer did not expect: $other")
    }

}
