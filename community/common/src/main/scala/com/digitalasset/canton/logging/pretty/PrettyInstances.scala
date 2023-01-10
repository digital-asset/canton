// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.logging.pretty

import cats.Show.Shown
import com.daml.ledger.api.DeduplicationPeriod
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset.LedgerBoundary
import com.daml.ledger.client.binding.Primitive
import com.daml.ledger.participant.state.v2
import com.daml.ledger.participant.state.v2.ChangeId
import com.daml.ledger.{configuration, offset}
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.{DottedName, PackageId, QualifiedName}
import com.daml.lf.transaction.ContractStateMachine.ActiveLedgerState
import com.daml.lf.transaction.Transaction.{
  DuplicateContractKey,
  InconsistentContractKey,
  KeyInputError,
}
import com.daml.lf.value.Value
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.topology.UniqueIdentifier
import com.digitalasset.canton.tracing.{TraceContext, W3CTraceContext}
import com.digitalasset.canton.util.ShowUtil.HashLength
import com.digitalasset.canton.util.{ErrorUtil, HexString}
import com.digitalasset.canton.{LedgerApplicationId, LfPartyId, LfTimestamp}
import com.google.protobuf.ByteString
import io.grpc.Status
import pprint.Tree
import slick.util.{DumpInfo, Dumpable}

import java.net.URI
import java.time.{Duration as JDuration, Instant}
import java.util.UUID
import scala.concurrent.duration.Duration

/** Collects instances of [[Pretty]] for common types.
  */
trait PrettyInstances {

  import Pretty.*

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  implicit def prettyPrettyPrinting[T <: PrettyPrinting]: Pretty[T] =
    // Cast is required to make IDEA happy.
    inst => inst.pretty.treeOf(inst.asInstanceOf[inst.type])

  implicit def prettyTree[T <: Tree]: Pretty[T] = identity

  /** Makes the syntax from [[com.digitalasset.canton.util.ShowUtil]] accessible in places where a Pretty is expected.
    */
  implicit def prettyShown: Pretty[Shown] = prettyOfString(_.toString)

  implicit def prettyInt: Pretty[Int] = prettyOfString(_.toString)

  implicit def prettyLong: Pretty[Long] = prettyOfString(_.toString)

  implicit def prettyBoolean: Pretty[Boolean] = prettyOfString(_.toString)

  implicit val prettyUnit: Pretty[Unit] = prettyOfString(_ => "()")

  implicit def prettySeq[T: Pretty]: Pretty[Seq[T]] = treeOfIterable("Seq", _)

  implicit def prettyNonempty[T: Pretty]: Pretty[NonEmpty[T]] =
    NonEmptyUtil.instances.prettyNonEmpty

  implicit def prettyArray[T: Pretty]: Pretty[Array[T]] = treeOfIterable("Array", _)

  implicit def prettySet[T: Pretty]: Pretty[Set[T]] = treeOfIterable("Set", _)

  implicit def prettyPair[T1: Pretty, T2: Pretty]: Pretty[(T1, T2)] =
    prettyNode("Pair", param("fst", _._1), param("snd", _._2))

  implicit def prettyTriple[T1: Pretty, T2: Pretty, T3: Pretty]: Pretty[(T1, T2, T3)] =
    prettyNode("Triple", param("#1", _._1), param("#2", _._2), param("#3", _._3))

  implicit def prettyOption[T: Pretty]: Pretty[Option[T]] = {
    case None => Tree.Apply("None", Iterator())
    case Some(x) => Tree.Apply("Some", Iterator(x.toTree))
  }

  implicit def prettyEither[L: Pretty, R: Pretty]: Pretty[Either[L, R]] = {
    case Left(x) => Tree.Apply("Left", Iterator(x.toTree))
    case Right(x) => Tree.Apply("Right", Iterator(x.toTree))
  }

  implicit def prettyThrowable: Pretty[Throwable] = prettyOfString(ErrorUtil.messageWithStacktrace)

  implicit def prettyMap[K: Pretty, V: Pretty]: Pretty[collection.Map[K, V]] =
    elements =>
      treeOfIterable("Map", elements.map { case (k, v) => Tree.Infix(k.toTree, "->", v.toTree) })

  private def treeOfIterable[T: Pretty](prefix: String, elements: Iterable[T]): Tree =
    if (elements.sizeCompare(1) == 0) {
      elements.iterator.next().toTree
    } else {
      Tree.Apply(prefix, elements.map(_.toTree).iterator)
    }

  implicit def prettyJDuration: Pretty[JDuration] = prettyOfString(
    // https://stackoverflow.com/a/40487511/6346418
    _.toString.substring(2).replaceAll("(\\d[HMS])(?!$)", "$1 ").toLowerCase
  )

  implicit def prettyDuration: Pretty[Duration] = prettyOfString(_.toString)

  implicit def prettyURI: Pretty[URI] = prettyOfString(_.toString)

  implicit def prettyInstant: Pretty[Instant] = prettyOfString(_.toString)

  implicit val prettyUuid: Pretty[UUID] = prettyOfString(_.toString.readableHash.toString)

  // There is deliberately no instance for `String` to force clients
  // use ShowUtil.ShowStringSyntax instead.
  def prettyString: Pretty[String] = prettyOfString(identity)

  implicit val prettyByteString: Pretty[ByteString] =
    prettyOfString(b => HexString.toHexString(b, HashLength).readableHash.toString)

  implicit def prettyDumpInfo: Pretty[DumpInfo] = {
    implicit def prettyDumpInfoChild: Pretty[(String, Dumpable)] = { case (label, child) =>
      Tree.Infix(label.unquoted.toTree, "=", child.toTree)
    }

    prettyOfClass(
      param("name", _.name.singleQuoted, _.name.nonEmpty),
      unnamedParam(_.mainInfo.doubleQuoted, _.mainInfo.nonEmpty),
      unnamedParamIfNonEmpty(_.children.toSeq),
      // Omitting attrInfo, as it may contain confidential data.
    )
  }

  implicit def prettyDumpable: Pretty[Dumpable] = prettyOfParam(_.getDumpInfo)

  implicit def prettyLedgerString: Pretty[Ref.LedgerString] = prettyOfString(id => id: String)

  implicit val prettyLedgerBoundary: Pretty[LedgerBoundary] = {
    case LedgerBoundary.LEDGER_BEGIN => Tree.Literal("LEDGER_BEGIN")
    case LedgerBoundary.LEDGER_END => Tree.Literal("LEDGER_END")
    case LedgerBoundary.Unrecognized(value) => Tree.Literal(s"Unrecognized($value)")
  }

  implicit val prettyLedgerOffset: Pretty[LedgerOffset] = {
    case LedgerOffset(LedgerOffset.Value.Absolute(absolute)) =>
      Tree.Apply("AbsoluteOffset", Iterator(Tree.Literal(absolute)))
    case LedgerOffset(LedgerOffset.Value.Boundary(boundary)) =>
      Tree.Apply("Boundary", Iterator(boundary.toTree))
    case LedgerOffset(LedgerOffset.Value.Empty) => Tree.Literal("Empty")
  }

  implicit val prettyReadServiceOffset: Pretty[offset.Offset] = prettyOfString(
    // Do not use `toReadableHash` because this is not a hash but a hex-encoded string
    // whose end contains the most important information
    _.toHexString
  )

  implicit def prettyLfParticipantId: Pretty[Ref.ParticipantId] = prettyOfString(prettyUidString(_))

  implicit def prettyLedgerApplicationId: Pretty[LedgerApplicationId] = prettyOfString(
    prettyUidString(_)
  )

  implicit def prettyLfTimestamp: Pretty[LfTimestamp] = prettyOfString(_.toString)

  implicit def prettyLfPartyId: Pretty[LfPartyId] = prettyOfString(prettyUidString(_))

  implicit def prettyLfHash: Pretty[LfHash] = prettyOfString(_.toHexString.readableHash.toString)

  implicit val prettyNodeId: Pretty[LfNodeId] = prettyOfParam(_.index)

  implicit def prettyPrimitiveParty: Pretty[Primitive.Party] =
    prettyOfString(partyId => prettyUidString(scalaz.Tag.unwrap(partyId)))

  private def prettyUidString(partyStr: String): String =
    UniqueIdentifier.fromProtoPrimitive_(partyStr) match {
      case Right(uid) => uid.show
      case Left(_) => partyStr
    }

  implicit def prettyPackageId: Pretty[PackageId] = prettyOfString(id => show"${id.readableHash}")

  implicit def prettyChangeId: Pretty[ChangeId] = prettyOfClass(
    param("application Id", _.applicationId),
    param("command Id", _.commandId),
    param("act as", _.actAs),
  )

  implicit def prettyLfDottedName: Pretty[DottedName] = prettyOfString { dottedName =>
    val segments = dottedName.segments
    val prefixes = segments.length - 1
    val shortenedPrefixes = if (prefixes > 0) {
      segments.init.map(_.substring(0, 1)).toSeq.mkString(".") + "."
    } else ""
    shortenedPrefixes + segments.last
  }

  implicit def prettyLfQualifiedName: Pretty[QualifiedName] =
    prettyOfString(qname => show"${qname.module}:${qname.name}")

  implicit def prettyLfIdentifier: Pretty[com.daml.lf.data.Ref.Identifier] =
    prettyOfString(id => show"${id.packageId}:${id.qualifiedName}")

  implicit def prettyLfContractId: Pretty[LfContractId] = prettyOfString {
    case LfContractId.V1(discriminator, suffix)
        // Shorten only Canton contract ids
        if suffix.startsWith(NonAuthenticatedContractIdVersion.versionPrefixBytes) ||
          suffix.startsWith(AuthenticatedContractIdVersion.versionPrefixBytes) =>
      val prefixBytesSize = CantonContractIdVersion.versionPrefixBytesSize

      val cantonVersionPrefix = suffix.slice(0, prefixBytesSize)
      val rawSuffix = suffix.slice(prefixBytesSize, suffix.length)

      discriminator.toHexString.readableHash.toString +
        cantonVersionPrefix.toHexString +
        rawSuffix.toHexString.readableHash.toString
    case lfContractId: LfContractId =>
      // Don't abbreviate anything for unusual contract ids
      lfContractId.toString
  }

  implicit def prettyLfTransactionVersion: Pretty[LfTransactionVersion] = prettyOfString(
    _.protoValue
  )

  implicit def prettyPrimitiveContractId: Pretty[Primitive.ContractId[_]] = prettyOfString { coid =>
    val coidStr = scalaz.Tag.unwrap(coid)
    val tokens = coidStr.split(':')
    if (tokens.lengthCompare(2) == 0) {
      tokens(0).readableHash.toString + ":" + tokens(1).readableHash.toString
    } else {
      // Don't abbreviate anything for unusual contract ids
      coidStr
    }
  }

  implicit def prettyLfGlobalKey: Pretty[LfGlobalKey] = prettyOfClass(
    param("templateId", _.templateId),
    param("hash", _.hash.toHexString.readableHash),
  )

  implicit def prettyLedgerTimeModel: Pretty[configuration.LedgerTimeModel] = prettyOfClass(
    param("avgTransactionLatency", _.avgTransactionLatency),
    param("minSkew", _.minSkew),
    param("maxSkew", _.maxSkew),
  )

  implicit def prettyLedgerConfiguration: Pretty[configuration.Configuration] = prettyOfClass(
    param("generation", _.generation),
    param("maxDeduplicationDuration", _.maxDeduplicationDuration),
    param("timeModel", _.timeModel),
  )

  implicit def prettyV2CompletionInfo: Pretty[v2.CompletionInfo] = prettyOfClass(
    param("actAs", _.actAs.mkShow()),
    param("commandId", _.commandId),
    param("applicationId", _.applicationId),
    paramIfDefined("deduplication period", _.optDeduplicationPeriod),
    param("submissionId", _.submissionId),
  )

  implicit def prettyV2DeduplicationPeriod: Pretty[DeduplicationPeriod] =
    prettyOfString {
      case deduplicationDuration: DeduplicationPeriod.DeduplicationDuration =>
        s"DeduplicationDuration(duration=${deduplicationDuration.duration}"
      case dedupOffset: DeduplicationPeriod.DeduplicationOffset =>
        s"DeduplicationOffset(offset=${dedupOffset.offset}"
    }

  implicit def prettyV2TransactionMeta: Pretty[v2.TransactionMeta] = prettyOfClass(
    param("ledgerEffectiveTime", _.ledgerEffectiveTime),
    paramIfDefined("workflowId", _.workflowId),
    param("submissionTime", _.submissionTime),
    customParam(_ => "..."),
  )
  implicit def prettyCompletion: Pretty[Completion] =
    prettyOfClass(
      unnamedParamIfDefined(_.status),
      param("commandId", _.commandId.singleQuoted),
      param("transactionId", _.transactionId.singleQuoted, _.transactionId.nonEmpty),
    )

  implicit def prettyRpcStatus: Pretty[com.google.rpc.status.Status] =
    prettyOfClass(
      customParam(rpcStatus => Status.fromCodeValue(rpcStatus.code).getCode.toString),
      customParam(_.message),
      paramIfNonEmpty("details", _.details.map(_.toString.unquoted)),
    )

  implicit def prettyGrpcStatus: Pretty[io.grpc.Status] =
    prettyOfClass(
      param("code", _.getCode.name().unquoted),
      paramIfDefined("description", x => Option(x.getDescription()).map(_.doubleQuoted)),
      paramIfDefined("cause", x => Option(x.getCause()).map(_.getMessage.doubleQuoted)),
    )

  implicit lazy val prettyValue: Pretty[Value] =
    adHocPrettyInstance // TODO(#3269) Using this pretty-printer may leak confidential data.

  implicit lazy val prettyVersionedValue: Pretty[Value.VersionedValue] = prettyOfClass(
    unnamedParam(_.unversioned),
    param("version", _.version),
  )

  implicit val prettyW3CTraceContext: Pretty[W3CTraceContext] = prettyOfClass(
    param("parent", _.parent.unquoted),
    paramIfDefined("state", _.state.map(_.unquoted)),
  )

  implicit val prettyTraceContext: Pretty[TraceContext] = prettyOfClass(
    paramIfDefined("trace id", _.traceId.map(_.unquoted)),
    paramIfDefined("W3C context", _.asW3CTraceContext),
  )

  implicit val prettyKeyInputError: Pretty[KeyInputError] = {
    case Left(e: InconsistentContractKey) =>
      prettyOfClass[InconsistentContractKey](unnamedParam(_.key)).treeOf(e)
    case Right(e: DuplicateContractKey) =>
      prettyOfClass[DuplicateContractKey](unnamedParam(_.key)).treeOf(e)
  }

  implicit def prettyActiveLedgerState[T: Pretty]: Pretty[ActiveLedgerState[T]] =
    prettyOfClass[ActiveLedgerState[T]](
      param("locallyCreatedThisTimeline", _.locallyCreatedThisTimeline),
      param("consumedBy", _.consumedBy),
      param("localActiveKeys", _.localActiveKeys),
    )
}

object PrettyInstances extends PrettyInstances
