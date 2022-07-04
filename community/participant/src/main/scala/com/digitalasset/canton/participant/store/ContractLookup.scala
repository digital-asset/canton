// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.data.{EitherT, OptionT}
import cats.syntax.traverse._
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.protocol.{
  LfContractId,
  LfContractInst,
  LfGlobalKey,
  SerializableContract,
}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

trait ContractLookup {

  protected implicit def ec: ExecutionContext

  protected[store] def logger: TracedLogger

  def lookup(id: LfContractId)(implicit traceContext: TraceContext): OptionT[Future, StoredContract]

  def lookupManyUncached(
      ids: Seq[LfContractId]
  )(implicit traceContext: TraceContext): Future[List[Option[StoredContract]]] =
    ids.toList.traverse(lookup(_).value)

  def lookupE(id: LfContractId)(implicit
      traceContext: TraceContext
  ): EitherT[Future, UnknownContract, StoredContract] =
    lookup(id).toRight(UnknownContract(id))

  /** Yields `None` (embedded in a Future) if the contract instance has not been stored or the id cannot be parsed.
    *
    * Discards the serialization.
    */
  def lookupLfInstance(lfId: LfContractId)(implicit
      traceContext: TraceContext
  ): OptionT[Future, LfContractInst] =
    lookup(lfId).map(_.contract.contractInstance)

  def lookupContract(id: LfContractId)(implicit
      traceContext: TraceContext
  ): OptionT[Future, SerializableContract] =
    lookup(id).map(_.contract)

  def lookupContractE(id: LfContractId)(implicit
      traceContext: TraceContext
  ): EitherT[Future, UnknownContract, SerializableContract] =
    lookupE(id).map(_.contract)

  def lookupStakeholders(ids: Set[LfContractId])(implicit
      traceContext: TraceContext
  ): EitherT[Future, UnknownContracts, Map[LfContractId, Set[LfPartyId]]]

}

object ContractLookup {
  def noContracts(logger: TracedLogger): ContractLookup =
    ContractAndKeyLookup.noContracts(logger)

}

trait ContractAndKeyLookup extends ContractLookup {

  /** Find a contract with the given key. Typically used for Daml interpretation in Phase 3, where the key resolution
    * is provided by the submitter.
    * Returns [[scala.None$]] if the key is not supposed to be resolved, e.g., during reinterpretation by Daml Engine.
    * Returns [[scala.Some$]]`(`[[scala.None$]]`)` if no contract with the given key can be found.
    */
  def lookupKey(key: LfGlobalKey)(implicit
      traceContext: TraceContext
  ): OptionT[Future, Option[LfContractId]]

}

object ContractAndKeyLookup {

  /** An empty contract and key lookup interface that fails to find any contracts and keys when asked,
    * but allows any key to be asked
    */
  def noContracts(logger1: TracedLogger): ContractAndKeyLookup = {
    new ContractAndKeyLookup {
      implicit val ec: ExecutionContext = DirectExecutionContext(logger1)
      override def logger: TracedLogger = logger1

      override def lookup(id: LfContractId)(implicit
          traceContext: TraceContext
      ): OptionT[Future, StoredContract] =
        OptionT.none[Future, StoredContract]

      override def lookupManyUncached(ids: Seq[LfContractId])(implicit
          traceContext: TraceContext
      ): Future[List[Option[StoredContract]]] = Future.successful(ids.map(_ => None).toList)

      override def lookupKey(key: LfGlobalKey)(implicit
          traceContext: TraceContext
      ): OptionT[Future, Option[LfContractId]] =
        OptionT.pure[Future](None)

      override def lookupStakeholders(ids: Set[LfContractId])(implicit
          traceContext: TraceContext
      ): EitherT[Future, UnknownContracts, Map[LfContractId, Set[LfPartyId]]] =
        EitherT.cond(ids.isEmpty, Map.empty, UnknownContracts(ids))

    }
  }

}
