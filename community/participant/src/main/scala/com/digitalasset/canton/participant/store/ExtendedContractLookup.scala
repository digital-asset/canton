// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.data.{EitherT, OptionT}
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.protocol.{LfContractId, LfGlobalKey}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

/** A contract lookup that adds a fixed set of contracts to a `backingContractLookup`.
  *
  * @param backingContractLookup The [[ContractLookup]] to default to if no overwrite is given in `additionalContracts`
  * @param additionalContracts Contracts in this map take precedence over contracts in `backingContractLookup`;
  *                            contract details (ledger time, contract instance) are overwritten and
  *                            the lower request counter wins.
  * @throws java.lang.IllegalArgumentException if `additionalContracts` stores a contract under a wrong id
  */
class ExtendedContractLookup(
    private val backingContractLookup: ContractLookup,
    private val additionalContracts: Map[LfContractId, StoredContract],
    private val keys: Map[LfGlobalKey, Option[LfContractId]],
)(protected implicit val ec: ExecutionContext)
    extends ContractAndKeyLookup {

  additionalContracts.foreach { case (id, storedContract) =>
    require(
      storedContract.contractId == id,
      s"Tried to store contract $storedContract under the wrong id $id",
    )
  }

  protected[store] override def logger: TracedLogger = backingContractLookup.logger

  override def lookup(
      id: LfContractId
  )(implicit traceContext: TraceContext): OptionT[Future, StoredContract] =
    additionalContracts.get(id) match {
      case None => backingContractLookup.lookup(id)
      case Some(inFlightContract) =>
        backingContractLookup
          .lookup(id)
          .transform({
            case None => Some(inFlightContract)
            case Some(storedContract) => Some(storedContract.mergeWith(inFlightContract))
          })
    }

  override def lookupKey(key: LfGlobalKey)(implicit
      traceContext: TraceContext
  ): OptionT[Future, Option[LfContractId]] =
    OptionT.fromOption[Future](keys.get(key))

  // This lookup is fairly inefficient in the additional stakeholders, but this function is currently not really
  // used anywhere
  override def lookupStakeholders(ids: Set[LfContractId])(implicit
      traceContext: TraceContext
  ): EitherT[Future, UnknownContracts, Map[LfContractId, Set[LfPartyId]]] = {
    val (inAdditional, notInAdditional) = ids.partition(cid => additionalContracts.contains(cid))
    for {
      m <- backingContractLookup.lookupStakeholders(notInAdditional)
    } yield {
      m ++ additionalContracts.filter { case (cid, _) => inAdditional.contains(cid) }.map {
        case (cid, c) => (cid, c.contract.metadata.stakeholders)
      }
    }

  }

}
