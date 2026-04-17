// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy

import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.Ref.*
import com.digitalasset.daml.lf.data.Time
import com.digitalasset.daml.lf.speedy.SError.*
import com.digitalasset.daml.lf.transaction.{FatContractInstance, GlobalKey, NeedKeyProgression}
import com.digitalasset.daml.lf.value.Value.ContractId

object Question {
  sealed abstract class Update extends Product with Serializable
  object Update {

    /** Update interpretation requires the current ledger time.
      */
    final case class NeedTime(callback: Time.Timestamp => Unit) extends Update

    /** Update interpretation requires access to a contract on the ledger. */
    final case class NeedContract(
        contractId: ContractId,
        committers: Set[Party],
        callback: (FatContractInstance, Hash.HashingMethod, Hash => Boolean) => Unit,
    ) extends Update

    /** Machine needs a definition that was not present when the machine was
      * initialized. The caller must retrieve the definition and fill it in
      * the packages cache it had provided to initialize the machine.
      */
    final case class NeedPackage(
        pkg: PackageId,
        context: language.Reference,
        callback: CompiledPackages => Unit,
    ) extends Update

    // Requests up to `limit` FatContractInstances matching `key`, delivered via `callback`.
    // `callback` takes at most `limit` contracts and a progression token:
    //   - Finished when all matches have been delivered (only valid with strictly fewer than `limit` results),
    //   - InProgress when more results may follow.
    // `progression` is Unstarted on the first call, InProgress on continuations.
    final case class NeedKey(
        key: GlobalKey,
        limit: Int,
        progression: NeedKeyProgression.CanContinue,
        committers: Set[Party],
        callback: (Vector[FatContractInstance], NeedKeyProgression.HasStarted) => Unit,
    ) extends Update

    /** Update interpretation requires data from an external TCP service.
      * The host must execute the fetch (during submission) or supply pinned data
      * (during validation), then invoke the callback with the response.
      *
      * See CIP-draft-external-data-pinning.
      */
    final case class NeedExternalFetch(
        endpoints: Seq[String], // fallback chain of endpoints "host:port", tried left-to-right
        payload: Array[Byte], // request payload
        signerKeys: Seq[Array[Byte]], // accepted signing public keys (DER)
        maxBytes: Int,
        timeoutMs: Int,
        nonce: Array[Byte], // 32-byte transaction-bound nonce
        callback: ExternalFetchResult => Unit,
    ) extends Update

    /** Result of an external fetch, passed back to the Speedy machine via callback. */
    final case class ExternalFetchResult(
        body: Array[Byte],
        signature: Array[Byte],
        signerKey: Array[Byte],
        fetchedAt: Long, // microseconds since epoch
    )
  }
}

/** The result from small-step evaluation.
  * If the result is not Done or Continue, then the machine
  * must be fed before it can be stepped further.
  */
sealed abstract class SResult[+Q] extends Product with Serializable

object SResult {

  final case class SResultQuestion[Q](question: Q) extends SResult[Q]

  /** The speedy machine has completed evaluation to reach a final value.
    * And, if the evaluation was on-ledger, a completed transaction.
    */
  final case class SResultFinal(v: SValue) extends SResult[Nothing]

  final case class SResultError(err: SError) extends SResult[Nothing]

  final case object SResultInterruption extends SResult[Nothing]

  sealed abstract class SVisibleToStakeholders extends Product with Serializable
  object SVisibleToStakeholders {
    // actAs and readAs are only included for better error messages.
    final case class NotVisible(
        actAs: Set[Party],
        readAs: Set[Party],
    ) extends SVisibleToStakeholders
    final case object Visible extends SVisibleToStakeholders

    def fromSubmitters(
        actAs: Set[Party],
        readAs: Set[Party] = Set.empty,
    ): Set[Party] => SVisibleToStakeholders = {
      val readers = actAs union readAs
      stakeholders =>
        if (readers.intersect(stakeholders).nonEmpty) {
          SVisibleToStakeholders.Visible
        } else {
          SVisibleToStakeholders.NotVisible(actAs, readAs)
        }
    }
  }
}
