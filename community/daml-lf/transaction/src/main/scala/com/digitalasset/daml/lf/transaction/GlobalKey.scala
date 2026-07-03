// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package transaction

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.{PackageName, Party, TypeConId}
import com.digitalasset.daml.lf.value.Value

/** Useful in various circumstances -- basically this is what a ledger implementation must use as a
  * key. The 'hash' is guaranteed to be stable over time.
  */
final case class GlobalKey(
    templateId: Ref.TypeConId,
    packageName: Ref.PackageName,
    key: Value,
    hash: crypto.Hash,
) extends {

  override def equals(obj: Any): Boolean = obj match {
    case that: GlobalKey => this.hash == that.hash
    case _ => false
  }

  def qualifiedName: Ref.QualifiedName = templateId.qualifiedName

  override def hashCode(): Int = hash.hashCode()

  def nonVerbose: GlobalKey = copy(key = key.nonVerboseWithoutTrailingNones)
}

object GlobalKey {

  implicit val globalKeyOrdering: Ordering[GlobalKey] = Ordering.by(_.hash)

}

final case class GlobalKeyWithMaintainers(
    globalKey: GlobalKey,
    maintainers: Set[Ref.Party],
) {
  def value: Value = globalKey.key

  def nonVerbose: GlobalKeyWithMaintainers = copy(globalKey = globalKey.nonVerbose)
}

object GlobalKeyWithMaintainers {

  def apply(
      templateId: TypeConId,
      value: Value,
      valueHash: crypto.Hash,
      maintainers: Set[Party],
      packageName: PackageName,
  ): GlobalKeyWithMaintainers =
    GlobalKeyWithMaintainers(GlobalKey(templateId, packageName, value, valueHash), maintainers)
}

/** Controls whether the engine should error out when it encounters duplicate keys. This is always
  * turned on with the exception of Canton which allows turning this on or off and forces it to be
  * turned off in multi-domain mode.
  */
sealed abstract class LegacyContractKeyUniquenessMode extends Product with Serializable

object LegacyContractKeyUniquenessMode {

  /** Disable key uniqueness checks and only consider byKey operations. Note that no stable
    * semantics are provided for off mode.
    */
  case object Off extends LegacyContractKeyUniquenessMode

  /** Considers all nodes mentioning keys as byKey operations and checks for contract key
    * uniqueness.
    */
  case object Strict extends LegacyContractKeyUniquenessMode
}
