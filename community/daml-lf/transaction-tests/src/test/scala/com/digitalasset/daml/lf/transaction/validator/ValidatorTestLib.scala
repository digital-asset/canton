// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package transaction
package validator

import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.{ImmArray, Ref, Time}
import com.digitalasset.daml.lf.transaction.test.TransactionBuilder
import com.digitalasset.daml.lf.value.Value as V

object ValidatorTestLib {
  import TransactionBuilder.Implicits.*

  implicit val version: SerializationVersion = SerializationVersion.minVersion

  val metadata = Transaction.Metadata(
    submissionSeed = None,
    preparationTime = Time.Timestamp.now(),
    usedPackages = Set.empty,
    timeBoundaries = Time.Range.unconstrained,
    nodeSeeds = ImmArray.empty,
    globalKeyMapping = Map.empty,
    contractOrder = List.empty,
  )
  val pkgName = Ref.PackageName.assertFromString("PkgName")
  val coid = V.ContractId.V1(Hash.hashPrivateKey("#cid1"))
  val templateId = "DummyModule:dummyName"
  val choiceName = "dummyChoice"
  val choiceArg = V.ValueUnit

  def globalKey(
      value: V = V.ValueUnit,
      maintainers: Set[Ref.Party] = Set(Ref.Party.assertFromString("Alice")),
  ): GlobalKeyWithMaintainers =
    GlobalKeyWithMaintainers(
      GlobalKey(
        templateId = templateId,
        packageName = pkgName,
        key = value,
        hash = Hash.assertHashContractKeyUnsafe(
          templateId,
          pkgName,
          value,
        ),
      ),
      maintainers,
    )

  def createNode(
      arg: V = V.ValueUnit,
      stakeholders: Set[Ref.Party] = Set.empty,
      signatories: Set[Ref.Party] = Set.empty,
      keyOpt: Option[GlobalKeyWithMaintainers] = None,
  ): Node.Create =
    Node.Create(
      coid = coid,
      packageName = pkgName,
      templateId = templateId,
      arg = arg,
      stakeholders = stakeholders,
      signatories = signatories,
      keyOpt = keyOpt,
      version = version,
    )

  def exerciseNode(
      choiceArg: V = V.ValueUnit,
      result: Option[V] = None,
      parties: Set[Ref.Party] = Set.empty,
      authorizers: Option[Set[Ref.Party]] = None,
      observers: Set[Ref.Party] = Set.empty,
      signatories: Set[Ref.Party] = Set.empty,
      stakeholders: Set[Ref.Party] = Set.empty,
      children: ImmArray[NodeId] = ImmArray.empty,
      keyOpt: Option[GlobalKeyWithMaintainers] = None,
      callResults: ImmArray[ExternalCallResult] = ImmArray.empty,
  ): Node.Exercise =
    Node.Exercise(
      targetCoid = coid,
      packageName = pkgName,
      templateId = templateId,
      interfaceId = None,
      choiceId = choiceName,
      consuming = true,
      actingParties = parties,
      chosenValue = choiceArg,
      stakeholders = stakeholders,
      signatories = signatories,
      choiceObservers = observers,
      choiceAuthorizers = authorizers,
      children = children,
      exerciseResult = result,
      keyOpt = keyOpt,
      byKey = false,
      externalCallResults = callResults,
      version = version,
    )

  def fetchNode(
      parties: Set[Ref.Party] = Set.empty,
      stakeholders: Set[Ref.Party] = Set.empty,
      signatories: Set[Ref.Party] = Set.empty,
      keyOpt: Option[GlobalKeyWithMaintainers] = None,
  ): Node.Fetch =
    Node.Fetch(
      coid = coid,
      packageName = pkgName,
      templateId = templateId,
      actingParties = parties,
      stakeholders = stakeholders,
      signatories = signatories,
      keyOpt = keyOpt,
      byKey = false,
      interfaceId = None,
      version = version,
    )

  def queryNode(key: GlobalKeyWithMaintainers): Node.QueryByKey =
    Node.QueryByKey(
      packageName = pkgName,
      templateId = templateId,
      exhaustive = true,
      key = key,
      result = Vector.empty,
      version = version,
    )
}
