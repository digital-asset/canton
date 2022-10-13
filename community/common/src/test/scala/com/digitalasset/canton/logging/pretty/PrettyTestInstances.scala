// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.logging.pretty

import com.daml.lf.CantonOnly.LfVersionedTransaction
import com.digitalasset.canton.protocol.{
  LfCommittedTransaction,
  LfKeyWithMaintainers,
  LfNode,
  LfNodeCreate,
  LfNodeExercises,
  LfNodeFetch,
  LfNodeLookupByKey,
  LfNodeRollback,
}

/** Pretty printing implicits for use by tests only. These enable showing readable multiline diffs when expected
  * and actual transactions differ unexpectedly.
  */
trait PrettyTestInstances {
  import Pretty.*

  implicit lazy val prettyLfCommittedTransaction: Pretty[LfCommittedTransaction] = prettyOfClass(
    param("nodes", _.nodes),
    param("roots", _.roots.toList),
  )

  implicit lazy val prettyLfVersionedTransaction: Pretty[LfVersionedTransaction] = prettyOfClass(
    param("nodes", _.nodes),
    param("roots", _.roots.toList),
    param("version", _.version),
  )

  implicit lazy val prettyLfNode: Pretty[LfNode] = {
    case n: LfNodeCreate => prettyLfNodeCreate.treeOf(n)
    case n: LfNodeExercises => prettyLfNodeExercises.treeOf(n)
    case n: LfNodeFetch => prettyLfNodeFetch.treeOf(n)
    case n: LfNodeLookupByKey => prettyLfNodeLookupByKey.treeOf(n)
    case n: LfNodeRollback => prettyLfNodeRollback.treeOf(n)
  }

  implicit lazy val prettyLfNodeCreate: Pretty[LfNodeCreate] = prettyOfClass(
    param("coid", _.coid),
    param("signatories", _.signatories),
    param("stakeholders", _.stakeholders),
    param("templateId", _.templateId),
    param("version", _.version),
    paramIfDefined("key", _.key),
    param("arguments", _.arg),
  )

  implicit lazy val prettyLfNodeExercises: Pretty[LfNodeExercises] = prettyOfClass(
    param("targetCoid", _.targetCoid),
    param("actingParties", _.actingParties),
    param("signatories", _.signatories),
    param("stakeholders", _.stakeholders),
    paramIfNonEmpty("choiceObservers", _.choiceObservers),
    paramIfNonEmpty("children", _.children.toList),
    param("choiceId", _.choiceId.singleQuoted),
    param("chosenValue", _.chosenValue),
    paramIfTrue("consuming", _.consuming),
    param("exerciseResult", _.exerciseResult.showValueOrNone),
    param("templateId", _.templateId),
    param("version", _.version),
    paramIfTrue("byKey", _.byKey),
    paramIfDefined("key", _.key),
  )

  implicit lazy val prettyLfNodeFetch: Pretty[LfNodeFetch] = prettyOfClass(
    param("coid", _.coid),
    param("signatories", _.signatories),
    param("stakeholders", _.stakeholders),
    param("actingParties", _.actingParties),
    param("templateId", _.templateId),
    param("version", _.version),
    paramIfTrue("byKey", _.byKey),
    paramIfDefined("key", _.key),
  )

  implicit lazy val prettyLfNodeLookupByKey: Pretty[LfNodeLookupByKey] = prettyOfClass(
    param("result", _.result.showValueOrNone),
    param("templateId", _.templateId),
    param("version", _.version),
    param("key", _.key),
  )

  implicit lazy val prettyLfNodeRollback: Pretty[LfNodeRollback] = prettyOfClass(
    paramIfNonEmpty("children", _.children.toList)
  )

  implicit lazy val prettyLfKeyWithMaintainers: Pretty[LfKeyWithMaintainers] = prettyOfClass(
    param("key", _.key),
    param("maintainers", _.maintainers),
  )

}

object PrettyTestInstances extends PrettyTestInstances
