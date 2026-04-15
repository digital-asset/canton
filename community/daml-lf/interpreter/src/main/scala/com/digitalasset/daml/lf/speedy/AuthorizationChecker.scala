// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy

import com.digitalasset.daml.lf.data.Ref.{ChoiceName, Location, Party, TypeConId}
import com.digitalasset.daml.lf.ledger.Authorize
import com.digitalasset.daml.lf.ledger.FailedAuthorization

private[lf] abstract class AuthorizationChecker {

  private[lf] def authorizeCreate(
      optLocation: Option[Location],
      templateId: TypeConId,
      signatories: Set[Party],
      maintainers: Option[Set[Party]],
  )(
      auth: Authorize
  ): List[FailedAuthorization]

  private[lf] def authorizeFetch(
      optLocation: Option[Location],
      templateId: TypeConId,
      stakeholders: Set[Party],
  )(
      auth: Authorize
  ): List[FailedAuthorization]

  private[lf] def authorizeLookupByKey(
      optLocation: Option[Location],
      templateId: TypeConId,
      maintainers: Set[Party],
  )(
      auth: Authorize
  ): List[FailedAuthorization]

  private[lf] def authorizeExercise(
      optLocation: Option[Location],
      templateId: TypeConId,
      choiceId: ChoiceName,
      actingParties: Set[Party],
      choiceAuthorizers: Option[Set[Party]],
  )(
      auth: Authorize
  ): List[FailedAuthorization]
}

/** Default AuthorizationChecker intended for production usage. */
private[lf] object DefaultAuthorizationChecker extends AuthorizationChecker {

  @inline
  private[this] def authorize(
      passIf: Boolean,
      failWith: => FailedAuthorization,
  ): List[FailedAuthorization] = {
    if (passIf)
      List()
    else
      List(failWith)
  }

  override private[lf] def authorizeCreate(
      optLocation: Option[Location],
      templateId: TypeConId,
      signatories: Set[Party],
      maintainers: Option[Set[Party]],
  )(
      auth: Authorize
  ): List[FailedAuthorization] = {
    authorize(
      passIf = signatories subsetOf auth.authParties,
      failWith = FailedAuthorization.CreateMissingAuthorization(
        templateId = templateId,
        optLocation = optLocation,
        authorizingParties = auth.authParties,
        requiredParties = signatories,
      ),
    ) ++
      authorize(
        passIf = signatories.nonEmpty,
        failWith = FailedAuthorization.NoSignatories(templateId, optLocation),
      ) ++
      (maintainers match {
        case None => List()
        case Some(maintainers) =>
          authorize(
            passIf = maintainers subsetOf signatories,
            failWith = FailedAuthorization.MaintainersNotSubsetOfSignatories(
              templateId = templateId,
              optLocation = optLocation,
              signatories = signatories,
              maintainers = maintainers,
            ),
          )
      })
  }

  override private[lf] def authorizeFetch(
      optLocation: Option[Location],
      templateId: TypeConId,
      stakeholders: Set[Party],
  )(
      auth: Authorize
  ): List[FailedAuthorization] = {
    authorize(
      passIf = stakeholders.intersect(auth.authParties).nonEmpty,
      failWith = FailedAuthorization.FetchMissingAuthorization(
        templateId = templateId,
        optLocation = optLocation,
        stakeholders = stakeholders,
        authorizingParties = auth.authParties,
      ),
    )
  }

  override private[lf] def authorizeLookupByKey(
      optLocation: Option[Location],
      templateId: TypeConId,
      maintainers: Set[Party],
  )(
      auth: Authorize
  ): List[FailedAuthorization] = {
    authorize(
      passIf = maintainers subsetOf auth.authParties,
      failWith = FailedAuthorization.LookupByKeyMissingAuthorization(
        templateId,
        optLocation,
        maintainers,
        auth.authParties,
      ),
    )
  }

  override private[lf] def authorizeExercise(
      optLocation: Option[Location],
      templateId: TypeConId,
      choiceId: ChoiceName,
      actingParties: Set[Party],
      choiceAuthorizers: Option[Set[Party]],
  )(
      auth: Authorize
  ): List[FailedAuthorization] = {
    authorize(
      passIf = actingParties.nonEmpty,
      failWith = FailedAuthorization.NoControllers(templateId, choiceId, optLocation),
    ) ++
      authorize(
        passIf = choiceAuthorizers match {
          case None => true
          case Some(explicitAuthorizers) => explicitAuthorizers.nonEmpty
        },
        failWith = FailedAuthorization.NoAuthorizers(templateId, choiceId, optLocation),
      ) ++
      authorize(
        passIf = (actingParties union choiceAuthorizers
          .getOrElse(Set.empty)) subsetOf auth.authParties,
        failWith = FailedAuthorization.ExerciseMissingAuthorization(
          templateId = templateId,
          choiceId = choiceId,
          optLocation = optLocation,
          authorizingParties = auth.authParties,
          requiredParties = actingParties union choiceAuthorizers.getOrElse(Set.empty),
        ),
      )
  }

}

/** Dummy authorization checker that does not check anything. Should only be used for testing. */
private[lf] object NoopAuthorizationChecker extends AuthorizationChecker {
  override private[lf] def authorizeCreate(
      optLocation: Option[Location],
      templateId: TypeConId,
      signatories: Set[Party],
      maintainers: Option[Set[Party]],
  )(
      auth: Authorize
  ): List[FailedAuthorization] = Nil

  override private[lf] def authorizeFetch(
      optLocation: Option[Location],
      templateId: TypeConId,
      stakeholders: Set[Party],
  )(
      auth: Authorize
  ): List[FailedAuthorization] = Nil

  override private[lf] def authorizeLookupByKey(
      optLocation: Option[Location],
      templateId: TypeConId,
      maintainers: Set[Party],
  )(auth: Authorize): List[FailedAuthorization] = Nil

  override private[lf] def authorizeExercise(
      optLocation: Option[Location],
      templateId: TypeConId,
      choiceId: ChoiceName,
      actingParties: Set[Party],
      choiceAuthorizers: Option[Set[Party]],
  )(
      auth: Authorize
  ): List[FailedAuthorization] = Nil
}
