// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.error

import com.digitalasset.canton.protocol.messages.Verdict
import com.digitalasset.canton.protocol.{v0 as protocolProto}
import magnolify.scalacheck.auto.*
import org.scalacheck.{Arbitrary, Gen}

object GeneratorsError {
  import com.digitalasset.canton.version.GeneratorsVersion.*

  implicit val timeoutRejectArb: Arbitrary[MediatorError.Timeout.Reject] = Arbitrary(
    representativeProtocolVersionGen(Verdict).map(MediatorError.Timeout.Reject()(_))
  )

  implicit val invalidMessageRejectArb: Arbitrary[MediatorError.InvalidMessage.Reject] = Arbitrary(
    for {
      cause <- Gen.asciiStr
      code <- implicitly[
        Arbitrary[protocolProto.MediatorRejection.Code]
      ].arbitrary
      pv <- representativeProtocolVersionGen(Verdict)
    } yield MediatorError.InvalidMessage.Reject(cause, code)(pv)
  )

  private implicit val damlErrorCategoryArb: Arbitrary[com.daml.error.ErrorCategory] = genArbitrary

  implicit val genericErrorRejectArb: Arbitrary[MediatorError.GenericError] = Arbitrary(for {
    cause <- Gen.asciiStr
    id <- Gen.alphaNumStr
    category <- implicitly[Arbitrary[com.daml.error.ErrorCategory]].arbitrary
    code <- implicitly[
      Arbitrary[protocolProto.MediatorRejection.Code]
    ].arbitrary
    pv <- representativeProtocolVersionGen(Verdict)
  } yield MediatorError.GenericError(cause, id, category, code)(pv))

  implicit val malformedMessageRejectArb: Arbitrary[MediatorError.MalformedMessage.Reject] =
    Arbitrary(for {
      cause <- Gen.asciiStr
      code <- implicitly[Arbitrary[protocolProto.MediatorRejection.Code]].arbitrary
      pv <- representativeProtocolVersionGen(Verdict)
    } yield MediatorError.MalformedMessage.Reject(cause, code)(pv))
}
