// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.p2p

import cats.syntax.either.*
import com.digitalasset.canton.Generators.boundedListGen
import com.digitalasset.canton.protocol.BftOrdererLimitsView
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.ProtocolVersionedMemoizedEvidence
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.ProtoMutations.{
  SignedPayloads,
  singleFieldMutants,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.integration.canton.SupportedVersions
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.AvailabilityModule
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.IssConsensusModule
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.{
  MessageFrom,
  SignedMessage,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Consensus.RetransmissionsMessage.{
  RetransmissionRequest,
  RetransmissionResponse,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.{
  BftSequencerBaseTest,
  GeneratorsBftOrdering,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.{v30, v31}
import com.digitalasset.canton.version.{ProtocolVersion, ProtocolVersionValidation}
import org.scalacheck.rng.Seed
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.wordspec.AnyWordSpec
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

/** Checks that the parsers of the messages received from other BFT nodes are total: whatever a peer
  * sends, they must return a parsing error and never throw, because an exception thrown while
  * handling a message is fatal for a module.
  *
  * The hostile inputs are all the single-field mutations (see [[ProtoMutations]]) of a few
  * well-formed messages of each kind.
  */
final class BftNetworkParsersTotalityTest extends AnyWordSpec with BftSequencerBaseTest {

  private val generators = new GeneratorsBftOrdering(synchronizerProtocolVersion)
  import generators.*

  private val limits = BftOrdererLimitsView.default
  private val samplesPerMessageKind = 3

  // The limits on the size of collections are only enforced by some validations: cover both paths.
  private val validations = Seq(
    ProtocolVersionValidation.PV(synchronizerProtocolVersion),
    ProtocolVersionValidation.AlwaysValidation,
  )

  private val consensusPayloads = SignedPayloads(v30.ConsensusMessage, v30.ConsensusMessage)

  "The parsers of BFT network messages" should {
    "not throw on hostile availability messages" in {
      Seq(
        signed(availabilityRemoteDisseminationRemoteBatchArb),
        signed(availabilityRemoteDisseminationRemoteBatchAcknowledgedArb),
        signed(availabilityRemoteOutputFetchFetchRemoteBatchDataArb),
        signed(availabilityRemoteOutputFetchRemoteBatchDataFetchedArb),
      ).foreach(
        assertTotal(_, SignedPayloads(v30.AvailabilityMessage, v30.ConsensusMessage))(validation =>
          AvailabilityModule.parseNetworkMessage(validation, limits, _)
        )
      )
    }

    "not throw on hostile consensus messages" in {
      Seq(
        signed(prePrepareArb),
        signed(prepareArb),
        signed(commitArb(Some(actualSender))),
        signed(viewChangeArb),
        signed(newViewArb),
      ).foreach(
        assertTotal(_, consensusPayloads)(validation =>
          IssConsensusModule.parseNetworkMessage(validation, limits, _, actualSender)
        )
      )
    }

    "not throw on hostile retransmission messages" in {
      val parse = (validation: ProtocolVersionValidation) =>
        IssConsensusModule.parseRetransmissionMessage(validation, limits, actualSender, _)

      assertTotal(
        signedMessage(epochStatusArb).arbitrary.map(RetransmissionRequest(_).toProto),
        SignedPayloads(v30.EpochStatus, v30.ConsensusMessage),
      )(parse)
      assertTotal(
        boundedListGen(commitCertificateArb.arbitrary)
          .map(RetransmissionResponse(actualSender, _).toProto),
        consensusPayloads,
      )(parse)
    }

    "not throw on hostile state transfer messages" in {
      // Same choice of wire format as in `P2PNetworkInModule`
      if (synchronizerProtocolVersion >= ProtocolVersion.v37)
        Seq(
          signed(stateTransferMessageBlockTransferRequestArb),
          signed(stateTransferMessageBlockTransferResponse31Arb),
        ).foreach(
          assertTotal(_, SignedPayloads(v31.StateTransferMessage, v30.ConsensusMessage))(
            validation =>
              SignedMessage.fromProto(
                validation,
                v31.StateTransferMessage,
                SupportedVersions.ProtoData31,
              )(IssConsensusModule.parseStateTransferMessage31(validation, limits, actualSender, _))
          )
        )
      else
        Seq(
          signed(stateTransferMessageBlockTransferRequestArb),
          signed(stateTransferMessageBlockTransferResponse30Arb),
        ).foreach(
          assertTotal(_, SignedPayloads(v30.StateTransferMessage, v30.ConsensusMessage))(
            validation =>
              SignedMessage.fromProto(
                validation,
                v30.StateTransferMessage,
                SupportedVersions.ProtoData30,
              )(IssConsensusModule.parseStateTransferMessage(validation, limits, actualSender, _))
          )
        )
    }
  }

  private def signed[M <: ProtocolVersionedMemoizedEvidence & MessageFrom](
      messages: Arbitrary[M]
  ): Gen[v30.SignedMessage] =
    signedMessage(messages).arbitrary.map(_.toProtoV1)

  /** Fails if `parse` rejects one of the generated messages (the hostile variants of a message that
    * is not accepted in the first place would say little) or throws on one of their mutants.
    */
  private def assertTotal[P <: GeneratedMessage: GeneratedMessageCompanion](
      messages: Gen[P],
      signedPayloads: SignedPayloads,
  )(parse: ProtocolVersionValidation => P => ParsingResult[?]): Unit =
    for {
      // A fixed seed, so that a failure can be reproduced
      message <- Gen
        .listOfN(samplesPerMessageKind, messages)
        .pureApply(Gen.Parameters.default, Seed(0L))
      validation <- validations
    } {
      parse(validation)(message) shouldBe a[Right[?, ?]]

      singleFieldMutants(message, signedPayloads)
        .flatMap(mutant =>
          Either.catchNonFatal(parse(validation)(mutant)).swap.toOption.map(mutant -> _)
        )
        .headOption
        .foreach { case (mutant, exception) =>
          fail(s"The parser threw on the hostile message:\n${mutant.toProtoString}", exception)
        }
    }
}
