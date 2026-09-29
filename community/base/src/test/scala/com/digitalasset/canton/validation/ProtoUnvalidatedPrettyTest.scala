// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.validation

import com.digitalasset.canton.config.ApiLoggingConfig
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.logging.audit.ApiRequestLogger
import com.digitalasset.canton.logging.pretty.Pretty.PrettyOps
import com.digitalasset.canton.logging.pretty.{CantonPrettyPrinter, Pretty}
import com.digitalasset.canton.protocol.v30
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

/** The API request logger renders a payload through a `protected` method; this exposes it. */
private class PayloadLogger(loggerFactory: NamedLoggerFactory)
    extends ApiRequestLogger(ApiLoggingConfig(messagePayloads = true), loggerFactory) {
  def render(message: Any): String = cutMessage(message)
}

class ProtoUnvalidatedPrettyTest extends AnyWordSpec with Matchers {

  // Content the rendering must never echo, as it fails the content check.
  private val secret = "secret"
  private val bell = "\u0007" // a control character, rejected by the content check
  private val max = ProtoUnvalidatedSeq.MaxPrettyElements

  private val printer = new CantonPrettyPrinter(maxStringLength = 250, maxMessageLines = 1000)
  private val loggerFactory = NamedLoggerFactory("test", getClass.getSimpleName)

  private def hosting(participant: String): v30.PartyToParticipant.HostingParticipant =
    v30.PartyToParticipant.HostingParticipant(
      participantUid = ProtoUnvalidatedString(participant),
      permission = v30.Enums.ParticipantPermission.PARTICIPANT_PERMISSION_SUBMISSION,
      onboarding = None,
    )

  private def partyToParticipant(party: String, participants: String*): v30.PartyToParticipant =
    v30.PartyToParticipant(
      party = ProtoUnvalidatedString(party),
      threshold = 1,
      participants = ProtoUnvalidatedSeq(participants.map(hosting)),
      partySigningKeys = None,
    )

  "ProtoUnvalidatedString" should {
    "print content that passes the check" in {
      ProtoUnvalidatedString("alice").toString shouldBe "alice"
    }

    "escape the whitespace controls the check accepts" in {
      ProtoUnvalidatedString("alice\r\nforged\tline").toString shouldBe "alice\\r\\nforged\\tline"
    }

    "name the violation instead of echoing rejected content" in {
      val rendered = ProtoUnvalidatedString(secret + bell).toString
      rendered should not include secret
      rendered shouldBe
        s"<invalid string: it contains a control character (U+0007) at index ${secret.length}>"
    }
  }

  "ProtoUnvalidatedSeq" should {
    "print its elements" in {
      ProtoUnvalidatedSeq(Seq("alice", "bob").map(ProtoUnvalidatedString(_))).toString shouldBe
        "Seq(alice, bob)"
    }

    "print a message element structurally, not as its proto text format" in {
      ProtoUnvalidatedSeq(Seq(hosting("PAR::one"), hosting("PAR::two"))).toString shouldBe
        "Seq(HostingParticipant(PAR::one, PARTICIPANT_PERMISSION_SUBMISSION, None), " +
        "HostingParticipant(PAR::two, PARTICIPANT_PERMISSION_SUBMISSION, None))"
    }

    "collapse a single element" in {
      ProtoUnvalidatedSeq(Seq(hosting("PAR::one"))).toString shouldBe
        "HostingParticipant(PAR::one, PARTICIPANT_PERMISSION_SUBMISSION, None)"
    }

    "check the content of a string element" in {
      val rendered =
        ProtoUnvalidatedSeq(Seq("alice", secret + bell).map(ProtoUnvalidatedString(_))).toString
      rendered should not include secret
      rendered shouldBe
        s"Seq(alice, <invalid string: it contains a control character (U+0007) at index ${secret.length}>)"
    }

    "cut off at the element cap" in {
      val seq = ProtoUnvalidatedSeq((1 to max + 5).map(i => ProtoUnvalidatedString(s"p$i")))
      // The default printer cuts the output off after 100 lines, short of the marker.
      val rendered = seq.toPrettyString(Pretty.DefaultPprinter.copy(defaultHeight = 1000))
      rendered should include("... more")
      rendered should not include s"p${max + 1}"
    }
  }

  "the adhoc printer" should {
    "print a topology mapping without naming the wrappers" in {
      val rendered =
        printer.printAdHoc(partyToParticipant("alice::default", "PAR::one", "PAR::two"))
      rendered should not include "ProtoUnvalidated"
      rendered shouldBe
        "PartyToParticipant(alice::default, 1, Seq(" +
        "HostingParticipant(PAR::one, PARTICIPANT_PERMISSION_SUBMISSION, None), " +
        "HostingParticipant(PAR::two, PARTICIPANT_PERMISSION_SUBMISSION, None)), None)"
    }

    "name the violation instead of echoing rejected content" in {
      val rendered = printer.printAdHoc(partyToParticipant(secret + bell, secret + bell))
      rendered should not include secret
      rendered should include("control character")
    }

    "check the content of a repeated string field" in {
      val parties = v30.StoredParties(
        ProtoUnvalidatedSeq(Seq("alice", secret + bell).map(ProtoUnvalidatedString(_)))
      )
      val rendered = printer.printAdHoc(parties)
      rendered should not include secret
      rendered shouldBe
        s"StoredParties(Seq(alice, <invalid string: it contains a control character (U+0007) at index ${secret.length}>))"
    }

    "reach the payload of an API request log" in {
      val parties = v30.StoredParties(
        ProtoUnvalidatedSeq(Seq("alice", secret + bell).map(ProtoUnvalidatedString(_)))
      )
      val rendered = new PayloadLogger(loggerFactory).render(parties)
      rendered should not include secret
      rendered shouldBe
        s"StoredParties(Seq(alice, <invalid string: it contains a control character (U+0007) at index ${secret.length}>))"
    }

    "not let a payload forge a log line" in {
      val parties = v30.StoredParties(
        ProtoUnvalidatedSeq(Seq(ProtoUnvalidatedString("alice\nRequest forged by nobody")))
      )
      printer.printAdHoc(parties) should not include "\n"
    }

    "limit a long string" in {
      val limiting = new CantonPrettyPrinter(maxStringLength = 5, maxMessageLines = 1000)
      val rendered = limiting.printAdHoc(partyToParticipant("a" * 100))
      rendered should include("aaaaa...")
      rendered should not include "a" * 6
    }

    "limit a long string inside a repeated field" in {
      val limiting = new CantonPrettyPrinter(maxStringLength = 5, maxMessageLines = 1000)
      val parties =
        v30.StoredParties(ProtoUnvalidatedSeq(Seq(ProtoUnvalidatedString("a" * 100))))
      val rendered = limiting.printAdHoc(parties)
      rendered should include("aaaaa...")
      rendered should not include "a" * 6
    }

    "cut a repeated field off at the element cap" in {
      val rendered =
        printer.printAdHoc(partyToParticipant("alice::default", (1 to max + 5).map(i => s"p$i")*))
      rendered should include("... more")
      rendered should not include s"p${max + 1}"
    }
  }
}
