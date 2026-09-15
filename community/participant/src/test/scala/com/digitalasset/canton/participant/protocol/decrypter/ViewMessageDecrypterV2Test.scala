// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.decrypter

import com.digitalasset.canton.BaseTestWordSpec
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceAlarm
import com.digitalasset.canton.sequencing.protocol.{OpenEnvelope, Recipients}
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.nonempty.NonEmptyUtil

class ViewMessageDecrypterV2Test extends BaseTestWordSpec with ViewMessageDecrypterTest {

  "A ViewMessageDecrypter version 2 (using ciphertext ID)" must {
    if (testedProtocolVersion >= ProtocolVersion.transparency) {
      behave like viewMessageDecrypterTest()

      // In V1, we used the listed view hashes to identify the view to decrypt and assumed that each
      // view hash was unique. In V2, we use the ciphertext ID to identify the view to decrypt,
      // which can be computed from the ciphertext itself. This allows us to successfully decrypt
      // even if different view messages list the same or incorrect view hashes.
      "successfully decrypt even if different view messages list the same view hash" in {
        val env = new Env(
          interceptEncryptedViewMessages = { encryptedViewMessages =>
            val sharedViewHash = encryptedViewMessages.head.viewHashes.head1
            // Force all encrypted view messages to use the same view hash
            // to verify that decryption does not rely on view-hash uniqueness.
            encryptedViewMessages.map {
              _.copy(viewHashes = NonEmptyUtil.fromUnsafe(Seq(sharedViewHash)))
            }
          }
        )

        val decryptedViews = env.decrypter
          .decryptViews(env.allEnvelopes, env.snapshot, env.defaultSynchronizerLimits)
          .futureValueUS
          .value

        env.checkDecryptedViews(decryptedViews)
      }

      "successfully decrypt if the same envelope is duplicated" in {
        val env = new Env()
        import env.*

        val decryptedViews = loggerFactory.assertLoggedWarningsAndErrorsSeq(
          decrypter
            .decryptViews(
              onlyChildEnvelopes ++ onlyChildEnvelopes,
              snapshot,
              defaultSynchronizerLimits,
            )
            .futureValueUS
            .value,
          entries =>
            LogEntry.assertLogSeq(
              Seq(
                (
                  entry => {
                    entry.shouldBeCantonErrorCode(SyncServiceAlarm)
                    entry.warningMessage should include("Discarding duplicate envelope")
                  },
                  "Message should contain a warning about discarding duplicate envelope",
                )
              )
            )(entries),
        )
        env.checkDecryptedViews(decryptedViews, nbrViews = 1)
      }

      "fail if different envelopes share the same ciphertext" in {
        val env = new Env()
        import env.*

        val originalChildMessage = encryptedViewMessage(child)
        // Keep the same encrypted payload (same ciphertext ID), but change the envelope metadata.
        val differentRecipients = Recipients.cc(ParticipantId("another-participant"))
        val modifiedEnvelope =
          OpenEnvelope(originalChildMessage, differentRecipients)(testedProtocolVersion)

        loggerFactory
          .assertInternalError[IllegalArgumentException](
            decrypter
              .decryptViews(
                onlyChildEnvelopes ++ Seq(modifiedEnvelope),
                snapshot,
                defaultSynchronizerLimits,
              )
              .futureValueUS,
            _.getMessage should include("Duplicate envelope with the same ciphertextID"),
          )
      }
    }
  }
}
