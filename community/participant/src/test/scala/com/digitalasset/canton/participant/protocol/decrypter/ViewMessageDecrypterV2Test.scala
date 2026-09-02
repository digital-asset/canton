// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.decrypter

import com.digitalasset.canton.BaseTestWordSpec
import com.digitalasset.canton.crypto.SecureRandomness
import com.digitalasset.canton.data.ViewType.TransactionViewType
import com.digitalasset.canton.protocol.messages.EncryptedMultipleViewsMessage
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.nonempty.NonEmptyUtil

class ViewMessageDecrypterV2Test extends BaseTestWordSpec with ViewMessageDecrypterTest {

  override protected def reportRandomnessMismatch(
      env: Env,
      dummyRandomness: SecureRandomness,
  ): Unit = {
    import env.*

    val decryptedViews =
      decrypter
        .decryptViews(allEnvelopes, snapshot, defaultSynchronizerLimits)
        .futureValueUS
        .valueOrFail("failed transaction")

    // We are able to decrypt the parent view, but the child view referenced by the parent cannot be decrypted because
    // the randomness provided by the parent fails to decrypt the child view. Since the child view cannot be decrypted,
    // the parent view is considered invalid as well.
    val errors = decryptedViews.decryptionErrors.map(_.show)

    // The child view decryption fails because the randomness listed in the parent's `subviewReferenceAndKey`
    // used to decrypt the child view is incorrect.
    errors should contain(
      "SymmetricDecryptError(FailedToDecrypt(\"javax.crypto.AEADBadTagException: mac check in GCM failed\"))"
    )

    // The child view is still successfully decrypted because it is also sent as a top-level view in another envelope,
    // allowing us to decrypt the randomness directly using the recipient's private key.
    decryptedViews.views.loneElement.view.unwrap should be(env.lightTree(env.child))

    // The parent view is considered invalid because the child view cannot be decrypted.
    errors.exists(
      _.matches(
        ".*Failed to decrypt parent view ViewHash\\(SHA-256:.*\\) because a subview failed to decrypt.*"
      )
    ) shouldBe true
  }

  "A ViewMessageDecrypter version 2 (using ciphertext ID)" must {
    if (testedProtocolVersion >= ProtocolVersion.transparency) {
      behave like viewMessageDecrypterTest()

      // Multiple encrypted view messages can theoretically share the same ciphertext ID
      // but have different encryption randomnesses. The decrypter must attempt each message.
      "handle multiple encrypted view messages with the same ciphertext ID" in {
        val env = new Env(
          interceptEncryptedViewMessages = { encryptedViewMessages =>
            val parentView = encryptedViewMessages(0)
              .asInstanceOf[EncryptedMultipleViewsMessage[TransactionViewType.type]]
            val childView = encryptedViewMessages(1)
              .asInstanceOf[EncryptedMultipleViewsMessage[TransactionViewType.type]]
            encryptedViewMessages :+
              parentView.copy(
                viewEncryptionKeyRandomness = childView.viewEncryptionKeyRandomness
              )
          }
        )

        val decryptedViews = env.decrypter
          .decryptViews(env.allEnvelopes, env.snapshot, env.defaultSynchronizerLimits)
          .futureValueUS
          .value

        decryptedViews.decryptionErrors.loneElement.show should include(
          "SymmetricDecryptError(FailedToDecrypt(\"javax.crypto.AEADBadTagException: mac check in GCM failed\"))"
        )
        env.checkDecryptedViews(decryptedViews.copy(decryptionErrors = Seq.empty))
      }

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
              _.asInstanceOf[EncryptedMultipleViewsMessage[TransactionViewType.type]]
                .copy(viewHashes = NonEmptyUtil.fromUnsafe(Seq(sharedViewHash)))
            }
          }
        )

        val decryptedViews = env.decrypter
          .decryptViews(env.allEnvelopes, env.snapshot, env.defaultSynchronizerLimits)
          .futureValueUS
          .value

        env.checkDecryptedViews(decryptedViews)
      }
    }
  }
}
