// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.decrypter

import com.digitalasset.canton.BaseTestWordSpec
import com.digitalasset.canton.crypto.SecureRandomness
import com.digitalasset.canton.data.FullTransactionViewTree
import com.digitalasset.canton.version.ProtocolVersion

class ViewMessageDecrypterV2Test extends BaseTestWordSpec with ViewMessageDecrypterTest {

  override protected def reportRandomnessMismatch(
      env: Env,
      dummyRandomness: SecureRandomness,
  ): Unit = {
    import env.*

    val decryptedViews =
      decrypter.decryptViews(allEnvelopes, snapshot).futureValueUS.valueOrFail("failed transaction")

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

      "successfully decrypt even if different encrypted view messages contain the same view with different randomnesses" in {
        // Unlike V1, we now preserve duplicate encrypted view messages.
        var ftt = Seq.empty[FullTransactionViewTree]
        val env = new Env(
          // We override the interceptFullTree method to capture the full tree and return a duplicate
          // of the same view (child view). Within `Env`, we use two different randomness values (i.e. keys)
          // for each index in the full view tree list.
          interceptFullTree = trees => {
            ftt = Seq(trees(1), trees(1))
            Seq(trees(1), trees(1))
          },
          // Make sure the two views have no children.
          interceptSubviewKeyRandomness = _ => Seq(Seq.empty, Seq.empty),
        )
        import env.*

        // We are able to decrypt both views because, although they represent the same view, they were
        // encrypted with different randomness. While the view hashes are identical, the ciphertext IDs
        // are different, allowing the two encrypted views to be uniquely identified and decrypted.
        // Since reconstruction of the full tree is done using the unique ciphertext IDs, we can successfully
        // reconstruct the full tree and discard the duplicate view.
        val decryptedViews = decrypter
          .decryptViews(allEnvelopes, snapshot)
          .futureValueUS
          .valueOrFail("failed transaction")

        decryptedViews.views.map(_.view.unwrap.tree) should contain theSameElementsAs ftt.map(
          _.tree
        )
      }
    }
  }

}
