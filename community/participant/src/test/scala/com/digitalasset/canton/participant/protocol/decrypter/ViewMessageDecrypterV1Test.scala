// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.decrypter

import com.digitalasset.canton.BaseTestWordSpec
import com.digitalasset.canton.crypto.SecureRandomness
import com.digitalasset.canton.version.ProtocolVersion

class ViewMessageDecrypterV1Test extends BaseTestWordSpec with ViewMessageDecrypterTest {

  override protected def reportRandomnessMismatch(
      env: Env,
      dummyRandomness: SecureRandomness,
  ): Unit = {
    import env.*
    loggerFactory
      .assertInternalErrorAsyncUS[IllegalArgumentException](
        decrypter.decryptViews(allEnvelopes, snapshot).value,
        _.getMessage shouldBe s"View ${encryptedViewMessage(child).viewHashes.head1} has different encryption keys associated with it. " +
          s"(previous: ${randomness(child)}, new: $dummyRandomness)",
      )
      .futureValueUS
  }

  "A ViewMessageDecrypter version 1 (unique view hashes)" must {
    if (testedProtocolVersion < ProtocolVersion.transparency) {
      behave like viewMessageDecrypterTest()

      "fail if different encrypted view messages contain the same view with different randomnesses" in {
        // Note: it would be desirable to keep the messages instead.

        val env = new Env(
          interceptFullTree = trees => Seq(trees(1), trees(1)),
          // Make sure the two views have no children.
          interceptSubviewKeyRandomness = _ => Seq(Seq.empty, Seq.empty),
        )
        import env.*

        loggerFactory.assertInternalErrorAsyncUS[IllegalArgumentException](
          decrypter.decryptViews(allEnvelopes, snapshot).value,
          { x =>
            val randomnesses = (randomness(parent), randomness(child))
            Seq(randomnesses, randomnesses.swap).map { case (r1, r2) =>
              s"View ${encryptedViewMessage(child).viewHashes.head1} has different encryption keys associated with it. " +
                s"(previous: $r1, new: $r2)"
            } should contain(x.getMessage)
          },
        )
      }.futureValueUS
    }
  }

}
