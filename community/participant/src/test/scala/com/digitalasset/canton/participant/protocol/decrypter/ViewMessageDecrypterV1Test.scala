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
        decrypter.decryptViews(allEnvelopes, snapshot, defaultSynchronizerLimits).value,
        _.getMessage shouldBe s"View ${encryptedViewMessage(child).viewHashes.head1} has different encryption keys associated with it. " +
          s"(previous: ${randomness(child)}, new: $dummyRandomness)",
      )
      .futureValueUS
  }

  "A ViewMessageDecrypter version 1 (unique view hashes)" must {
    if (testedProtocolVersion < ProtocolVersion.transparency) {
      behave like viewMessageDecrypterTest()
    }
  }

}
