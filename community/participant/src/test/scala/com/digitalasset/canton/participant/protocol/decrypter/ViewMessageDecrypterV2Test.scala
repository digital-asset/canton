// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.decrypter

import com.digitalasset.canton.BaseTestWordSpec
import com.digitalasset.canton.version.ProtocolVersion

class ViewMessageDecrypterV2Test extends BaseTestWordSpec with ViewMessageDecrypterTest {
  // TODO(#32393): Enable after implementing the new decryption logic.
  "A ViewMessageDecrypter version 2 (using ciphertext ID)" ignore {
    if (testedProtocolVersion >= ProtocolVersion.transparency)
      behave like viewMessageDecrypterTest()
  }

}
