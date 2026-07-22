// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.decrypter

import com.digitalasset.canton.BaseTestWordSpec

class ViewMessageDecrypterV1Test extends BaseTestWordSpec with ViewMessageDecrypterTest {

  "A ViewMessageDecrypter version 1 (unique view hashes)" must {
    // if (testedProtocolVersion < ProtocolVersion.transparency) {
    behave like viewMessageDecrypterTest()
    // }
  }

}
