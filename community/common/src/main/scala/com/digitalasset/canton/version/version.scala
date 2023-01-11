// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

package object version {
  type VersionedMessage[+M] = VersionedMessageImpl.Instance.VersionedMessage[M]
}
