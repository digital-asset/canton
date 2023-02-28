// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.junit.jupiter;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.digitalasset.canton.environment.CommunityConsoleEnvironment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(CantonExtension.class)
public class CantonExtensionTest {

    @Test
    void defaultTopologyHasTwoParticipants(@Canton CommunityConsoleEnvironment console) {
        assertEquals(console.participants().all().size(), 2);
    }

}
