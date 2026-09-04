// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.annotations;

import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Annotation for declaring that a test suite runs only with the given protocol version or an
 * earlier one. All tests of an annotated suite are reported as ignored when {@code
 * BaseTest.testedProtcolVersion} is higher.
 *
 * <p>A suite using the annotation needs to mix in {@code ProtocolVersionSuiteChecks}. The base
 * types {@code BaseTestWordSpec} and {@code BaseIntegrationTest} do so already. Being
 * {@code @Inherited}, the annotation has no effect when it is declared on a trait.
 */
@Inherited
@Retention(RUNTIME)
@Target(TYPE)
public @interface MaxProtocolVersion {
  String value();
}
