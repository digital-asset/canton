// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.annotations;

import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Annotation for tagging test suites that require access to real AWS or GCP KMS credentials. These
 * tests will only run in dedicated KMS test jobs where the necessary credentials are available.
 * They are excluded from forked PR CI runs.
 */
@org.scalatest.TagAnnotation
@Inherited
@Retention(RUNTIME)
@Target({METHOD, TYPE})
public @interface RequiresExternalKms {}
