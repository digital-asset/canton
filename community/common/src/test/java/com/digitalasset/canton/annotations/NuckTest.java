// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.annotations;

import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@org.scalatest.TagAnnotation
@Inherited
@Retention(RUNTIME)
@Target({METHOD, TYPE})
// TODO(#30913): Remove this annotation and usages once the NUCK feature development is complete
// Annotation used for selecting NUCK-only tests to be run in CI for the period of the feature's
// development
public @interface NuckTest {}
