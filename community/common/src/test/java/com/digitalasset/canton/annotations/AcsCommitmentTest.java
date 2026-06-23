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
// TODO(#33536): Remove this annotation and usages once the ACS commitment redesign development is
// complete
// Annotation used for selecting tests around ACS commitments to be run in CI for the period of the
// feature's development
public @interface AcsCommitmentTest {}
