// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.TransactionOuterClass;
import java.util.List;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * This interface represents events in {@link TransactionTree}s.
 *
 * @see CreatedEvent
 * @see ExercisedEvent
 * @see TransactionTree; Interface will be removed in 3.4.0
 */
// TODO(#23504) remove
@Deprecated
public interface TreeEvent {

  @NonNull
  List<@NonNull String> getWitnessParties();

  @NonNull
  Long getOffset();

  @NonNull
  Integer getNodeId();

  @NonNull
  Identifier getTemplateId();

  @NonNull
  String getPackageName();

  @NonNull
  String getContractId();

  default TransactionOuterClass.TreeEvent toProtoTreeEvent() {
    TransactionOuterClass.TreeEvent.Builder eventBuilder =
        TransactionOuterClass.TreeEvent.newBuilder();
    if (this instanceof CreatedEvent) {
      CreatedEvent event = (CreatedEvent) this;
      eventBuilder.setCreated(event.toProto());
    } else if (this instanceof ExercisedEvent) {
      ExercisedEvent event = (ExercisedEvent) this;
      eventBuilder.setExercised(event.toProto());
    } else {
      throw new RuntimeException(
          "this should be CreatedEvent or ExercisedEvent, found " + this.toString());
    }
    return eventBuilder.build();
  }

  static TreeEvent fromProtoTreeEvent(TransactionOuterClass.TreeEvent event) {
    if (event.hasCreated()) {
      return CreatedEvent.fromProto(event.getCreated());
    } else if (event.hasExercised()) {
      return ExercisedEvent.fromProto(event.getExercised());
    } else {
      throw new UnsupportedEventTypeException(event.toString());
    }
  }
}
