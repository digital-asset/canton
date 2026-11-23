// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.StateServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.time.Instant;
import java.util.Objects;

public final class HostingParticipant {
  private final @NonNull String participantId;
  private final @NonNull String synchronizerId;
  private final @NonNull Instant recordTime;
  private final @NonNull Long offset;
  private final @NonNull ParticipantPermission participantPermission;
  private final @NonNull HostingState hostingState;

  public HostingParticipant(
      @NonNull String participantId,
      @NonNull String synchronizerId,
      @NonNull Instant recordTime,
      @NonNull Long offset,
      @NonNull ParticipantPermission participantPermission,
      @NonNull HostingState hostingState) {
    this.participantId = participantId;
    this.synchronizerId = synchronizerId;
    this.recordTime = recordTime;
    this.offset = offset;
    this.participantPermission = participantPermission;
    this.hostingState = hostingState;
  }

  @NonNull
  public String getParticipantId() {
    return participantId;
  }

  @NonNull
  public String getSynchronizerId() {
    return synchronizerId;
  }

  @NonNull
  public Instant getRecordTime() {
    return recordTime;
  }

  @NonNull
  public Long getOffset() {
    return offset;
  }

  @NonNull
  public ParticipantPermission getParticipantPermission() {
    return participantPermission;
  }

  @NonNull
  public HostingState getHostingState() {
    return hostingState;
  }

  public static HostingParticipant fromProto(StateServiceOuterClass.HostingParticipant proto) {
    return new HostingParticipant(
        proto.getParticipantId(),
        proto.getSynchronizerId(),
        Utils.instantFromProto(proto.getRecordTime()),
        proto.getOffset(),
        ParticipantPermission.fromProto(proto.getParticipantPermission()),
        HostingState.fromProto(proto.getHostingState()));
  }

  public StateServiceOuterClass.HostingParticipant toProto() {
    return StateServiceOuterClass.HostingParticipant.newBuilder()
        .setParticipantId(participantId)
        .setSynchronizerId(synchronizerId)
        .setRecordTime(Utils.instantToProto(recordTime))
        .setOffset(offset)
        .setParticipantPermission(participantPermission.toProto())
        .setHostingState(hostingState.toProto())
        .build();
  }

  @Override
  public String toString() {
    return "HostingParticipant{"
        + "participantId='"
        + participantId
        + '\''
        + ", synchronizerId='"
        + synchronizerId
        + '\''
        + ", recordTime="
        + recordTime
        + ", offset="
        + offset
        + ", participantPermission="
        + participantPermission
        + ", hostingState="
        + hostingState
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    HostingParticipant that = (HostingParticipant) o;
    return Objects.equals(participantId, that.participantId)
        && Objects.equals(synchronizerId, that.synchronizerId)
        && Objects.equals(recordTime, that.recordTime)
        && Objects.equals(offset, that.offset)
        && Objects.equals(participantPermission, that.participantPermission)
        && hostingState == that.hostingState;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        participantId, synchronizerId, recordTime, offset, participantPermission, hostingState);
  }
}
