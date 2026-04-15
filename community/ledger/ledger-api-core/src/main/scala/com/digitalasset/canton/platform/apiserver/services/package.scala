// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver

import com.daml.ledger.api.v2.commands.Commands
import com.daml.ledger.api.v2.reassignment_commands.ReassignmentCommands
import com.daml.tracing.SpanAttribute
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}

package object services {

  def getAnnotatedCommandTraceContext(commands: Option[Commands]): TraceContext = {
    val traceContext = TraceContextGrpc.fromGrpcContext
    commands.foreach { commands =>
      traceContext.setSpanAttributes(
        Seq(
          SpanAttribute.UserId -> commands.userId,
          SpanAttribute.CommandId -> commands.commandId,
          SpanAttribute.Submitter -> commands.actAs.headOption.getOrElse(""),
          SpanAttribute.WorkflowId -> commands.workflowId,
        )
      )
    }
    traceContext
  }

  def getAnnotatedReassignmentCommandTraceContext(
      commands: Option[ReassignmentCommands]
  ): TraceContext = {
    val traceContext = TraceContextGrpc.fromGrpcContext
    commands.foreach { commands =>
      traceContext.setSpanAttributes(
        Seq(
          SpanAttribute.UserId -> commands.userId,
          SpanAttribute.CommandId -> commands.commandId,
          SpanAttribute.Submitter -> commands.submitter,
          SpanAttribute.WorkflowId -> commands.workflowId,
        )
      )
    }
    traceContext
  }

  def getPrepareRequestTraceContext(
      userId: String,
      commandId: String,
      actAs: Seq[String],
  ): TraceContext = {
    val traceContext = TraceContextGrpc.fromGrpcContext
    traceContext.setSpanAttributes(
      Seq(
        SpanAttribute.UserId -> userId,
        SpanAttribute.CommandId -> commandId,
        SpanAttribute.Submitter -> actAs.headOption.getOrElse(""),
      )
    )
    traceContext
  }

  def getExecuteRequestTraceContext(
      userId: String,
      commandId: Option[String],
      actAs: Seq[String],
  ): TraceContext = {
    val traceContext = TraceContextGrpc.fromGrpcContext
    traceContext.setSpanAttributes(
      Seq(
        SpanAttribute.UserId -> userId,
        SpanAttribute.CommandId -> commandId.getOrElse(""),
        SpanAttribute.Submitter -> actAs.headOption.getOrElse(""),
      )
    )
    traceContext
  }

}
