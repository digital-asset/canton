// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.refinements

import com.daml.ledger.api.v2.value.Identifier

object ApiTypes {

  final case class UpdateId(unwrap: String) extends AnyVal {
    override def toString: String = unwrap
  }

  final case class CommandId(unwrap: String) extends AnyVal {
    override def toString: String = unwrap
  }

  final case class WorkflowId(unwrap: String) extends AnyVal {
    override def toString: String = unwrap
  }

  final case class TemplateId(unwrap: Identifier) extends AnyVal {
    override def toString: String = unwrap.toString
  }

  final case class InterfaceId(unwrap: Identifier) extends AnyVal {
    override def toString: String = unwrap.toString
  }

  final case class UserId(unwrap: String) extends AnyVal {
    override def toString: String = unwrap
  }

  final case class ContractId(unwrap: String) extends AnyVal {
    override def toString: String = unwrap
  }

  final case class Choice(unwrap: String) extends AnyVal {
    override def toString: String = unwrap
  }

  final case class Party(unwrap: String) extends AnyVal {
    override def toString: String = unwrap
  }

}
