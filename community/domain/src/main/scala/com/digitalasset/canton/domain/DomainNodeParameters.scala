// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain

import com.digitalasset.canton.environment.{
  CantonNodeParameters,
  HasGeneralCantonNodeParameters,
  HasProtocolCantonNodeParameters,
}

/** Parameters used by domain nodes
  *
  * We "merge" parameters that are valid for all nodes (i.e. canton.parameters) and
  * the node specific parameters together into this class.
  * @param general the node parameters required by the base class
  * @param protocol related parameters which are configured differently (not all nodes have the same set of parameters)
  */
case class DomainNodeParameters(
    general: CantonNodeParameters.General,
    protocol: CantonNodeParameters.Protocol,
) extends CantonNodeParameters
    with HasGeneralCantonNodeParameters
    with HasProtocolCantonNodeParameters