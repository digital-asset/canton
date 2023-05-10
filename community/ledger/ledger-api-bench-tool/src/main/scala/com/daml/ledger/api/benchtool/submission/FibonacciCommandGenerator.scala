// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission

import com.daml.ledger.api.benchtool.config.WorkflowConfig.FibonacciSubmissionConfig
import com.daml.ledger.api.benchtool.infrastructure.TestDars
import com.daml.ledger.api.v1.commands.{Command, CreateAndExerciseCommand}
import com.daml.ledger.api.v1.value.{Record, RecordField, Value}
import com.daml.ledger.client.binding.Primitive
import com.daml.lf.data.Ref

import scala.util.{Success, Try}

final class FibonacciCommandGenerator(
    config: FibonacciSubmissionConfig,
    signatory: Primitive.Party,
    names: Names,
) extends CommandGenerator {

  private val packageId: Ref.PackageId = TestDars.benchtoolDarPackageId

  override def nextApplicationId(): String = names.benchtoolApplicationId

  override def nextExtraCommandSubmitters(): List[Primitive.Party] = List.empty

  def next(): Try[Seq[Command]] = {
    val createArguments: Option[Record] = Some(
      Record(
        None,
        Seq(
          RecordField(
            label = "owner",
            value = Some(Value(Value.Sum.Party(signatory.toString))),
          )
        ),
      )
    )
    Success(
      Seq(
        Command(
          Command.Command.CreateAndExercise(
            CreateAndExerciseCommand(
              templateId = Some(FooTemplateDescriptor.inefficientFibonacciTemplateId(packageId)),
              createArguments = createArguments,
              choice = "InefficientFibonacci_Compute",
              choiceArgument = Some(
                Value(
                  Value.Sum.Record(
                    Record(
                      None,
                      Seq(
                        RecordField(
                          label = "value",
                          value = Some(Value(Value.Sum.Int64(config.value.toLong))),
                        )
                      ),
                    )
                  )
                )
              ),
            )
          )
        )
      )
    )
  }

}
