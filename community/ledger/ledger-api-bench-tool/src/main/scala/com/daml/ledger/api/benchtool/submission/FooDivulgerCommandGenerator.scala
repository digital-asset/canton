// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission

import com.daml.ledger.api.benchtool.infrastructure.TestDars
import com.daml.ledger.api.v1.commands.{Command, CreateCommand}
import com.daml.ledger.api.v1.value.{Record, RecordField, Value}
import com.daml.ledger.client.binding.Primitive
import com.daml.lf.data.Ref

object FooDivulgerCommandGenerator {

  private val packageId: Ref.PackageId = TestDars.benchtoolDarPackageId

  /** Builds a create Divulger command for each non-empty subset of divulgees
    * such that the created Divulger contract can be used to divulge (by immediate divulgence) Foo1, Foo2 or Foo3 contracts
    * to the corresponding subset of divulgees.
    *
    * @param allDivulgees - Small number of divulgees. At most 5.
    * @return A tuple of:
    *          - a sequence of create Divulger commands,
    *          - a map from sets of divulgees (all non-empty subsets of all divulgees) to corresponding contract keys,
    */
  def makeCreateDivulgerCommands(
      divulgingParty: Primitive.Party,
      allDivulgees: List[Primitive.Party],
  ): (List[Command], Map[Set[Primitive.Party], Value]) = {
    require(
      allDivulgees.size <= 5,
      s"Number of divulgee parties must be at most 5, was: ${allDivulgees.size}.",
    )
    def allNonEmptySubsets(divulgees: List[Primitive.Party]): List[List[Primitive.Party]] = {
      def iter(remaining: List[Primitive.Party]): List[List[Primitive.Party]] = {
        remaining match {
          case Nil => List(List.empty)
          case head :: tail =>
            val sub: List[List[Primitive.Party]] = iter(tail)
            val sub2: List[List[Primitive.Party]] = sub.map(xs => xs.prepended(head))
            sub ::: sub2
        }
      }
      import scalaz.syntax.tag.*
      iter(divulgees)
        .collect {
          case parties if parties.nonEmpty => parties.sortBy(_.unwrap)
        }
    }

    def createDivulgerFor(divulgees: List[Primitive.Party]): (Command, Value) = {
      val keyId = "divulger-" + FooCommandGenerator.nextContractNumber.getAndIncrement()
      val createDivulgerCmd: Command = makeCreateDivulgerCommand(
        divulgees = divulgees,
        divulger = divulgingParty,
        keyId = keyId,
      )
      val divulgerKey: Value = FooCommandGenerator.makeContractKeyValue(divulgingParty, keyId)
      (createDivulgerCmd, divulgerKey)
    }

    val allSubsets = allNonEmptySubsets(allDivulgees)
    val (commands, keys, divulgeeSets) = allSubsets.map { divulgees: List[Primitive.Party] =>
      val (cmd, key) = createDivulgerFor(divulgees)
      (cmd, key, divulgees.toSet)
    }.unzip3
    val divulgeesToContractKeysMap = divulgeeSets.zip(keys).toMap
    (commands, divulgeesToContractKeysMap)
  }

  private def makeCreateDivulgerCommand(
      keyId: String,
      divulger: Primitive.Party,
      divulgees: List[Primitive.Party],
  ) = {
    val createArguments: Option[Record] = Some(
      Record(
        None,
        Seq(
          RecordField(
            label = "divulger",
            value = Some(Value(Value.Sum.Party(divulger.toString))),
          ),
          RecordField(
            label = "divulgees",
            value = Some(
              Value(
                Value.Sum.List(
                  com.daml.ledger.api.v1.value.List(
                    divulgees.map(d => Value(Value.Sum.Party(d.toString)))
                  )
                )
              )
            ),
          ),
          RecordField(
            label = "keyId",
            value = Some(Value(Value.Sum.Text(keyId))),
          ),
        ),
      )
    )
    val c: Command = Command(
      command = Command.Command.Create(
        CreateCommand(
          templateId = Some(FooTemplateDescriptor.divulgerTemplateId(packageId = packageId)),
          createArguments = createArguments,
        )
      )
    )
    c
  }

}
