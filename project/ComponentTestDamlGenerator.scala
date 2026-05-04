// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import sbt.IO

import java.io.File
import java.nio.file.Path

object ComponentTestDamlGenerator {
  def generateProjectForComponentTest(damlSourceDirectory: File): File = {
    val projDir = damlSourceDirectory.toPath.resolve("P")
    val damlFile = projDir.resolve("daml.yaml")
    val sourceFile = projDir.resolve("daml").resolve("M.daml")

    sourceFile.getParent.toFile.mkdirs()

    val damlYamlContents = """override-components:
                             |  damlc:
                             |    version: $DAML_VERSION
                             |name: P
                             |source: daml
                             |version: 1.0.0
                             |dependencies:
                             |- daml-prim
                             |- daml-stdlib""".stripMargin

    val sourceFileContents = new StringBuilder

    sourceFileContents.append(
      """-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
                                |-- SPDX-License-Identifier: Apache-2.0
                                |
                                |module M where
                                |""".stripMargin
    )

    Range
      .inclusive(1, 300)
      .foreach(idx => sourceFileContents.append(s"""
                                  |template T$idx
                                  |  with
                                  |   payload: Text
                                  |   party: [Party]
                                  |  where
                                  |    signatory party
                                  |    choice Archivingarchivingarchivingarchivingarchivingarchiving$idx : Text
                                  |        with
                                  |           argument: Text
                                  |        controller party
                                  |        do pure argument
                                  |
                                  |""".stripMargin))

    IO.write(damlFile.toFile, damlYamlContents)
    IO.write(sourceFile.toFile, sourceFileContents.toString())

    damlSourceDirectory
  }
}
