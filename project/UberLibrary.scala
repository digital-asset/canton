// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import sbt.*
import sbt.Keys.*

import sbtassembly.AssemblyPlugin.autoImport.*

// TODO(i12761): remove when closed
object UberLibrary {

  private def externalDependenciesOf(project: Project): Def.Initialize[Task[Seq[ModuleID]]] =
    Def.task {
      val thisOrg = (project / organization).value
      val descriptors = (project / projectDescriptors).value
      for {
        moduleDescriptor <- descriptors.valuesIterator.toSeq
        dependency <- moduleDescriptor.getDependencies
        // Preserve runtime dependencies and avoid putting plugin and test dependencies to POM files that land
        //  into `UberLibraries` so that they effectively do not become runtime dependencies on the "client" side.
        if !dependency.getModuleConfigurations.exists(List("plugin", "test").contains)
        revisionId = dependency.getDependencyRevisionId
        org = revisionId.getOrganisation if org != thisOrg
        name = revisionId.getName
        version = revisionId.getRevision
      } yield ModuleID(org, name, version)
    }

  private def copy(outputOf: TaskKey[File], to: SettingKey[File]): Def.Initialize[Task[File]] =
    Def.task {
      val source = outputOf.value
      val destination = to.value
      IO.copyFile(source, destination)
      destination
    }

  def of(project: Project): Seq[Def.Setting[?]] =
    Seq(
      projectDependencies := externalDependenciesOf(project).value,
      Compile / packageBin :=
        copy(
          outputOf = project / assembly,
          to = Compile / packageBin / artifactPath,
        ).value,
      Compile / packageSrc :=
        copy(
          outputOf = project / Compile / packageSrc,
          to = Compile / packageSrc / artifactPath,
        ).value,
      Compile / packageDoc :=
        copy(
          outputOf = project / Compile / packageDoc,
          to = Compile / packageDoc / artifactPath,
        ).value,
    )

  def assemblySettings(project: String): Seq[Def.Setting[?]] = Seq(
    // Conforming to the Maven dependency naming convention.
    assembly / assemblyJarName := s"${project}_${scalaBinaryVersion.value}-${version.value}.jar",
    // Do not assembly dependencies other than local projects.
    assemblyPackageDependency / assembleArtifact := false,
    assemblyPackageScala / assembleArtifact := false,
  )

}
