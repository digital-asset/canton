// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.proto

import com.digitalasset.canton.http.json.v2.ExtractedProtoComments
import io.protostuff.compiler.model.Proto
import io.protostuff.compiler.parser.{
  ClasspathFileReader,
  FileDescriptorLoaderImpl,
  FileReader,
  ImporterImpl,
  LocalFileReader,
  ParseErrorLogger,
  ProtoContextPostProcessor,
}

import java.net.JarURLConnection
import java.nio.file.{Files, Path, Paths}
import java.util.jar.JarFile
import scala.jdk.CollectionConverters.*
import scala.util.Using

object ProtoParser {

  val ledgerApiProtoLocation = "com/daml/ledger/api/v2"
  val teaApiProtoLocation = "com/digitalasset/canton/tea/v1"

  private val protoLocations: Seq[(String, String)] = Seq(
    ledgerApiProtoLocation -> s"$ledgerApiProtoLocation/transaction.proto",
    teaApiProtoLocation -> s"$teaApiProtoLocation/traffic_service.proto",
  )

  def readProto(): ExtractedProtoComments =
    ProtoDescriptionExtractor.extract(parseProtos())

  def parseProtos(): Seq[Proto] = {
    val classLoader = Thread.currentThread.getContextClassLoader

    val errorListener = new ParseErrorLogger()
    val fdLoader =
      new FileDescriptorLoaderImpl(errorListener, Set.empty[ProtoContextPostProcessor].asJava)
    val importer = new ImporterImpl(fdLoader)

    protoLocations.flatMap { case (location, sampleProtoFile) =>
      val (protoFiles, fileReader) = locateProtoFiles(classLoader, location, sampleProtoFile)
      protoFiles.map { pf =>
        val protoCtx = importer.importFile(fileReader, pf)
        protoCtx.getProto()
      }
    }
  }

  private def locateProtoFiles(
      classLoader: ClassLoader,
      location: String,
      sampleProtoFile: String,
  ): (Seq[String], FileReader) = {
    val url = Option(classLoader.getResource(sampleProtoFile))
      .getOrElse(
        throw new IllegalStateException(
          s"Could not find proto resource '$sampleProtoFile' on the classpath"
        )
      )
    val resourceConnection = url.openConnection
    resourceConnection match {
      case jarResource: JarURLConnection =>
        Using(jarResource.getJarFile()) { jarFile =>
          val entries = findProtoFilesInJar(jarFile, location)
          (entries.map(entry => entry.getRealName()), new ClasspathFileReader())
        }.fold(cause => throw new IllegalStateException(cause), identity)
      case fileResource =>
        val protoMainPath = Paths.get(fileResource.getURL().getPath()).getParent()
        (findProtoFilesInFileSystem(protoMainPath), new LocalFileReader(protoMainPath))
    }
  }

  private def findProtoFilesInJar(jarFile: JarFile, location: String) =
    jarFile
      .entries()
      .asScala
      .filter(entry =>
        entry.getRealName.startsWith(location) && entry.getRealName.endsWith(".proto")
      )
      .toSeq

  private def findProtoFilesInFileSystem(protoFolder: Path, prefix: String = ""): Seq[String] =
    Files
      .list(protoFolder)
      .toList
      .asScala
      .toSeq
      .flatMap { f =>
        if (Files.isRegularFile(f) && f.getFileName().toString().endsWith(".proto")) {
          Seq(f.getFileName().toString()).map(f => s"$prefix$f")
        } else if (Files.isDirectory(f)) {
          findProtoFilesInFileSystem(f, s"$prefix${f.getFileName()}/")
        } else Seq.empty
      }
}
