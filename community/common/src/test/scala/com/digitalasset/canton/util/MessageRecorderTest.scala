// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.protocol.v30
import com.digitalasset.canton.util.MessageRecorderTest.{Data, Data2}
import com.digitalasset.canton.validation.ProtoUnvalidated.syntax.*
import com.digitalasset.canton.{BaseTestWordSpec, HasTempDirectory}

import java.nio.file.Path

class MessageRecorderTest extends BaseTestWordSpec with HasTempDirectory {

  val testData: Seq[Data] = (0 until 3) map Data.apply

  val recordFile: Path = tempDirectory.resolve("recorded-test-data")

  val recorder = new MessageRecorder(DefaultProcessingTimeouts.testing, loggerFactory)

  "A message recorder" can {
    "record data" in {
      recorder.startRecording(recordFile)
      testData.foreach(m => recorder.record(m))
      recorder.stopRecording()
    }

    "read recorded data" in {
      val readData = MessageRecorder.load[Data](recordFile, logger)
      readData shouldBe testData
    }

    "catch type errors" in {
      a[ClassCastException] shouldBe thrownBy(MessageRecorder.load[Data2](recordFile, logger))
    }

    "round-trip a proto message with unvalidated strings" in {
      // A repeated `string` field boxes the `ProtoUnvalidatedString` value class, so it serializes
      // only because the value class is `Serializable`; a scalar field erases to `String`.
      val message = v30.RecipientsTree(
        recipients = Seq("alice", "bob").map(_.toProtoUnvalidated),
        children = Seq.empty,
      )
      val protoFile = tempDirectory.resolve("recorded-proto-data")
      val protoRecorder = new MessageRecorder(DefaultProcessingTimeouts.testing, loggerFactory)

      protoRecorder.startRecording(protoFile)
      protoRecorder.record(message)
      protoRecorder.stopRecording()

      val loaded = MessageRecorder.load[v30.RecipientsTree](protoFile, logger)
      loaded shouldBe List(message)
      // The elements must come back boxed, or using them casts a String to the wrapper.
      loaded.map(_.toByteString) shouldBe List(message.toByteString)
    }
  }
}

object MessageRecorderTest {
  final case class Data(i: Int)
  final case class Data2(s: String)
}
