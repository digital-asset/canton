import Dependencies.z3
import sbt.{Def, taskKey, Task, Keys, IO, Cache}
import sbt.Keys.*
import sbt.io.syntax.*
import sbt.util.CacheImplicits.*
import sjsonnew.{JsonFormat, BasicJsonProtocol, deserializationError}
import sjsonnew.support.scalajson.unsafe.CompactPrinter

import java.net.URI
import java.io.File
import java.nio.file.Files
import java.security.MessageDigest

object Z3 {

  case class Z3Install(jar: File, nativeLibraryPath: File)

  object Z3Install {
    implicit val jsonFormat: JsonFormat[Z3Install] = {
      import BasicJsonProtocol.*
      projectFormat[Z3Install, (File, File)](
        z => (z.jar, z.nativeLibraryPath),
        { case (jar, nativeLibraryPath) => Z3Install(jar, nativeLibraryPath) },
      )
    }
  }

  val z3Install = taskKey[Z3Install]("install z3 from github.com/Z3Prover/z3")

  private def sha256Hex(f: File): String = {
    val digest = MessageDigest.getInstance("SHA-256")
    val bytes = Files.readAllBytes(f.toPath)
    digest.update(bytes)
    digest.digest().map("%02x".format(_)).mkString
  }

  // The native library files we need from the z3 distribution's bin/ directory.
  private val neededFiles: Seq[String] = {
    val os = sys.props("os.name").toLowerCase
    val libExt = if (os.contains("mac")) "dylib" else "so"
    Seq("com.microsoft.z3.jar", s"libz3java.$libExt", s"libz3.$libExt")
  }

  def buildSettings: Seq[Def.Setting[Task[Z3Install]]] = Seq(
    z3Install := {
      val stream = Keys.streams.value
      val targetDir = baseDirectory.value / "target"
      val outputFolder = targetDir / "z3"

      val cacheStore = stream.cacheStoreFactory.make("z3")
      val downloadZ3IfNeeded = Cache.cached(cacheStore) { (_: String) =>
        IO.withTemporaryDirectory { tmpDir =>
          stream.log.info(s"Downloading ${z3.url}")
          val zipFile = tmpDir / s"z3-${z3.version}.zip"
          IO.transfer(URI.create(z3.url).toURL.openStream(), zipFile)
          val actual = sha256Hex(zipFile)
          if (actual != z3.sha256) {
            throw new RuntimeException(
              s"SHA-256 mismatch for Z3 download. Expected: ${z3.sha256}, got: $actual"
            )
          }
          IO.unzip(zipFile, tmpDir)
          val binDir = tmpDir / z3.dirName / "bin"
          IO.createDirectory(outputFolder)
          for (name <- neededFiles) {
            val src = binDir / name
            if (!src.exists())
              throw new RuntimeException(s"Expected file not found in z3 archive: $src")
            IO.copyFile(src, outputFolder / name)
          }
        }
        Z3Install(
          jar = outputFolder / "com.microsoft.z3.jar",
          nativeLibraryPath = outputFolder,
        )
      }
      downloadZ3IfNeeded(z3.version)
    }
  )
}
