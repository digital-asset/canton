// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console.commands

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.admin.api.client.commands.{TopologyAdminCommands, VaultAdminCommands}
import com.digitalasset.canton.admin.api.client.data.ListKeyOwnersResult
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.{AdminCommandRunner, ConsoleEnvironment, Help, Helpful}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.store.CryptoPublicStoreError
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.topology.{KeyOwner, KeyOwnerCode}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.BinaryFileUtil
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString

import java.io.File
import java.nio.file.Files
import java.nio.file.attribute.PosixFilePermission.{OWNER_READ, OWNER_WRITE}
import java.time.Instant
import scala.concurrent.ExecutionContext.Implicits.*
import scala.concurrent.Future
import scala.jdk.CollectionConverters.*

class SecretKeyAdministration(runner: AdminCommandRunner, consoleEnvironment: ConsoleEnvironment)
    extends Helpful {

  import runner.*

  @Help.Summary("List keys in private vault")
  @Help.Description("""Returns all public keys to the corresponding private keys in the key vault.
                      |Optional arguments can be used for filtering.""")
  def list(
      filterFingerprint: String = "",
      filterName: String = "",
      purpose: Set[KeyPurpose] = Set.empty,
  ): Seq[PublicKeyWithName] =
    consoleEnvironment.run {
      adminCommand(VaultAdminCommands.ListMyKeys(filterFingerprint, filterName, purpose))
    }

  @Help.Summary("Generate new public/private key pair for signing and store it in the vault")
  @Help.Description(
    """
      |The optional name argument allows you to store an associated string for your convenience.
      |The scheme can be used to select a key scheme and the default scheme is used if left unspecified."""
  )
  def generate_signing_key(
      name: String = "",
      scheme: Option[SigningKeyScheme] = None,
  ): SigningPublicKey = {
    consoleEnvironment.run {
      adminCommand(VaultAdminCommands.GenerateSigningKey(name, scheme))
    }
  }

  @Help.Summary("Generate new public/private key pair for encryption and store it in the vault")
  @Help.Description(
    """
      |The optional name argument allows you to store an associated string for your convenience.
      |The scheme can be used to select a key scheme and the default scheme is used if left unspecified."""
  )
  def generate_encryption_key(
      name: String = "",
      scheme: Option[EncryptionKeyScheme] = None,
  ): EncryptionPublicKey = {
    consoleEnvironment.run {
      adminCommand(VaultAdminCommands.GenerateEncryptionKey(name, scheme))
    }
  }

  @Help.Summary("Change the wrapper key for encrypted private keys store")
  @Help.Description(
    """Change the wrapper key (e.g. AWS KMS key) being used to encrypt the private keys in the store.
      |newWrapperKeyId: The optional new wrapper key id to be used. If the wrapper key id is empty Canton will generate a new key based on the current configuration."""
  )
  def rotate_wrapper_key(
      newWrapperKeyId: String = ""
  ): Unit = {
    consoleEnvironment.run {
      adminCommand(VaultAdminCommands.RotateWrapperKey(newWrapperKeyId))
    }
  }

}

class LocalSecretKeyAdministration(
    runner: AdminCommandRunner,
    consoleEnvironment: ConsoleEnvironment,
    crypto: => Crypto,
) extends SecretKeyAdministration(runner, consoleEnvironment) {

  private def run[V](eitherT: EitherT[Future, String, V], action: String): V = {
    import TraceContext.Implicits.Empty.*
    implicit val loggingContext: ErrorLoggingContext =
      ErrorLoggingContext.fromTracedLogger(runner.tracedLogger)
    consoleEnvironment.environment.config.parameters.timeouts.processing.default
      .await(action)(eitherT.value) match {
      case Left(error) =>
        throw new IllegalArgumentException(s"Problem while $action. Error: $error")
      case Right(value) => value
    }
  }

  @Help.Summary("Upload (load and import) a key pair from file")
  def upload(filename: String, name: Option[String]): Unit = TraceContext.withNewTraceContext {
    implicit traceContext =>
      val cmd = for {
        keyPairContent <- EitherT.fromEither[Future](
          BinaryFileUtil.readByteStringFromFile(filename)
        )
        validatedName <- name.traverse(KeyName.create).toEitherT[Future]
        keyPair <- CryptoKeyPair
          .fromByteString(keyPairContent)
          .leftMap(_.toString)
          .toEitherT[Future]
        _ <- loadKeyPair(validatedName, keyPair)
      } yield ()
      run(cmd, "importing key pair")
  }

  @Help.Summary("Upload a key pair")
  def upload(
      pairBytes: ByteString,
      name: Option[String],
  ): Unit =
    TraceContext.withNewTraceContext { implicit traceContext =>
      val cmd = for {
        validatedName <- name.traverse(KeyName.create).toEitherT[Future]
        keyPair <- CryptoKeyPair
          .fromByteString(pairBytes)
          .leftMap(_.toString)
          .toEitherT[Future]
        _ <- loadKeyPair(validatedName, keyPair)
      } yield ()
      run(cmd, "importing key pair")
    }

  private def loadKeyPair(
      validatedName: Option[KeyName],
      keyPair: CryptoKeyPair[_ <: PublicKey, _ <: PrivateKey],
  )(implicit traceContext: TraceContext): EitherT[Future, String, Unit] =
    for {
      _ <- crypto.cryptoPublicStore
        .storePublicKey(keyPair.publicKey, validatedName)
        .recoverWith {
          // if the existing key is the same, then ignore error
          case error: CryptoPublicStoreError.KeyAlreadyExists =>
            for {
              existing <- crypto.cryptoPublicStore.publicKey(keyPair.publicKey.fingerprint)
              _ <-
                if (existing.contains(keyPair.publicKey))
                  EitherT.rightT[Future, CryptoPublicStoreError](())
                else EitherT.leftT[Future, Unit](error: CryptoPublicStoreError)
            } yield ()
        }
        .leftMap(_.toString)
      _ <- crypto.cryptoPrivateStore
        .storePrivateKey(keyPair.privateKey, validatedName)
        .leftMap(_.toString)
    } yield ()

  @Help.Summary("Download key pair")
  def download(
      fingerprint: Fingerprint,
      protocolVersion: ProtocolVersion = ProtocolVersion.latest,
  ): ByteString =
    TraceContext.withNewTraceContext { implicit traceContext =>
      val cmd = for {
        privateKey <- crypto.cryptoPrivateStore
          .exportPrivateKey(fingerprint)
          .leftMap(_.toString)
          .subflatMap(_.toRight(s"no private key found for [$fingerprint]"))
          .leftMap(err => s"Error retrieving private key [$fingerprint] $err")
        publicKey <- crypto.cryptoPublicStore
          .publicKey(fingerprint)
          .leftMap(_.toString)
          .subflatMap(_.toRight(s"no public key found for [$fingerprint]"))
          .leftMap(err => s"Error retrieving public key [$fingerprint] $err")
        keyPair: CryptoKeyPair[PublicKey, PrivateKey] = (publicKey, privateKey) match {
          case (pub: SigningPublicKey, pkey: SigningPrivateKey) =>
            new SigningKeyPair(pub, pkey)
          case (pub: EncryptionPublicKey, pkey: EncryptionPrivateKey) =>
            new EncryptionKeyPair(pub, pkey)
          case _ => sys.error("public and private keys must have same purpose")
        }
        keyPairBytes = keyPair.toByteString(protocolVersion)
      } yield keyPairBytes
      run(cmd, "exporting key pair")
    }

  @Help.Summary("Download key pair and save it to a file")
  def download_to(
      fingerprint: Fingerprint,
      outputFile: String,
      protocolVersion: ProtocolVersion = ProtocolVersion.latest,
  ): Unit =
    run(
      EitherT.rightT(writeToFile(Some(outputFile), download(fingerprint, protocolVersion))),
      "saving key pair to file",
    )

  private def writeToFile(outputFile: Option[String], bytes: ByteString): Unit =
    outputFile.foreach { filename =>
      val file = new File(filename)
      file.createNewFile()
      // only current user has permissions with the file
      try {
        Files.setPosixFilePermissions(file.toPath, Set(OWNER_READ, OWNER_WRITE).asJava)
      } catch {
        // the above will throw on non-posix systems such as windows
        case _: UnsupportedOperationException =>
      }
      BinaryFileUtil.writeByteStringToFile(filename, bytes)
    }

  @Help.Summary("Delete private key")
  def delete(fingerprint: Fingerprint, force: Boolean = false): Unit =
    TraceContext.withNewTraceContext { implicit traceContext =>
      def deleteKey(): Unit =
        run(
          crypto.cryptoPrivateStore.removePrivateKey(fingerprint).leftMap(_.toString),
          "deleting private key",
        )

      if (force)
        deleteKey()
      else {
        println(
          s"Are you sure you want to delete the private key with fingerprint $fingerprint? yes/no"
        )
        println(s"This action is irreversible and can have undesired effects if done carelessly.")
        print("> ")
        val answer = Option(scala.io.StdIn.readLine())
        if (answer.exists(_.toLowerCase == "yes")) deleteKey()
      }
    }

}

class PublicKeyAdministration(
    runner: AdminCommandRunner,
    consoleEnvironment: ConsoleEnvironment,
) extends Helpful {

  import runner.*

  private def defaultLimit: PositiveInt =
    consoleEnvironment.environment.config.parameters.console.defaultLimit

  @Help.Summary("Upload public key")
  @Help.Description(
    """Import a public key and store it together with a name used to provide some context to that key."""
  )
  def upload(keyBytes: ByteString, name: Option[String]): Fingerprint = consoleEnvironment.run {
    adminCommand(
      VaultAdminCommands.ImportPublicKey(keyBytes, name)
    )
  }

  @Help.Summary("Upload public key")
  @Help.Summary(
    "Load a public key from a file and store it together with a name used to provide some context to that key."
  )
  def upload(filename: String, name: Option[String]): Fingerprint = consoleEnvironment.run {
    BinaryFileUtil.readByteStringFromFile(filename) match {
      case Right(bytes) => adminCommand(VaultAdminCommands.ImportPublicKey(bytes, name))
      case Left(err) => throw new IllegalArgumentException(err)
    }
  }

  @Help.Summary("Download public key")
  def download(
      fingerprint: Fingerprint,
      protocolVersion: ProtocolVersion = ProtocolVersion.latest,
  ): ByteString = {
    val keys = list(fingerprint.unwrap)
    if (keys.sizeCompare(1) == 0) { // vector doesn't like matching on Nil
      val key = keys.headOption.getOrElse(sys.error("no key"))
      key.publicKey.toByteString(protocolVersion)
    } else {
      if (keys.isEmpty) throw new IllegalArgumentException(s"no key found for [$fingerprint]")
      else
        throw new IllegalArgumentException(
          s"found multiple results for [$fingerprint]: ${keys.map(_.publicKey.fingerprint)}"
        )
    }
  }

  @Help.Summary("Download public key and save it to a file")
  def download_to(
      fingerprint: Fingerprint,
      outputFile: String,
      protocolVersion: ProtocolVersion = ProtocolVersion.latest,
  ): Unit = {
    BinaryFileUtil.writeByteStringToFile(
      outputFile,
      download(fingerprint, protocolVersion),
    )
  }

  @Help.Summary("List public keys in registry")
  @Help.Description("""Returns all public keys that have been added to the key registry.
    Optional arguments can be used for filtering.""")
  def list(filterFingerprint: String = "", filterContext: String = ""): Seq[PublicKeyWithName] =
    consoleEnvironment.run {
      adminCommand(VaultAdminCommands.ListPublicKeys(filterFingerprint, filterContext))
    }

  @Help.Summary("List active owners with keys for given search arguments.")
  @Help.Description("""This command allows deep inspection of the topology state.
      |The response includes the public keys.
      |Optional filterKeyOwnerType type can be 'ParticipantId.Code' , 'MediatorId.Code','SequencerId.Code', 'DomainTopologyManagerId.Code'.
      |""")
  def list_owners(
      filterKeyOwnerUid: String = "",
      filterKeyOwnerType: Option[KeyOwnerCode] = None,
      filterDomain: String = "",
      asOf: Option[Instant] = None,
      limit: PositiveInt = defaultLimit,
  ): Seq[ListKeyOwnersResult] = consoleEnvironment.run {
    adminCommand(
      TopologyAdminCommands.Aggregation
        .ListKeyOwners(filterDomain, filterKeyOwnerType, filterKeyOwnerUid, asOf, limit)
    )
  }

  @Help.Summary("List keys for given keyOwner.")
  @Help.Description(
    """This command is a convenience wrapper for `list_key_owners`, taking an explicit keyOwner as search argument.
      |The response includes the public keys."""
  )
  def list_by_owner(
      keyOwner: KeyOwner,
      filterDomain: String = "",
      asOf: Option[Instant] = None,
      limit: PositiveInt = defaultLimit,
  ): Seq[ListKeyOwnersResult] = consoleEnvironment.run {
    adminCommand(
      TopologyAdminCommands.Aggregation.ListKeyOwners(
        filterDomain = filterDomain,
        filterKeyOwnerType = Some(keyOwner.code),
        filterKeyOwnerUid = keyOwner.uid.toProtoPrimitive,
        asOf,
        limit,
      )
    )
  }
}

class KeyAdministrationGroup(
    runner: AdminCommandRunner,
    consoleEnvironment: ConsoleEnvironment,
) extends Helpful {

  private lazy val publicAdmin =
    new PublicKeyAdministration(runner, consoleEnvironment)
  private lazy val secretAdmin = new SecretKeyAdministration(runner, consoleEnvironment)

  @Help.Summary("Manage public keys")
  @Help.Group("Public keys")
  def public: PublicKeyAdministration = publicAdmin

  @Help.Summary("Manage secret keys")
  @Help.Group("Secret keys")
  def secret: SecretKeyAdministration = secretAdmin

}

class LocalKeyAdministrationGroup(
    runner: AdminCommandRunner,
    consoleEnvironment: ConsoleEnvironment,
    crypto: => Crypto,
) extends KeyAdministrationGroup(runner, consoleEnvironment) {

  private lazy val localSecretAdmin: LocalSecretKeyAdministration =
    new LocalSecretKeyAdministration(runner, consoleEnvironment, crypto)

  @Help.Summary("Manage secret keys")
  @Help.Group("Secret keys")
  override def secret: LocalSecretKeyAdministration = localSecretAdmin

}
