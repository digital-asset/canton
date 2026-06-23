// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine

import com.daml.scalautil.Statement.discard
import com.digitalasset.daml.lf.data.Ref.{Name, Party}
import com.digitalasset.daml.lf.data.{BackStack, ImmArray, Ref, Time}
import com.digitalasset.daml.lf.transaction.TransactionOuterClass.Node.NodeTypeCase
import com.digitalasset.daml.lf.transaction.{
  ContractInstanceCoder,
  CreationTime,
  ExternalCallResult,
  FatContractInstance,
  GlobalKey,
  GlobalKeyWithMaintainers,
  Node,
  NodeId,
  SerializationVersion,
  TransactionOuterClass as proto,
  VersionedTransaction,
  ensuresNoUnknownFieldsThenDecode,
  sequence,
}
import com.digitalasset.daml.lf.value.{DecodeError, EncodeError, Value}
import com.digitalasset.daml.lf.{crypto, data, value}
import com.google.protobuf.{ByteString, ProtocolStringList}

import scala.Ordering.Implicits.infixOrderingOps
import scala.collection.immutable.{HashMap, TreeSet}
import scala.jdk.CollectionConverters.*

object TransactionCoder extends TransactionCoder(allowNullCharacters = false)

class TransactionCoder(allowNullCharacters: Boolean) {

  private[this] val ValueCoder =
    new value.ValueCoder(allowNullCharacters = allowNullCharacters).internal
  private[this] val ContractCoder = new ContractInstanceCoder(
    allowNullCharacters = allowNullCharacters
  )

  /** Reads a [[com.digitalasset.daml.lf.transaction.VersionedTransaction]] from protobuf and checks
    * if [[com.digitalasset.daml.lf.transaction.SerializationVersion]] passed in the protobuf is
    * currently supported.
    *
    * Supported serialization versions configured in
    * [[com.digitalasset.daml.lf.transaction.SerializationVersion]].
    *
    * @param protoTx
    *   protobuf encoded transaction
    * @return
    *   decoded transaction
    */
  private[lf] def decodeTransaction(
      protoTx: proto.Transaction
  ): Either[DecodeError, VersionedTransaction] =
    ensuresNoUnknownFieldsThenDecode(protoTx)(internal.decodeTransaction)

  /** Encode a [[com.digitalasset.daml.lf.transaction.Transaction]] to protobuf using
    * [[com.digitalasset.daml.lf.transaction.SerializationVersion]] provided by the libary.
    *
    * @param tx
    *   the transaction to be encoded
    * @return
    *   protobuf encoded transaction
    */
  private[engine] def encodeTransaction(
      tx: VersionedTransaction
  ): Either[EncodeError, proto.Transaction] =
    internal.encodeTransaction(tx)

  private[engine] object internal {

    private[this] def encodeNodeId(id: NodeId): String = id.index.toString

    private[this] def decodeNodeId(s: String): Either[DecodeError, NodeId] =
      scalaz.std.string
        .parseInt(s)
        .fold(_ => Left(DecodeError(s"cannot parse node Id $s")), idx => Right(NodeId(idx)))

    private[this] def encodeValue(
        nodeVersion: SerializationVersion,
        value: Value,
    ): Either[EncodeError, ByteString] =
      ValueCoder.encodeValue(nodeVersion, value)

    private[this] def decodePackageName(s: String): Either[DecodeError, Ref.PackageName] =
      Either
        .cond(
          s.nonEmpty,
          s,
          DecodeError(s"packageName must not be empty"),
        )
        .flatMap(
          Ref.PackageName
            .fromString(_)
            .left
            .map(err => DecodeError(s"Invalid package name '$s': $err"))
        )

    private[this] def encodeKeyWithMaintainers(
        version: SerializationVersion,
        key: GlobalKeyWithMaintainers,
    ): Either[EncodeError, proto.KeyWithMaintainers] =
      if (version >= SerializationVersion.minVersion) {
        val builder = proto.KeyWithMaintainers.newBuilder()
        TreeSet.from(key.maintainers).foreach(builder.addMaintainers(_))
        ValueCoder.encodeValue(version, key.globalKey.key).map { bs =>
          builder.setKey(bs)
          // In canton >=3.5, encodeKeyWithMaintainers should always be called with vSerializationVersion.V2,
          // so the `else` branch here is dead production code. We however need it to preserve canton<3.5's behavior
          // in tests which test the interaction between the encoding of fat contract instances in canton<3.5 and
          // the decoding of fact contract instances in canton>=3.5.
          if (version >= SerializationVersion.V2)
            discard(builder.setHash(key.globalKey.hash.bytes.toByteString))
          builder.build
        }
      } else
        Left(EncodeError(s"Contract key are not supported by $version"))

    private[this] def encodeOptKeyWithMaintainers(
        version: SerializationVersion,
        key: Option[GlobalKeyWithMaintainers],
    ): Either[EncodeError, Option[proto.KeyWithMaintainers]] =
      key match {
        case Some(key) =>
          encodeKeyWithMaintainers(version, key).map(Some(_))
        case None =>
          Right(None)
      }

    private[this] def encodeOptionalValue(
        version: SerializationVersion,
        valueOpt: Option[Value],
    ): Either[EncodeError, ByteString] =
      valueOpt match {
        case Some(value) =>
          encodeValue(version, value)
        case None =>
          Right(ByteString.empty())
      }

    /** encodes a [[Node]] to protocol buffer
      *
      * @param enclosingVersion
      *   the version of the transaction
      * @param nodeId
      *   node id of the node to be encoded
      * @param node
      *   the node to be encoded
      * @return
      *   protocol buffer format node
      */
    private[engine] def encodeNode(
        enclosingVersion: SerializationVersion,
        nodeId: NodeId,
        node: Node,
    ) = {
      val nodeBuilder =
        proto.Node.newBuilder().setNodeId(encodeNodeId(nodeId))

      node match {
        case Node.Rollback(children) =>
          val builder = proto.Node.Rollback.newBuilder()
          children.foreach(id => discard(builder.addChildren(encodeNodeId(id))))
          Right(nodeBuilder.setRollback(builder).build())

        case node: Node.Action =>
          val nodeVersion = node.version
          if (enclosingVersion < nodeVersion)
            Left(
              EncodeError(
                s"A transaction of version $enclosingVersion cannot contain nodes of newer version ($nodeVersion)"
              )
            )
          else {
            discard(nodeBuilder.setVersion(SerializationVersion.toProtoValue(nodeVersion)))

            node match {
              case nc: Node.Create =>
                val fatContractInstance = FatContractInstance.fromCreateNode(
                  create = nc,
                  createTime = CreationTime.CreatedAt(data.Time.Timestamp.Epoch),
                  authenticationData = data.Bytes.Empty,
                )
                for {
                  unversioned <- ContractCoder.encodeFatContractInstanceInternal(
                    fatContractInstance
                  )
                } yield nodeBuilder.setCreate(unversioned).build()

              case nf: Node.Fetch =>
                for {
                  unversioned <- encodeFetch(nf)
                } yield nodeBuilder.setFetch(unversioned).build()

              case ne: Node.Exercise =>
                for {
                  unversioned <- encodeExercise(ne)
                } yield nodeBuilder.setExercise(unversioned).build()

              case nqbk: Node.QueryByKey =>
                for {
                  unversioned <- encodeQueryByKey(nqbk)
                } yield nodeBuilder.setQueryByKey(unversioned).build()

            }
          }
      }
    }

    private[this] def encodeFetch(
        node: Node.Fetch
    ): Either[EncodeError, proto.Node.Fetch] = {
      val version = node.version
      val maintainers = node.keyOpt.fold(Set.empty[Party])(_.maintainers)
      val signatories = node.signatories
      val stakeholders = node.stakeholders
      require(maintainers.subsetOf(signatories))
      require(signatories.subsetOf(stakeholders))
      val non_maintainer_signatories = signatories -- maintainers
      val non_signatory_stakeholders = stakeholders -- signatories

      val builder = proto.Node.Fetch.newBuilder()
      discard(builder.setContractId(node.coid.toBytes.toByteString))
      discard(builder.setPackageName(node.packageName))
      discard(builder.setTemplateId(ValueCoder.encodeIdentifier(node.templateId)))
      node.interfaceId.foreach(iface => builder.setInterfaceId(ValueCoder.encodeIdentifier(iface)))
      non_maintainer_signatories.foreach(builder.addNonMaintainerSignatories)
      non_signatory_stakeholders.foreach(builder.addNonSignatoryStakeholders)
      node.actingParties.foreach(builder.addActors)

      for {
        protoKey <- encodeOptKeyWithMaintainers(version, node.keyOpt)
        _ = protoKey.foreach(builder.setKeyWithMaintainers)
        _ <-
          if (node.byKey)
            Either.cond(
              version >= SerializationVersion.minContractKeys,
              discard(builder.setByKey(true)),
              EncodeError(s"Node field byKey is not supported by version $version"),
            )
          else
            Right(())
      } yield builder.build()
    }

    private[this] def encodeExternalCallResult(
        result: ExternalCallResult
    ): proto.ExternalCallResult =
      proto.ExternalCallResult
        .newBuilder()
        .setExtensionId(result.extensionId)
        .setFunctionId(result.functionId)
        .setConfig(result.config.toByteString)
        .setInput(result.input.toByteString)
        .setOutput(result.output.toByteString)
        .build()

    private[this] def encodeExercise(
        node: Node.Exercise
    ): Either[EncodeError, proto.Node.Exercise] = {
      val builder = proto.Node.Exercise.newBuilder()
      val fetch = Node.Fetch(
        coid = node.targetCoid,
        packageName = node.packageName,
        templateId = node.templateId,
        interfaceId = node.interfaceId,
        actingParties = node.actingParties,
        signatories = node.signatories,
        stakeholders = node.stakeholders,
        keyOpt = node.keyOpt,
        byKey = node.byKey,
        version = node.version,
      )
      for {
        protoFetch <- encodeFetch(fetch)
        _ = builder.setFetch(protoFetch)
        _ = node.interfaceId.foreach(id => builder.setInterfaceId(ValueCoder.encodeIdentifier(id)))
        _ = builder.setChoice(node.choiceId)
        protoArg <- encodeValue(node.version, node.chosenValue)
        _ = builder.setArg(protoArg)
        _ = builder.setConsuming(node.consuming)
        _ = node.children.foreach(id => discard(builder.addChildren(encodeNodeId(id))))
        protoResult <- encodeOptionalValue(node.version, node.exerciseResult)
        _ = builder.setResult(protoResult)
        _ = node.choiceObservers.foreach(builder.addObservers)
        _ <- node.choiceAuthorizers match {
          case Some(authorizers) =>
            if (node.version < SerializationVersion.minChoiceAuthorizers)
              Left(
                EncodeError(s"choice authorizers are not supported by version ${node.version}")
              )
            else
              Either.cond(
                node.choiceAuthorizers.nonEmpty,
                authorizers.foreach(builder.addAuthorizers),
                EncodeError(s"choice authorizers cannot be empty"),
              )
          case None =>
            Right(())
        }
        _ <-
          if (node.externalCallResults.nonEmpty) {
            if (node.version < SerializationVersion.minExternalCallResults)
              Left(
                EncodeError(
                  s"external call results are not supported by version ${node.version}"
                )
              )
            else {
              node.externalCallResults.foreach { result =>
                discard(builder.addExternalCallResults(encodeExternalCallResult(result)))
              }
              Right(())
            }
          } else {
            Right(())
          }
      } yield builder.build()
    }

    private[this] def encodeQueryByKey(
        node: Node.QueryByKey
    ): Either[EncodeError, proto.Node.QueryByKey] =
      for {
        _ <- Either.cond(
          node.version >= SerializationVersion.minContractKeys,
          (),
          EncodeError(s"Contract keys not supported by version ${node.version}"),
        )
        _ <- Either.cond(
          node.exhaustive || node.result.nonEmpty,
          (),
          EncodeError("non exhaustive query by key node must have at least one contract id"),
        )
        builder = proto.Node.QueryByKey.newBuilder()
        _ = discard(builder.setPackageName(node.packageName))
        _ = discard(builder.setTemplateId(ValueCoder.encodeIdentifier(node.templateId)))
        _ = node.result.foreach(cid => discard(builder.addContractId(cid.toBytes.toByteString)))
        _ = discard(builder.setExaustive(node.exhaustive))
        encodedKey <- encodeKeyWithMaintainers(node.version, node.key)
      } yield builder.setKeyWithMaintainers(encodedKey).build()

    private[this] def decodeKeyWithMaintainers(
        version: SerializationVersion,
        templateId: Ref.TypeConId,
        packageName: Ref.PackageName,
        msg: proto.KeyWithMaintainers,
    ): Either[DecodeError, GlobalKeyWithMaintainers] =
      for {
        maintainers <- toPartySet(msg.getMaintainersList)
        // Contracts written with SerializationVersion.V1 should never contain a key, because canton >=3.5 uses
        // V2 for contracts with keys, and canton <3.5 doesn't support keys.
        // Therefore, we fail deserialization, if a key is found with version < SerializationVersin.V2.
        _ <- Either.cond(
          version >= SerializationVersion.V2,
          (),
          DecodeError(s"unexpected contract key encountered at transaction version $version"),
        )
        keyValue <- ValueCoder.decodeValue(version, msg.getKey)
        hash <- crypto.Hash
          .fromBytes(data.Bytes.fromByteString(msg.getHash))
          .left
          .map(DecodeError(_))
        gkey <- GlobalKey
          .build(templateId, packageName, keyValue, hash)
          .left
          .map(hashErr => DecodeError(hashErr.msg))
      } yield GlobalKeyWithMaintainers(gkey, maintainers)

    // package private for test, do not use outside TransactionCoder
    private[this] def decodeValue(
        version: SerializationVersion,
        unversionedProto: ByteString,
    ): Either[DecodeError, Value] =
      ValueCoder.decodeValue(version, unversionedProto)

    private[this] def decodeOptionalValue(
        version: SerializationVersion,
        unversionedProto: ByteString,
    ): Either[DecodeError, Option[Value]] =
      if (unversionedProto.isEmpty)
        Right(None)
      else
        ValueCoder.decodeValue(version, unversionedProto).map(Some(_))

    /** read a [[Node]] from protobuf
      *
      * @param protoNode
      *   protobuf encoded node
      * @return
      *   decoded GenNode
      */
    private[this] def decodeVersionedNode(
        serializationVersion: SerializationVersion,
        protoNode: proto.Node,
    ): Either[DecodeError, (NodeId, Node)] =
      decodeNode(serializationVersion, protoNode)

    private[engine] def decodeNode(
        txVersion: SerializationVersion,
        msg: proto.Node,
    ): Either[DecodeError, (NodeId, Node)] =
      for {
        nodeId <- decodeNodeId(msg.getNodeId)
        node <- msg.getNodeTypeCase match {
          case NodeTypeCase.ROLLBACK =>
            decodeRollback(msg.getVersion, msg.getRollback)
          case NodeTypeCase.CREATE =>
            decodeCreate(txVersion, msg.getVersion, msg.getCreate)
          case NodeTypeCase.FETCH =>
            decodeFetch(txVersion, msg.getVersion, msg.getFetch)
          case NodeTypeCase.EXERCISE =>
            decodeExercise(txVersion, msg.getVersion, msg.getExercise)
          case NodeTypeCase.QUERY_BY_KEY =>
            decodeQueryByKey(txVersion, msg.getVersion, msg.getQueryByKey)
          case NodeTypeCase.NODETYPE_NOT_SET => Left(DecodeError("Unset Node type"))
        }
      } yield (nodeId, node)

    private[this] def decodeRollback(
        nodeVersionStr: String,
        msg: proto.Node.Rollback,
    ) =
      for {
        _ <- Either.cond(
          nodeVersionStr.isEmpty,
          (),
          DecodeError("unexpected node version for Rollback node"),
        )
        children <- decodeChildren(msg.getChildrenList)
      } yield Node.Rollback(children)

    private[this] def decodeCreate(
        txVersion: SerializationVersion,
        nodeVersionStr: String,
        msg: proto.FatContractInstance,
    ): Either[DecodeError, Node.Create] =
      for {
        // call to decodeFatContractInstance checks for unknown fields
        nodeVersion <- decodeActionNodeVersion(txVersion, nodeVersionStr)
        contract <- ContractCoder.decodeFatContractInstance(nodeVersion, msg)
        _ <- Either.cond(
          contract.createdAt == CreationTime.CreatedAt(Time.Timestamp.Epoch),
          (),
          DecodeError("unexpected created_at field in create node"),
        )
        _ <- Either.cond(
          contract.authenticationData.isEmpty,
          (),
          DecodeError("unexpected canton_data field in create node"),
        )
      } yield contract.toCreateNode

    private[this] def decodeFetch(
        txVersion: SerializationVersion,
        nodeVersionStr: String,
        msg: proto.Node.Fetch,
    ): Either[DecodeError, Node.Fetch] =
      for {
        nodeVersion <- decodeActionNodeVersion(txVersion, nodeVersionStr)
        cid <- ValueCoder.decodeCoid(msg.getContractId)
        pkgName <- decodePackageName(msg.getPackageName)
        templateId <- ValueCoder.decodeIdentifier(msg.getTemplateId)
        interfaceId <-
          if (msg.hasInterfaceId) {
            ValueCoder.decodeIdentifier(msg.getInterfaceId).map(Some(_))
          } else {
            Right(None)
          }
        nonMaintainerSignatories <- toPartySet(msg.getNonMaintainerSignatoriesList)
        nonSignatoryStakeholdersList <- toPartySet(msg.getNonSignatoryStakeholdersList)
        actingParties <- toPartySet(msg.getActorsList)
        keyOpt <-
          if (msg.hasKeyWithMaintainers)
            decodeKeyWithMaintainers(
              nodeVersion,
              templateId,
              pkgName,
              msg.getKeyWithMaintainers,
            ).map(Some(_))
          else
            Right(None)
        maintainers = keyOpt.fold(Set.empty[Party])(_.maintainers)
        signatories = nonMaintainerSignatories ++ maintainers
        stakeholders = nonSignatoryStakeholdersList ++ signatories
        byKey <-
          if (msg.getByKey)
            Either.cond(
              nodeVersion >= SerializationVersion.minContractKeys,
              true,
              DecodeError(s"transaction key is not supported by version $nodeVersion"),
            )
          else
            Right(false)
      } yield Node.Fetch(
        coid = cid,
        packageName = pkgName,
        templateId = templateId,
        actingParties = actingParties,
        signatories = signatories,
        stakeholders = stakeholders,
        keyOpt = keyOpt,
        byKey = byKey,
        version = nodeVersion,
        interfaceId = interfaceId,
      )

    private[this] def decodeExternalCallResult(
        resultProto: proto.ExternalCallResult
    ): ExternalCallResult =
      ExternalCallResult(
        extensionId = resultProto.getExtensionId,
        functionId = resultProto.getFunctionId,
        config = data.Bytes.fromByteString(resultProto.getConfig),
        input = data.Bytes.fromByteString(resultProto.getInput),
        output = data.Bytes.fromByteString(resultProto.getOutput),
      )

    private[this] def decodeExercise(
        txVersion: SerializationVersion,
        nodeVersionStr: String,
        msg: proto.Node.Exercise,
    ): Either[DecodeError, Node.Exercise] =
      for {
        fetch <- decodeFetch(txVersion, nodeVersionStr, msg.getFetch)
        nodeVersion = fetch.version
        interfaceId <-
          if (msg.hasInterfaceId) {
            ValueCoder.decodeIdentifier(msg.getInterfaceId).map(Some(_))
          } else {
            Right(None)
          }
        choiceName <- toIdentifier(msg.getChoice)
        arg <- decodeValue(nodeVersion, msg.getArg)
        children <- decodeChildren(msg.getChildrenList)
        result <- decodeOptionalValue(nodeVersion, msg.getResult)
        observers <- toPartySet(msg.getObserversList)
        authorizers <-
          if (msg.getAuthorizersCount == 0)
            Right(None)
          else if (nodeVersion < SerializationVersion.minChoiceAuthorizers)
            Left(DecodeError(s"Exercise Authorizer not supported by version $nodeVersion"))
          else
            toPartySet(msg.getAuthorizersList).map(Some(_))
        externalCallResults <-
          if (msg.getExternalCallResultsCount == 0)
            Right(ImmArray.empty[ExternalCallResult])
          else if (nodeVersion < SerializationVersion.minExternalCallResults)
            Left(
              DecodeError(
                s"External call results not supported by version $nodeVersion"
              )
            )
          else
            Right(
              ImmArray.from(
                msg.getExternalCallResultsList.asScala.map(decodeExternalCallResult)
              )
            )
      } yield Node.Exercise(
        targetCoid = fetch.coid,
        packageName = fetch.packageName,
        templateId = fetch.templateId,
        interfaceId = interfaceId,
        choiceId = choiceName,
        consuming = msg.getConsuming,
        actingParties = fetch.actingParties,
        chosenValue = arg,
        stakeholders = fetch.stakeholders,
        signatories = fetch.signatories,
        choiceObservers = observers,
        choiceAuthorizers = authorizers,
        children = children,
        exerciseResult = result,
        keyOpt = fetch.keyOpt,
        byKey = fetch.byKey,
        externalCallResults = externalCallResults,
        version = fetch.version,
      )

    private[this] def decodeQueryByKey(
        txVersion: SerializationVersion,
        nodeVersionStr: String,
        msg: proto.Node.QueryByKey,
    ) =
      for {
        nodeVersion <- decodeActionNodeVersion(txVersion, nodeVersionStr)
        _ <- Either.cond(
          txVersion >= SerializationVersion.minContractKeys,
          (),
          DecodeError(s"Contract key not supported by version $nodeVersion"),
        )
        _ <- Either.cond(
          msg.getExaustive || msg.getContractIdCount > 0,
          (),
          DecodeError("non exhaustive query by key node must have at least one contract id"),
        )
        pkgName <- decodePackageName(msg.getPackageName)
        templateId <- ValueCoder.decodeIdentifier(msg.getTemplateId)
        key <-
          decodeKeyWithMaintainers(
            nodeVersion,
            templateId,
            pkgName,
            msg.getKeyWithMaintainers,
          )
        contractIds <- msg.getContractIdList.asScala.toVector
          .foldLeft[Either[DecodeError, Vector[Value.ContractId]]](Right(Vector.empty)) {
            case (acc, cidBytes) =>
              for {
                prev <- acc
                cid <- ValueCoder.decodeCoid(cidBytes)
              } yield prev :+ cid
          }
      } yield Node.QueryByKey(pkgName, templateId, msg.getExaustive, key, contractIds, nodeVersion)

    private[this] def decodeChildren(
        strList: ProtocolStringList
    ): Either[DecodeError, ImmArray[NodeId]] =
      strList.asScala
        .foldLeft[Either[DecodeError, BackStack[NodeId]]](Right(BackStack.empty)) {
          case (Left(e), _) => Left(e)
          case (Right(ids), s) => decodeNodeId(s).map(ids :+ _)
        }
        .map(_.toImmArray)

    private[TransactionCoder] def encodeTransaction(
        transaction: VersionedTransaction
    ): Either[EncodeError, proto.Transaction] = {
      val builder = proto.Transaction
        .newBuilder()
        .setVersion(SerializationVersion.toProtoValue(transaction.version))
      transaction.roots.foreach(nid => discard(builder.addRoots(encodeNodeId(nid))))

      transaction
        .fold[Either[EncodeError, proto.Transaction.Builder]](
          Right(builder)
        ) { case (builderOrError, (nid, _)) =>
          for {
            builder <- builderOrError
            encodedNode <- encodeNode(
              transaction.version,
              nid,
              transaction.nodes(nid),
            )
          } yield builder.addNodes(encodedNode)
        }
        .map(_.build)
    }

    private[this] def decodeActionNodeVersion(
        txVersion: SerializationVersion,
        nodeVersionStr: String,
    ): Either[DecodeError, SerializationVersion] =
      for {
        nodeVersion <- decodeVersion(nodeVersionStr)
        _ <- Either.cond(
          nodeVersion <= txVersion,
          (),
          DecodeError(
            s"A transaction of version $txVersion cannot contain node of newer version (version $nodeVersion)"
          ),
        )
      } yield nodeVersion

    private[this] def decodeVersion(vs: String): Either[DecodeError, SerializationVersion] =
      SerializationVersion.fromString(vs).left.map(DecodeError)

    private[TransactionCoder] def decodeTransaction(
        protoTx: proto.Transaction
    ): Either[DecodeError, VersionedTransaction] =
      for {
        version <- decodeVersion(protoTx.getVersion)
        tx <- decodeTransaction(version, protoTx)
      } yield tx

    private[this] def decodeTransaction(
        txVersion: SerializationVersion,
        msg: proto.Transaction,
    ): Either[DecodeError, VersionedTransaction] = for {
      roots <- msg.getRootsList.asScala
        .foldLeft[Either[DecodeError, BackStack[NodeId]]](Right(BackStack.empty[NodeId])) {
          case (Right(acc), s) => decodeNodeId(s).map(acc :+ _)
          case (Left(e), _) => Left(e)
        }
        .map(_.toImmArray)
      nodes <- msg.getNodesList.asScala
        .foldLeft[Either[DecodeError, HashMap[NodeId, Node]]](Right(HashMap.empty)) {
          case (Left(e), _) => Left(e)
          case (Right(acc), s) =>
            decodeVersionedNode(txVersion, s).map(acc + _)
        }
    } yield VersionedTransaction(txVersion, nodes, roots)

    private[this] def toPartySet(strList: ProtocolStringList): Either[DecodeError, Set[Party]] = {
      val parties = strList
        .asByteStringList()
        .asScala
        .map(bs => Party.fromString(bs.toStringUtf8))

      sequence(parties) match {
        case Left(err) => Left(DecodeError(s"Cannot decode party: $err"))
        case Right(ps) => Right(ps.toSet)
      }
    }

    private[this] def toIdentifier(s: String): Either[DecodeError, Name] =
      Name.fromString(s).left.map(DecodeError)
  }
}
