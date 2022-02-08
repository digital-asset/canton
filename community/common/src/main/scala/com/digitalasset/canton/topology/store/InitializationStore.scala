// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store

import java.util.concurrent.atomic.AtomicReference

import com.digitalasset.canton.crypto.Fingerprint
import com.digitalasset.canton.topology._
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import io.functionmeta.functionFullName
import slick.jdbc.TransactionIsolation.Serializable

import scala.concurrent.{ExecutionContext, Future}

/** Store where we keep the core identity of the node
  *
  * In Canton, everybody is known by his unique identifier which consists of a string and a fingerprint of a signing key.
  * Participant nodes and domains are known by their UID. This store here stores the identity of the node.
  */
trait InitializationStore {

  def id(implicit traceContext: TraceContext): Future[Option[NodeId]]

  def setId(id: NodeId)(implicit traceContext: TraceContext): Future[Unit]

}

object InitializationStore {
  def apply(storage: Storage, loggerFactory: NamedLoggerFactory)(implicit
      ec: ExecutionContext
  ): InitializationStore =
    storage match {
      case _: MemoryStorage => new InMemoryInitializationStore(loggerFactory)
      case jdbc: DbStorage => new DbInitializationStore(jdbc, loggerFactory)
    }
}

class InMemoryInitializationStore(override protected val loggerFactory: NamedLoggerFactory)
    extends InitializationStore
    with NamedLogging {
  private val myId = new AtomicReference[Option[NodeId]](None)
  override def id(implicit traceContext: TraceContext): Future[Option[NodeId]] =
    Future.successful(myId.get())

  override def setId(id: NodeId)(implicit traceContext: TraceContext): Future[Unit] =
    if (myId.compareAndSet(None, Some(id))) Future.successful(())
    // once we get to this branch, we know that the id is already set (so the logic used here is safe)
    else
      ErrorUtil.requireArgumentAsync(
        myId.get().contains(id),
        s"Unique id of node is already defined as ${myId.get().map(_.toString).getOrElse("")} and can't be changed to $id!",
      )
}

class DbInitializationStore(
    storage: DbStorage,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends InitializationStore
    with NamedLogging {
  import storage.api._

  override def id(implicit traceContext: TraceContext): Future[Option[NodeId]] =
    storage.query(
      {
        for {
          data <- idQuery
        } yield data.headOption.map { case (identity, fingerprint) =>
          NodeId(UniqueIdentifier(identity, Namespace(fingerprint)))
        }
      },
      functionFullName,
    )

  private val idQuery =
    sql"select identifier, namespace from node_id"
      .as[(Identifier, Fingerprint)]

  override def setId(id: NodeId)(implicit traceContext: TraceContext): Future[Unit] =
    storage.queryAndUpdate(
      {
        for {
          storedData <- idQuery
          _ <-
            if (storedData.nonEmpty) {
              val data = storedData(0)
              val prevNodeId = NodeId(UniqueIdentifier(data._1, Namespace(data._2)))
              ErrorUtil.requireArgument(
                prevNodeId == id,
                s"Unique id of node is already defined as $prevNodeId and can't be changed to $id!",
              )
              DbStorage.DbAction.unit
            } else
              sqlu"insert into node_id(identifier, namespace) values(${id.identity.id},${id.identity.namespace.fingerprint.unwrap})"
        } yield ()
      }.transactionally.withTransactionIsolation(Serializable),
      functionFullName,
    )
}
