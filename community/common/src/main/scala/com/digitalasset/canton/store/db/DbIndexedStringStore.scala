// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.db

import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.{IndexedStringStore, IndexedStringType}
import io.functionmeta.functionFullName
import cats.data.OptionT

import scala.concurrent.{ExecutionContext, Future}

class DbIndexedStringStore(
    storage: DbStorage,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit val executionContext: ExecutionContext)
    extends IndexedStringStore
    with NamedLogging {

  import com.digitalasset.canton.tracing.TraceContext.Implicits.Empty._
  import storage.api._

  override def getOrCreateIndex(dbTyp: IndexedStringType, str: String): Future[Int] =
    getIndexForStr(dbTyp.source, str).getOrElseF {
      insertIgnore(dbTyp.source, str).flatMap { _ =>
        getIndexForStr(dbTyp.source, str).getOrElse {
          noTracingLogger.error(s"static string $str is still missing in db after i just stored it")
          throw new IllegalStateException(
            s"static string $str is still missing in db after i just stored it"
          )
        }
      }
    }

  private def getIndexForStr(dbType: Int, str: String): OptionT[Future, Int] =
    OptionT(
      storage
        .query(
          sql"select id from static_strings where string = $str and source = $dbType"
            .as[Int]
            .headOption,
          functionFullName,
        )
    )

  private def insertIgnore(dbType: Int, str: String): Future[Unit] = {
    // not sure how to get "last insert id" here in case the row was inserted
    // therefore, we're just querying the db again. this is a bit dorky,
    // but we'll hardly ever do this, so should be good
    val query = storage.profile match {
      case _: DbStorage.Profile.Postgres | _: DbStorage.Profile.H2 =>
        sqlu"insert into static_strings (string, source) values ($str, $dbType) ON CONFLICT DO NOTHING"
      case _: DbStorage.Profile.Oracle =>
        sqlu"""INSERT
              /*+  IGNORE_ROW_ON_DUPKEY_INDEX ( static_strings (string, source) ) */
              INTO static_strings (string, source) VALUES ($str,$dbType)"""
    }
    // and now query it
    storage.update_(query, functionFullName)
  }

  override def getForIndex(dbTyp: IndexedStringType, idx: Int): Future[Option[String]] = {
    storage
      .query(
        sql"select string from static_strings where id = $idx and source = ${dbTyp.source}"
          .as[String],
        functionFullName,
      )
      .map(_.headOption)
  }
}
