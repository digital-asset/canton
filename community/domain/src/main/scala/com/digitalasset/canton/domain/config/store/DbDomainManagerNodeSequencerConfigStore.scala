// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.config.store

import cats.data.EitherT
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.sequencing.SequencerConnection
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString
import slick.jdbc.{GetResult, PositionedParameters, SetParameter}

import scala.concurrent.{ExecutionContext, Future}

class DbDomainManagerNodeSequencerConfigStore(
    storage: DbStorage,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends DomainManagerNodeSequencerConfigStore
    with NamedLogging {
  import storage.api._
  import storage.converters._

  implicit val getSequencerConnection: GetResult[SequencerConnection] = GetResult(r =>
    SequencerConnection
      .fromByteString(ByteString.copyFrom(r.<<[Array[Byte]]))
      .fold[SequencerConnection](
        error =>
          throw new DbDeserializationException(s"Error deserializing sequencer connection $error"),
        identity,
      )
  )

  implicit val setParameterSequencerConnection: SetParameter[SequencerConnection] =
    (s: SequencerConnection, pp: PositionedParameters) =>
      pp >> s.toByteString(ProtocolVersion.default).toByteArray

  // sentinel value used to ensure the table can only have a single row
  // see create table sql for more details
  private val singleRowLockValue = "X"

  override def fetchConfiguration(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, Option[DomainNodeSequencerConfig]] =
    EitherT.right(
      storage
        .query(
          for {
            connection <-
              sql"""select sequencer_connection from domain_sequencer_config #${storage.limit(1)}"""
                .as[SequencerConnection]
                .headOption
          } yield connection,
          "fetchConfiguration",
        )
        .map(_.map(DomainNodeSequencerConfig(_)))
    )

  override def saveConfiguration(
      configuration: DomainNodeSequencerConfig
  )(implicit traceContext: TraceContext): EitherT[Future, String, Unit] =
    EitherT.right(
      storage
        .update_(
          storage.profile match {
            case _: DbStorage.Profile.H2 =>
              sqlu"""merge into domain_sequencer_config
                   (lock, sequencer_connection)
                   values
                   ($singleRowLockValue, ${configuration.connection})"""
            case _: DbStorage.Profile.Postgres =>
              sqlu"""insert into domain_sequencer_config (sequencer_connection)
              values (${configuration.connection})
              on conflict (lock) do update set sequencer_connection = excluded.sequencer_connection"""
            case _: DbStorage.Profile.Oracle =>
              sqlu"""merge into domain_sequencer_config dsc
                      using (
                        select
                          ${configuration.connection} sequencer_connection
                          from dual
                          ) excluded
                      on (dsc."LOCK" = 'X')
                       when matched then
                        update set dsc.sequencer_connection = excluded.sequencer_connection
                       when not matched then
                        insert (sequencer_connection)
                        values (excluded.sequencer_connection)
                     """
          },
          "saveConfiguration",
        )
    )
}
