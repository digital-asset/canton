// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import akka.NotUsed
import akka.http.scaladsl.model.StatusCodes
import akka.stream.scaladsl.*
import akka.stream.Materializer
import com.daml.lf
import com.daml.http.LedgerClientJwt.Terminates
import com.daml.http.domain.{ContractTypeId, GetActiveContractsRequest, JwtPayload}
import com.daml.http.json.JsonProtocol.LfValueCodec
import com.daml.http.query.ValuePredicate
import com.daml.fetchcontracts.util.{AbsoluteBookmark, ContractStreamStep, InsertDeleteStep}
import util.{ApiValueToLfValueConverter, toLedgerId}
import com.daml.fetchcontracts.AcsTxStreams.transactionFilter
import com.daml.fetchcontracts.util.ContractStreamStep.{Acs, LiveBegin}
import com.daml.fetchcontracts.util.GraphExtensions.*
import com.daml.http.metrics.HttpApiMetrics
import com.daml.http.util.FutureUtil.toFuture
import com.daml.http.util.Logging.{InstanceUUID, RequestID}
import com.daml.jwt.domain.Jwt
import com.daml.ledger.api.v1.active_contracts_service.GetActiveContractsResponse
import com.daml.ledger.api.v1 as api
import com.daml.logging.LoggingContextOf
import com.daml.metrics.Timed
import com.daml.nonempty.NonEmpty
import com.daml.scalautil.ExceptionOps.*
import com.daml.nonempty.NonEmptyReturningOps.*
import scalaz.std.option.*
import scalaz.syntax.show.*
import scalaz.syntax.std.option.*
import scalaz.syntax.traverse.*
import scalaz.{-\/, OptionT, Show, \/, \/-}
import spray.json.JsValue

import scala.concurrent.{ExecutionContext, Future}
import com.digitalasset.canton.ledger.api.domain as LedgerApiDomain
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.NoTracing
import scalaz.std.scalaFuture.*

class ContractsService(
    resolveContractTypeId: PackageService.ResolveContractTypeId,
    allTemplateIds: PackageService.AllTemplateIds,
    getActiveContracts: LedgerClientJwt.GetActiveContracts,
    getCreatesAndArchivesSince: LedgerClientJwt.GetCreatesAndArchivesSince,
    lookupType: query.ValuePredicate.TypeLookup,
    val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext, mat: Materializer)
    extends NamedLogging
    with NoTracing {
  import ContractsService.*

  def resolveContractReference(
      jwt: Jwt,
      parties: domain.PartySet,
      contractLocator: domain.ContractLocator[LfValue],
      ledgerId: LedgerApiDomain.LedgerId,
  )(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      metrics: HttpApiMetrics,
  ): Future[Option[domain.ResolvedContractRef[LfValue]]] =
    contractLocator match {
      case domain.EnrichedContractKey(templateId, key) =>
        resolveContractTypeId(jwt, ledgerId)(templateId).map(
          _.toOption.flatten.map(x => -\/(x -> key))
        )
      case domain.EnrichedContractId(Some(templateId), contractId) =>
        resolveContractTypeId(jwt, ledgerId)(templateId).map(
          _.toOption.flatten.map(x => \/-(x -> contractId))
        )
      case domain.EnrichedContractId(None, contractId) =>
        findByContractId(jwt, parties, None, ledgerId, contractId)
          .map(_.map(a => \/-(a.templateId -> a.contractId)))
    }

  def lookup(
      jwt: Jwt,
      jwtPayload: JwtPayload,
      req: domain.FetchRequest[LfValue],
  )(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      metrics: HttpApiMetrics,
  ): Future[Option[domain.ActiveContract.ResolvedCtTyId[JsValue]]] = {
    val ledgerId = toLedgerId(jwtPayload.ledgerId)
    val readAs = req.readAs.cata(_.toSet1, jwtPayload.parties)
    req.locator match {
      case domain.EnrichedContractKey(templateId, contractKey) =>
        findByContractKey(jwt, readAs, templateId, ledgerId, contractKey)
      case domain.EnrichedContractId(templateId, contractId) =>
        findByContractId(
          jwt,
          readAs,
          templateId,
          ledgerId,
          contractId,
        )
    }
  }

  private[this] def findByContractKey(
      jwt: Jwt,
      parties: domain.PartySet,
      templateId: ContractTypeId.Template.OptionalPkg,
      ledgerId: LedgerApiDomain.LedgerId,
      contractKey: LfValue,
  )(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      metrics: HttpApiMetrics,
  ): Future[Option[domain.ActiveContract.ResolvedCtTyId[JsValue]]] = {
    Timed.future(
      metrics.dbFindByContractKey,
      search.toFinal
        .findByContractKey(
          SearchContext(jwt, parties, templateId, ledgerId),
          contractKey,
        ),
    )
  }

  private[this] def findByContractId(
      jwt: Jwt,
      parties: domain.PartySet,
      templateId: Option[domain.ContractTypeId.OptionalPkg],
      ledgerId: LedgerApiDomain.LedgerId,
      contractId: domain.ContractId,
  )(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      metrics: HttpApiMetrics,
  ): Future[Option[domain.ActiveContract.ResolvedCtTyId[JsValue]]] = {
    Timed.future(
      metrics.dbFindByContractId,
      search.toFinal.findByContractId(SearchContext(jwt, parties, templateId, ledgerId), contractId),
    )
  }

  private[this] def search: Search = SearchInMemory

  private object SearchInMemory extends Search {
    type LfV = LfValue
    override val lfvToJsValue = SearchValueFormat(lfValueToJsValue)

    override def findByContractKey(
        ctx: SearchContext.Key,
        contractKey: LfValue,
    )(implicit
        lc: LoggingContextOf[InstanceUUID with RequestID],
        metrics: HttpApiMetrics,
    ): Future[Option[domain.ActiveContract.ResolvedCtTyId[LfValue]]] = {
      import ctx.{jwt, parties, templateIds as templateId, ledgerId}
      for {
        resolvedTemplateId <- OptionT(
          resolveContractTypeId(jwt, ledgerId)(templateId)
            .map(
              _.toOption.flatten
            )
        )

        predicate = domain.ActiveContract.matchesKey(contractKey) _

        result <- OptionT(
          searchInMemoryOneTpId(jwt, ledgerId, parties, resolvedTemplateId, predicate)
            .runWith(Sink.headOption)
            .flatMap(lookupResult)
        )

      } yield result
    }.run

    override def findByContractId(
        ctx: SearchContext.ById,
        contractId: domain.ContractId,
    )(implicit
        lc: LoggingContextOf[InstanceUUID with RequestID],
        metrics: HttpApiMetrics,
    ): Future[Option[domain.ActiveContract.ResolvedCtTyId[LfValue]]] = {
      import ctx.{jwt, parties, templateIds as templateId, ledgerId}
      for {

        resolvedTemplateIds <- OptionT(
          templateId.cata(
            x =>
              resolveContractTypeId(jwt, ledgerId)(x)
                .map(_.toOption.flatten.map(Set(_))),
            // ignoring interface IDs for all-templates query
            allTemplateIds(lc)(jwt, ledgerId).map(_.toSet[domain.ContractTypeId.Resolved].some),
          )
        )
        resolvedQuery <- OptionT(
          Future.successful(
            domain
              .ResolvedQuery(resolvedTemplateIds)
              .toOption
          )
        )
        result <- OptionT(
          searchInMemory(
            jwt,
            ledgerId,
            parties,
            resolvedQuery,
            InMemoryQuery.Filter(isContractId(contractId)),
          )
            .runWith(Sink.headOption)
            .flatMap(lookupResult)
        )

      } yield result
    }.run

    override def search(ctx: SearchContext.QueryLang, queryParams: Map[String, JsValue])(implicit
        lc: LoggingContextOf[InstanceUUID with RequestID],
        metrics: HttpApiMetrics,
    ) = {
      import ctx.{jwt, parties, templateIds, ledgerId}
      searchInMemory(
        jwt,
        ledgerId,
        parties,
        templateIds,
        InMemoryQuery.Params(queryParams),
      )
    }
  }

  private def lookupResult(
      errorOrAc: Option[Error \/ domain.ActiveContract.ResolvedCtTyId[LfValue]]
  ): Future[Option[domain.ActiveContract.ResolvedCtTyId[LfValue]]] =
    errorOrAc traverse (toFuture(_))

  private def isContractId(k: domain.ContractId)(
      a: domain.ActiveContract.ResolvedCtTyId[LfValue]
  ): Boolean =
    (a.contractId: domain.ContractId) == k

  def retrieveAll(
      jwt: Jwt,
      jwtPayload: JwtPayload,
  )(implicit
      lc: LoggingContextOf[InstanceUUID]
  ): SearchResult[Error \/ domain.ActiveContract.ResolvedCtTyId[LfValue]] =
    retrieveAll(jwt, toLedgerId(jwtPayload.ledgerId), jwtPayload.parties)

  def retrieveAll(
      jwt: Jwt,
      ledgerId: LedgerApiDomain.LedgerId,
      parties: domain.PartySet,
  )(implicit
      lc: LoggingContextOf[InstanceUUID]
  ): SearchResult[Error \/ domain.ActiveContract.ResolvedCtTyId[LfValue]] =
    domain.OkResponse(
      Source
        .future(allTemplateIds(lc)(jwt, ledgerId))
        .flatMapConcat(x =>
          Source(x)
            .flatMapConcat(x => searchInMemoryOneTpId(jwt, ledgerId, parties, x, _ => true))
        )
    )

  def search(
      jwt: Jwt,
      jwtPayload: JwtPayload,
      request: GetActiveContractsRequest,
  )(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      metrics: HttpApiMetrics,
  ): Future[SearchResult[Error \/ domain.ActiveContract.ResolvedCtTyId[JsValue]]] =
    search(
      jwt,
      toLedgerId(jwtPayload.ledgerId),
      request.readAs.cata((_.toSet1), jwtPayload.parties),
      request.templateIds,
      request.query,
    )

  def search(
      jwt: Jwt,
      ledgerId: LedgerApiDomain.LedgerId,
      parties: domain.PartySet,
      templateIds: NonEmpty[Set[domain.ContractTypeId.OptionalPkg]],
      queryParams: Map[String, JsValue],
  )(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      metrics: HttpApiMetrics,
  ): Future[SearchResult[Error \/ domain.ActiveContract.ResolvedCtTyId[JsValue]]] = for {
    res <- resolveContractTypeIds(jwt, ledgerId)(templateIds)
    (resolvedContractTypeIds, unresolvedContractTypeIds) = res

    warnings: Option[domain.UnknownTemplateIds] =
      if (unresolvedContractTypeIds.isEmpty) None
      else Some(domain.UnknownTemplateIds(unresolvedContractTypeIds.toList))
  } yield {
    domain
      .ResolvedQuery(resolvedContractTypeIds)
      .leftMap(handleResolvedQueryErrors(warnings))
      .map { resolvedQuery =>
        val searchCtx = SearchContext(jwt, parties, resolvedQuery, ledgerId)
        val source = search.toFinal.search(searchCtx, queryParams)
        domain.OkResponse(source, warnings)
      }
      .merge
  }
  private def handleResolvedQueryErrors(
      warnings: Option[domain.UnknownTemplateIds]
  ): domain.ResolvedQuery.Unsupported => domain.ErrorResponse = unsuppoerted =>
    mkErrorResponse(unsuppoerted.errorMsg, warnings)

  private def mkErrorResponse(errorMessage: String, warnings: Option[domain.UnknownTemplateIds]) =
    domain.ErrorResponse(
      errors = List(errorMessage),
      warnings = warnings,
      status = StatusCodes.BadRequest,
    )

  private[this] def searchInMemory(
      jwt: Jwt,
      ledgerId: LedgerApiDomain.LedgerId,
      parties: domain.PartySet,
      resolvedQuery: domain.ResolvedQuery,
      queryParams: InMemoryQuery,
  )(implicit
      lc: LoggingContextOf[InstanceUUID]
  ): Source[InternalError \/ domain.ActiveContract.ResolvedCtTyId[LfValue], NotUsed] = {
    val templateIds = resolvedQuery.resolved
    logger.debug(
      s"Searching in memory, parties: $parties, templateIds: $templateIds, queryParms: $queryParams, ${lc.makeString}"
    )

    type Ac = domain.ActiveContract.ResolvedCtTyId[LfValue]
    val empty = (Vector.empty[Error], Vector.empty[Ac])
    import InsertDeleteStep.appendForgettingDeletes

    val funPredicates: Map[domain.ContractTypeId.RequiredPkg, Ac => Boolean] =
      templateIds.iterator.map(tid => (tid, queryParams.toPredicate(tid))).toMap

    insertDeleteStepSource(jwt, ledgerId, parties, templateIds.toList)
      .map { step =>
        val (errors, converted) = step.toInsertDelete.partitionMapPreservingIds { apiEvent =>
          domain.ActiveContract
            .fromLedgerApi(resolvedQuery, apiEvent)
            .leftMap(e => InternalError(Symbol("searchInMemory"), e.shows))
            .flatMap(apiAcToLfAc): Error \/ Ac
        }
        val convertedInserts = converted.inserts filter { ac =>
          funPredicates.get(ac.templateId).exists(_(ac))
        }
        (errors, converted.copy(inserts = convertedInserts))
      }
      .fold(empty) { case ((errL, stepL), (errR, stepR)) =>
        (errL ++ errR, appendForgettingDeletes(stepL, stepR))
      }
      .mapConcat { case (err, inserts) =>
        inserts.map(\/-(_)) ++ err.map(-\/(_))
      }
  }

  private[this] def searchInMemoryOneTpId(
      jwt: Jwt,
      ledgerId: LedgerApiDomain.LedgerId,
      parties: domain.PartySet,
      templateId: domain.ContractTypeId.Resolved,
      queryParams: InMemoryQuery.P,
  )(implicit
      lc: LoggingContextOf[InstanceUUID]
  ): Source[Error \/ domain.ActiveContract.ResolvedCtTyId[LfValue], NotUsed] = {
    val resolvedQuery = domain.ResolvedQuery(templateId)
    searchInMemory(jwt, ledgerId, parties, resolvedQuery, InMemoryQuery.Filter(queryParams))
  }

  private[this] sealed abstract class InMemoryQuery extends Product with Serializable {
    import InMemoryQuery.*
    def toPredicate(tid: domain.ContractTypeId.RequiredPkg): P =
      this match {
        case Params(q) =>
          val vp = valuePredicate(tid, q).toFunPredicate
          ac => vp(ac.payload)
        case Filter(p) => p
      }
  }

  private[this] object InMemoryQuery {
    type P = domain.ActiveContract.ResolvedCtTyId[LfValue] => Boolean
    sealed case class Params(params: Map[String, JsValue]) extends InMemoryQuery
    sealed case class Filter(p: P) extends InMemoryQuery
  }

  def liveAcsAsInsertDeleteStepSource(
      jwt: Jwt,
      ledgerId: LedgerApiDomain.LedgerId,
      parties: domain.PartySet,
      templateIds: List[domain.ContractTypeId.Resolved],
  )(implicit lc: LoggingContextOf[InstanceUUID]): Source[ContractStreamStep.LAV1, NotUsed] = {
    val txnFilter = transactionFilter(parties, templateIds)
    getActiveContracts(jwt, ledgerId, txnFilter, true)(lc)
      .map { case GetActiveContractsResponse(offset, _, activeContracts) =>
        if (activeContracts.nonEmpty) Acs(activeContracts.toVector)
        else LiveBegin(AbsoluteBookmark(domain.Offset(offset)))
      }
  }

  /** An ACS ++ transaction stream of `templateIds`, starting at `startOffset`
    * and ending at `terminates`.
    */
  def insertDeleteStepSource(
      jwt: Jwt,
      ledgerId: LedgerApiDomain.LedgerId,
      parties: domain.PartySet,
      templateIds: List[domain.ContractTypeId.Resolved],
      startOffset: Option[domain.StartingOffset] = None,
      terminates: Terminates = Terminates.AtLedgerEnd,
  )(implicit
      lc: LoggingContextOf[InstanceUUID]
  ): Source[ContractStreamStep.LAV1, NotUsed] = {

    val txnFilter = transactionFilter(parties, templateIds)
    def source =
      (getActiveContracts(jwt, ledgerId, txnFilter, true)(lc)
        via logTermination(logger, "ACS upstream"))

    val transactionsSince
        : api.ledger_offset.LedgerOffset => Source[api.transaction.Transaction, NotUsed] =
      getCreatesAndArchivesSince(
        jwt,
        ledgerId,
        txnFilter,
        _: api.ledger_offset.LedgerOffset,
        terminates,
      )(lc) via logTermination(logger, "transactions upstream")

    import com.daml.fetchcontracts.AcsTxStreams.{
      acsFollowingAndBoundary,
      transactionsFollowingBoundary,
    }, com.daml.fetchcontracts.util.GraphExtensions.*
    val contractsAndBoundary = startOffset
      .cata(
        so =>
          Source
            .single(AbsoluteBookmark(so.offset))
            .viaMat(transactionsFollowingBoundary(transactionsSince, logger).divertToHead)(
              Keep.right
            ),
        source.viaMat(acsFollowingAndBoundary(transactionsSince, logger).divertToHead)(
          Keep.right
        ),
      )
      .via(logTermination(logger, "ACS+tx or tx stream"))
    contractsAndBoundary mapMaterializedValue { fob =>
      fob.foreach(a =>
        logger.debug(s"contracts fetch completed at: ${a.toString}, ${lc.makeString}")
      )
      NotUsed
    }
  }

  private def apiAcToLfAc(
      ac: domain.ActiveContract.ResolvedCtTyId[ApiValue]
  ): Error \/ domain.ActiveContract.ResolvedCtTyId[LfValue] =
    ac.traverse(ApiValueToLfValueConverter.apiValueToLfValue)
      .leftMap(e => InternalError(Symbol("apiAcToLfAc"), e.shows))

  def valuePredicate(
      templateId: domain.ContractTypeId.RequiredPkg,
      q: Map[String, JsValue],
  ): query.ValuePredicate =
    ValuePredicate.fromTemplateJsObject(q, templateId, lookupType)

  private def lfValueToJsValue(a: LfValue): Error \/ JsValue =
    \/.attempt(LfValueCodec.apiValueToJsValue(a))(e =>
      InternalError(Symbol("lfValueToJsValue"), e.description)
    )

  def resolveContractTypeIds[Tid <: domain.ContractTypeId.OptionalPkg](
      jwt: Jwt,
      ledgerId: LedgerApiDomain.LedgerId,
  )(
      xs: NonEmpty[Set[Tid]]
  )(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): Future[(Set[domain.ContractTypeId.Resolved], Set[Tid])] = {
    import scalaz.syntax.traverse.*
    import scalaz.std.list.*, scalaz.std.scalaFuture.*

    xs.toList.toNEF
      .traverse { x =>
        resolveContractTypeId(jwt, ledgerId)(x)
          .map(_.toOption.flatten.toLeft(x)): Future[
          Either[domain.ContractTypeId.Resolved, Tid]
        ]
      }
      .map(_.toSet.partitionMap(a => a))
  }
}

object ContractsService {
  private type ApiValue = api.value.Value

  private type LfValue = lf.value.Value

  private final case class SearchValueFormat[-T](encode: T => (Error \/ JsValue))

  private final case class SearchContext[+TpIds](
      jwt: Jwt,
      parties: domain.PartySet,
      templateIds: TpIds,
      ledgerId: LedgerApiDomain.LedgerId,
  )

  private object SearchContext {

    type QueryLang = SearchContext[
      domain.ResolvedQuery
    ]
    type ById = SearchContext[Option[domain.ContractTypeId.OptionalPkg]]
    type Key = SearchContext[domain.ContractTypeId.Template.OptionalPkg]
  }

  // A prototypical abstraction over the in-memory/in-DB split, accounting for
  // the fact that in-memory works with ADT-encoded LF values,
  // whereas in-DB works with JsValues
  private sealed abstract class Search { self =>
    type LfV
    val lfvToJsValue: SearchValueFormat[LfV]

    final def toFinal(implicit
        ec: ExecutionContext
    ): Search { type LfV = JsValue } = {
      val SearchValueFormat(convert) = lfvToJsValue
      new Search {
        type LfV = JsValue
        override val lfvToJsValue = SearchValueFormat(\/.right)

        override def findByContractId(
            ctx: SearchContext.ById,
            contractId: domain.ContractId,
        )(implicit
            lc: LoggingContextOf[InstanceUUID with RequestID],
            metrics: HttpApiMetrics,
        ): Future[Option[domain.ActiveContract.ResolvedCtTyId[LfV]]] =
          self
            .findByContractId(ctx, contractId)
            .flatMap(oac => toFuture(oac traverse (_ traverse convert)))

        override def findByContractKey(
            ctx: SearchContext.Key,
            contractKey: LfValue,
        )(implicit
            lc: LoggingContextOf[InstanceUUID with RequestID],
            metrics: HttpApiMetrics,
        ): Future[Option[domain.ActiveContract.ResolvedCtTyId[LfV]]] =
          self
            .findByContractKey(ctx, contractKey)
            .flatMap(oac => toFuture(oac traverse (_ traverse convert)))

        override def search(
            ctx: SearchContext.QueryLang,
            queryParams: Map[String, JsValue],
        )(implicit
            lc: LoggingContextOf[InstanceUUID with RequestID],
            metrics: HttpApiMetrics,
        ): Source[Error \/ domain.ActiveContract.ResolvedCtTyId[LfV], NotUsed] =
          self.search(ctx, queryParams) map (_ flatMap (_ traverse convert))
      }
    }

    def findByContractId(
        ctx: SearchContext.ById,
        contractId: domain.ContractId,
    )(implicit
        lc: LoggingContextOf[InstanceUUID with RequestID],
        metrics: HttpApiMetrics,
    ): Future[Option[domain.ActiveContract.ResolvedCtTyId[LfV]]]

    def findByContractKey(
        ctx: SearchContext.Key,
        contractKey: LfValue,
    )(implicit
        lc: LoggingContextOf[InstanceUUID with RequestID],
        metrics: HttpApiMetrics,
    ): Future[Option[domain.ActiveContract.ResolvedCtTyId[LfV]]]

    def search(
        ctx: SearchContext.QueryLang,
        queryParams: Map[String, JsValue],
    )(implicit
        lc: LoggingContextOf[InstanceUUID with RequestID],
        metrics: HttpApiMetrics,
    ): Source[Error \/ domain.ActiveContract.ResolvedCtTyId[LfV], NotUsed]
  }

  final case class Error(id: Symbol, message: String)
  private type InternalError = Error
  val InternalError: Error.type = Error

  object Error {
    implicit val errorShow: Show[Error] = Show shows { e =>
      s"ContractService Error, ${e.id: Symbol}: ${e.message: String}"
    }
  }

  type SearchResult[A] = domain.SyncResponse[Source[A, NotUsed]]
}
