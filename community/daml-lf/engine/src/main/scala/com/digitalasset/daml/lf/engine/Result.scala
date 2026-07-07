// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine

import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.Ref.*
import com.digitalasset.daml.lf.data.{BackStack, FrontStack, ImmArray}
import com.digitalasset.daml.lf.engine.ResultNeedContract.Response
import com.digitalasset.daml.lf.engine.ResultNeedKey.Response.AuthenticableFatContractInstance
import com.digitalasset.daml.lf.language.Ast.*
import com.digitalasset.daml.lf.transaction.{FatContractInstance, GlobalKey, NeedKeyProgression}
import com.digitalasset.daml.lf.value.Value.*

import scala.annotation.tailrec

/** many operations require to look up packages and contracts. we do this by allowing our functions
  * to pause and resume after the contract has been fetched.
  */
sealed trait Result[+A] extends Product with Serializable {
  def map[B](f: A => B): Result[B] = this match {
    case ResultInterruption(continue, abort) =>
      ResultInterruption(() => continue().map(f), abort)
    case ResultDone(x) => ResultDone(f(x))
    case ResultError(err) => ResultError(err)
    case ResultNeedContract(coid, resume) =>
      ResultNeedContract(coid, response => resume(response).map(f))
    case ResultNeedPackage(pkgId, resume) =>
      ResultNeedPackage(pkgId, mbPkg => resume(mbPkg).map(f))
    case ResultNeedKey(gk, limit, token, resume) =>
      ResultNeedKey(gk, limit, token, response => resume(response).map(f))
    case ResultPrefetch(contractIds, keys, resume) =>
      ResultPrefetch(contractIds, keys, () => resume().map(f))
    case ResultNeedExternalCall(extId, funcId, configHash, input, resume) =>
      ResultNeedExternalCall(extId, funcId, configHash, input, result => resume(result).map(f))
  }

  def flatMap[B](f: A => Result[B]): Result[B] = this match {
    case ResultInterruption(continue, abort) =>
      ResultInterruption(() => continue().flatMap(f), abort)
    case ResultDone(x) => f(x)
    case ResultError(err) => ResultError(err)
    case ResultNeedContract(coid, resume) =>
      ResultNeedContract(coid, response => resume(response).flatMap(f))
    case ResultNeedPackage(pkgId, resume) =>
      ResultNeedPackage(pkgId, mbPkg => resume(mbPkg).flatMap(f))
    case ResultNeedKey(gk, limit, token, resume) =>
      ResultNeedKey(
        gk,
        limit,
        token,
        response => resume(response).flatMap(f),
      )
    case ResultPrefetch(contractIds, keys, resume) =>
      ResultPrefetch(contractIds, keys, () => resume().flatMap(f))
    case ResultNeedExternalCall(extId, funcId, configHash, input, resume) =>
      ResultNeedExternalCall(extId, funcId, configHash, input, result => resume(result).flatMap(f))
  }

  private[lf] def consume(
      pcs: PartialFunction[ContractId, FatContractInstance] = PartialFunction.empty,
      pkgs: PartialFunction[PackageId, Package] = PartialFunction.empty,
      keys: PartialFunction[GlobalKey, Vector[FatContractInstance]] = PartialFunction.empty,
      hashingMethod: ContractId => Hash.HashingMethod = _ => Hash.HashingMethod.TypedNormalForm,
      idValidator: (ContractId, Hash) => Boolean = (_, _) => true,
  ): Either[Error, A] = {

    @tailrec
    def go(res: Result[A]): Either[Error, A] =
      res match {
        case ResultDone(x) => Right(x)
        case ResultInterruption(continue, _) => go(continue())
        case ResultError(err) => Left(err)
        case ResultNeedContract(acoid, resume) =>
          go(resume(pcs.lift(acoid) match {
            case None => ResultNeedContract.Response.ContractNotFound
            case Some(coInst) =>
              Response.ContractFound(coInst, hashingMethod(acoid), idValidator(acoid, _))
          }))
        case ResultNeedPackage(pkgId, resume) => go(resume(pkgs.lift(pkgId)))
        case ResultNeedKey(key, _, _, resume) =>
          go(
            resume(
              ResultNeedKey.Response(
                keys
                  .lift(key)
                  .getOrElse(Vector.empty)
                  .map(fci =>
                    AuthenticableFatContractInstance(
                      fci,
                      hashingMethod(fci.contractId),
                      hash => idValidator(fci.contractId, hash),
                    )
                  ),
                NeedKeyProgression.Finished,
              )
            )
          )

        case ResultPrefetch(_, _, result) => go(result())
        case ResultNeedExternalCall(extId, funcId, _, _, _) =>
          Left(
            Error.Interpretation(
              Error.Interpretation.Internal(
                "Result.consume",
                s"Result.consume cannot handle ResultNeedExternalCall " +
                  s"(extensionId=$extId, functionId=$funcId)",
                None,
              ),
              None,
            )
          )
      }
    go(this)
  }
}

final case class ResultInterruption[A](continue: () => Result[A], abort: () => Option[String])
    extends Result[A]

/** Indicates that the command (re)interpretation was successful.
  */
final case class ResultDone[A](result: A) extends Result[A]
object ResultDone {
  private[engine] val Unit: ResultDone[Unit] = new ResultDone(())
  private[engine] val None: ResultDone[scala.None.type] = new ResultDone(scala.None)
}

/** Indicates that the command (re)interpretation has failed.
  */
final case class ResultError(err: Error) extends Result[Nothing]
object ResultError {
  def apply(packageError: Error.Package.Error): ResultError =
    ResultError(Error.Package(packageError))
  def apply(preprocessingError: Error.Preprocessing.Error): ResultError =
    ResultError(Error.Preprocessing(preprocessingError))
  def apply(
      interpretationError: Error.Interpretation.Error,
      details: Option[String] = None,
  ): ResultError =
    ResultError(Error.Interpretation(interpretationError, details))
  def apply(validationError: Error.Validation.Error): ResultError =
    ResultError(Error.Validation(validationError))
}

/** Intermediate result indicating that a [[ResultNeedContract.Response]] is required to complete
  * the computation. To resume the computation, the caller must invoke `resume` with the following
  * argument:
  *   - `ContractFound(...)`, if the caller can dereference `coid` to a fat contract instance in the
  *     contract store or in the command's explicit disclosures.
  *   - `NotFound`, if the caller is unable to dereference `coid`
  *   - `UnsupportedContractIdVersion` if the caller is able to dereference `coid` but the resulting
  *     contract's id is malformed or uses an unsupported version.
  *
  * In the `ContractFound` case, the caller of `resume` must provide the following information:
  *   - `contractInstance`: The fat contract instance that has previously been associated with
  *     `coid` by the engine. The caller must not / cannot authenticate or validate the
  *     FatContractInstance aside from extracting the hashing scheme version from its contract ID.
  *   - `expectedHashingMethod`: The hashing method that the engine expects the engine to use for
  *     authenticating the contract instance.
  *   - `idValidator`: A function that authenticates the contract given a hash of the contract
  *     instance computed by the engine using the `expectedHashingMethod`.
  */
final case class ResultNeedContract[A](
    coid: ContractId,
    resume: ResultNeedContract.Response => Result[A],
) extends Result[A]

object ResultNeedContract {
  sealed trait Response

  object Response {
    final case class ContractFound(
        contractInstance: FatContractInstance,
        expectedHashingMethod: Hash.HashingMethod,
        idValidator: Hash => Boolean,
    ) extends Response

    final case object ContractNotFound extends Response

    final case object UnsupportedContractIdVersion extends Response
  }
}

/** Intermediate result indicating that a [[com.digitalasset.daml.lf.language.Ast.Package]] is
  * required to complete the computation. To resume the computation, the caller must invoke `resume`
  * with the following argument: <ul> <li>`Some(package)`, if the caller can dereference `packageId`
  * to `package`</li> <li>`None`, if the caller is unable to dereference `packageId`</li> </ul>
  *
  * It depends on the engine configuration whether the engine will validate the package provided to
  * `resume`. If validation is switched off, it is the callers responsibility to provide a valid
  * package corresponding to `packageId`.
  */
final case class ResultNeedPackage[A](packageId: PackageId, resume: Option[Package] => Result[A])
    extends Result[A]

/** Intermediate result indicating that contracts matching a key are required to complete the
  * computation. To resume the computation, the caller must invoke `resume` with a page containting
  * the following information: <ul> <li>`contracts`: a vector of authenticable fat contract
  * instances whose key matches `key`. `limit` is a hint for the preferred page size; the caller may
  * return more than `limit` entries and the engine will buffer the overflow internally. If no
  * contracts match, an empty vector should be provided.</li> <li>`hasStarted`: a
  * [[transaction.NeedKeyProgression.HasStarted]] token indicating the progression state. Use
  * [[transaction.NeedKeyProgression.Finished]] if all matching contracts have been returned. Use
  * [[transaction.NeedKeyProgression.InProgress]] if there may be more results; the token will be
  * passed back as `continuationToken` in a subsequent `ResultNeedKey` to fetch the next page.</li>
  * </ul>
  *
  * When `continuationToken` is [[transaction.NeedKeyProgression.InProgress]], the caller should
  * resume from where the previous query left off. When it is
  * [[transaction.NeedKeyProgression.Unstarted]], this is the initial query for the given key.
  *
  * Note that we only look up a key and not a key + its maintainers. If the indexer were to index
  * contracts by keys + maintainers, and an invalid package upgrade were to change the maintainers
  * expression in an incompatible way, then when looking up a v2 key, we'd get a negative lookup
  * instead of an upgrade error.
  */
final case class ResultNeedKey[A](
    key: GlobalKey,
    limit: Int,
    continuationToken: NeedKeyProgression.CanContinue,
    resume: ResultNeedKey.Response => Result[A],
) extends Result[A]

object ResultNeedKey {

  object Response {

    /** An entry in a [[Response]] result: either an authenticable contract or an error. */
    sealed trait ContractEntry extends Product with Serializable

    /** A fat contract instance and the necessary information to authenticate it.
      *
      * @param contractInstance
      *   a fat contract instance whose key matches the requested key.
      * @param expectedHashingMethod
      *   the hashing method that the engine expects the engine to use for authenticating the
      *   contract instance.
      * @param idValidator
      *   a function that authenticates the contract given a hash of the contract instance computed
      *   by the engine using the `expectedHashingMethod`.
      */
    final case class AuthenticableFatContractInstance(
        contractInstance: FatContractInstance,
        expectedHashingMethod: Hash.HashingMethod,
        idValidator: Hash => Boolean,
    ) extends ContractEntry

    /** Indicates that the contract ID uses an unsupported version. */
    final case class UnsupportedContractIdVersion(contractId: ContractId) extends ContractEntry
  }

  /** The response to the [[ResultNeedKey]] question.
    *
    * @param contracts
    *   a vector of contract entries whose key matches the requested key. Each entry is either an
    *   [[Response.AuthenticableFatContractInstance]] or an
    *   [[Response.UnsupportedContractIdVersion]].
    * @param hasStarted
    *   a token indicating the progression state: [[transaction.NeedKeyProgression.Finished]] if all
    *   matching contracts have been returned, [[transaction.NeedKeyProgression.InProgress]] if
    *   there may be more results.
    */
  final case class Response(
      contracts: Vector[Response.ContractEntry],
      hasStarted: NeedKeyProgression.HasStarted,
  )
}

/** Indicates that the interpretation will likely need to resolve the given contract keys. The
  * caller may resolve the keys in parallel to the interpretation, but does not have to. The keys
  * map associates each key with the maximum number of contracts to prefetch for it.
  */
final case class ResultPrefetch[A](
    contractIds: Seq[ContractId],
    keys: Map[GlobalKey, Int],
    resume: () => Result[A],
) extends Result[A]

/** Indicates that the host must provide an external-call result to complete the computation. The
  * request fields use canonical lowercase hexadecimal encoding. To resume a successful external
  * call, the host must provide the output using the same canonical encoding.
  *
  * To resume the computation, the caller must invoke `resume` with either:
  *   - Right(output) if the external call succeeded with canonical lowercase hexadecimal output
  *   - Left(error) if the host failed to complete the external call
  *
  * @param extensionId
  *   Identifier of the configured extension
  * @param functionId
  *   Function identifier within the extension
  * @param configHash
  *   Configuration hash as canonical lowercase hex
  * @param input
  *   Input data as canonical lowercase hex
  * @param resume
  *   Callback to provide the result or error
  */
final case class ResultNeedExternalCall[A](
    extensionId: String,
    functionId: String,
    configHash: String,
    input: String,
    resume: Either[ResultNeedExternalCall.Error, String] => Result[A],
) extends Result[A]

object ResultNeedExternalCall {

  /** Error information from external call failures */
  final case class Error(message: String)
}

object Result {

  val Unit: ResultDone[Unit] = ResultDone.Unit

  // fails with ResultError if the package is not found
  private[lf] def needPackage[A](
      packageId: PackageId,
      context: language.Reference,
      resume: Package => Result[A],
  ) =
    ResultNeedPackage(
      packageId,
      {
        case Some(pkg) => resume(pkg)
        case None => ResultError(Error.Package.MissingPackage(packageId, context))
      },
    )

  private[lf] def needContract[A](
      acoid: ContractId,
      resume: (FatContractInstance, Hash.HashingMethod, Hash => Boolean) => Result[A],
  ) = {
    import ResultNeedContract.Response.*
    ResultNeedContract(
      acoid,
      {
        case ContractFound(contractInstance, expectedHashingMethod, authenticator) =>
          resume(contractInstance, expectedHashingMethod, authenticator)
        case ContractNotFound =>
          ResultError(
            Error.Interpretation.DamlException(interpretation.Error.ContractNotFound(acoid))
          )
        case UnsupportedContractIdVersion =>
          ResultError(
            Error.Interpretation.DamlException(interpretation.Error.UnsupportedContractId(acoid))
          )
      },
    )
  }

  def sequence[A](results0: FrontStack[Result[A]]): Result[ImmArray[A]] = {
    @tailrec
    def go(okResults: BackStack[A], results: FrontStack[Result[A]]): Result[BackStack[A]] =
      results.pop match {
        case None => ResultDone(okResults)
        case Some((res, results_)) =>
          res match {
            case ResultDone(x) => go(okResults :+ x, results_)
            case ResultError(err) => ResultError(err)
            case ResultNeedPackage(packageId, resume) =>
              ResultNeedPackage(
                packageId,
                pkg =>
                  resume(pkg).flatMap(x =>
                    Result
                      .sequence(results_)
                      .map(otherResults => (okResults :+ x) :++ otherResults)
                  ),
              )
            case ResultNeedContract(coid, resume) =>
              ResultNeedContract(
                coid,
                response =>
                  resume(response).flatMap(x =>
                    Result
                      .sequence(results_)
                      .map(otherResults => (okResults :+ x) :++ otherResults)
                  ),
              )
            case ResultNeedKey(gk, limit, token, resume) =>
              ResultNeedKey(
                gk,
                limit,
                token,
                response =>
                  resume(response).flatMap(x =>
                    Result
                      .sequence(results_)
                      .map(otherResults => (okResults :+ x) :++ otherResults)
                  ),
              )
            case ResultInterruption(continue, abort) =>
              ResultInterruption(
                () =>
                  continue().flatMap(x =>
                    Result
                      .sequence(results_)
                      .map(otherResults => (okResults :+ x) :++ otherResults)
                  ),
                abort,
              )
            case ResultPrefetch(contractIds, keys, resume) =>
              ResultPrefetch(
                contractIds,
                keys,
                () =>
                  resume().flatMap(x =>
                    Result.sequence(results_).map(otherResults => (okResults :+ x) :++ otherResults)
                  ),
              )
            case ResultNeedExternalCall(extId, funcId, configHash, input, resume) =>
              ResultNeedExternalCall(
                extId,
                funcId,
                configHash,
                input,
                result =>
                  resume(result).flatMap(x =>
                    Result.sequence(results_).map(otherResults => (okResults :+ x) :++ otherResults)
                  ),
              )
          }
      }
    go(BackStack.empty, results0).map(_.toImmArray)
  }

  def assert(assertion: Boolean)(err: Error): Result[Unit] =
    if (assertion)
      Result.Unit
    else
      ResultError(err)
}
