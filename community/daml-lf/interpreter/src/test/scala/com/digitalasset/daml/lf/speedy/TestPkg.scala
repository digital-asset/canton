// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy

import com.digitalasset.canton.logging.NamedLoggingContext
import com.digitalasset.daml.lf.data.Freer.Step
import com.digitalasset.daml.lf.data.{FrontStack, ImmArray, Ref, Time}
import com.digitalasset.daml.lf.language.{Ast, LanguageVersion}
import com.digitalasset.daml.lf.speedy
import com.digitalasset.daml.lf.speedy.TransactionConductor.Upd
import com.digitalasset.daml.lf.testing.parser.Implicits.SyntaxHelper
import com.digitalasset.daml.lf.testing.parser.ParserParameters
import com.digitalasset.daml.lf.transaction.{
  FatContractInstance,
  GlobalKey,
  NeedKeyProgression,
  SerializationVersion,
}
import com.digitalasset.daml.lf.value.Value

import scala.collection.immutable.ArraySeq

/** Shared Daml-LF package definition and common constants used by both [[EvaluationOrderTest]] and
  * [[AuthorizationTest]].
  */
object TestPkg {
  val packageId: Ref.PackageId = Ref.PackageId.assertFromString("-pkg-")

  object CmdFlow extends data.Freer.Companion {
    sealed class F[A]
    type E = Throwable
    object F {
      case class Submit(cmd: Command) extends F[SValue]
    }
    def submit(cmd: Command): CmdFlow[SValue] = lift(F.Submit(cmd))
  }
  type CmdFlow[X] = CmdFlow.T[X]

  implicit class RecordOps(val record: SValue.SRecord) extends AnyVal {
    def update(field: String, value: SValue): SValue.SRecord = {
      val idx = record.fields.indexWhere(_ == field)
      if (idx < 0) throw new RuntimeException(s"Field $field not found in record $record")
      record.copy(values = record.values.updated(idx, value))
    }
  }
  implicit class CmdFlowOps[X](val flow: this.CmdFlow[X]) extends AnyVal {
    def withFilter(f: X => Boolean): CmdFlow[X] =
      flow.flatMap(x =>
        if (f(x)) CmdFlow.pure(x)
        else CmdFlow.raise(new RuntimeException("Filter failed"))
      )
  }

  lazy val seed = crypto.Hash.hashPrivateKey("seed")

  def asSCid(value: SValue): SValue.SContractId = value match {
    case SValue.SContractId(coid) => SValue.SContractId(coid)
    case _ => throw new RuntimeException(s"Expected SContractId, got $value")
  }

}

class TestPkg(withKey: Boolean, languageVersion: LanguageVersion, cmdMode: Boolean) {
  import TestPkg.packageId

  val serializationVersion: SerializationVersion = SerializationVersion.assign(hasKey = true)

  implicit val parserParameters: ParserParameters[this.type] =
    ParserParameters(packageId, languageVersion = languageVersion)

  val pkg: language.Ast.Package = {
    val ifKey = if (withKey) "    " else "//  "
    p"""  metadata ( 'evaluation-order-test' : '1.0.0' )
      module M {

        record @serializable TKey = { maintainers : List Party, optCid : Option (ContractId Unit), nat: M:Nat };

        variant @serializable Either (a:*) (b:*) = Left: a | Right : b;

        variant @serializable Nat =
          Z : Unit
        | S : M:Nat;

        val intToNat : Int64 -> M:Nat = \(i: Int64) ->
          case (EQUAL @Int64 i 0) of
            True -> M:Nat:Z ()
          | _ -> M:Nat:S (M:intToNat (SUB_INT64 i 1));

        record @serializable IView = { nat: M:Nat };

        interface (this: I) = {
          viewtype M:IView;
          method getCtrls: List Party;
          choice @nonConsuming Choice (self) (arg: M:Either M:Nat Int64): M:Nat
          , controllers TRACE @(List Party) "interface choice controllers" (call_method @M:I getCtrls this)
          , observers TRACE @(List Party) "interface choice observers" (Nil @Party)
          to upure @M:Nat (TRACE @M:Nat "choice body" (case arg of M:Either:Right i -> M:intToNat i | M:Either:Left x -> x));
        };

        record @serializable T = {
          signatory : Party,
          observer : Party,
          maintainers : List Party,
          precondition : Bool,
          input: M:Nat,
          keySize: Int64,
          keyCidOpt: Option (ContractId Unit),
          viewSize: Int64
        };
        template (this: T) = {
          precondition TRACE @Bool "precondition" (M:T {precondition} this);
          signatories TRACE @(List Party) "contract signatories" (Cons @Party [M:T {signatory} this] (Nil @Party));
          observers TRACE @(List Party) "contract observers" (Cons @Party [M:T {observer} this] (Nil @Party));
          choice Choice (self) (arg: M:Either M:Nat Int64) : M:Nat,
            controllers TRACE @(List Party) "template choice controllers" (Cons @Party [M:T {signatory} this] (Nil @Party)),
            observers TRACE @(List Party) "template choice observers" (Nil @Party),
            authorizers TRACE @(List Party) "template choice authorizers" (Cons @Party [M:T {signatory} this] (Nil @Party))
            to upure @M:Nat (TRACE @M:Nat "choice body" (case arg of M:Either:Right i -> M:intToNat i | M:Either:Left x -> x));
          choice Archive (self) (arg: Unit): Unit,
            controllers Cons @Party [M:T {signatory} this] (Nil @Party)
            to upure @Unit (TRACE @Unit "archive" ());
          choice @nonConsuming Divulge (self) (divulgee: Party): Unit,
            controllers Cons @Party [divulgee] (Nil @Party)
            to upure @Unit ();
            implements M:I {
              view = TRACE @M:IView "view" (M:IView { nat = M:intToNat (M:T {viewSize} this) });
              method getCtrls = Cons @Party [M:T {signatory} this] (Nil @Party);
            };
$ifKey    key @M:TKey
$ifKey       (TRACE @M:TKey "key" (M:TKey {
$ifKey          maintainers = M:T {maintainers} this,
$ifKey          optCid = M:T {keyCidOpt} this,
$ifKey          nat = M:intToNat (M:T {keySize} this)
$ifKey        }))
$ifKey       (\(key : M:TKey) -> TRACE @(List Party) "maintainers" (M:TKey {maintainers} key));
        };

        record @serializable Dummy = { signatory : Party };
        template (this: Dummy) = {
          precondition True;
          signatories Cons @Party [M:Dummy {signatory} this] (Nil @Party);
          observers Nil @Party;
          choice Archive (self) (arg: Unit): Unit,
            controllers Cons @Party [M:Dummy {signatory} this] (Nil @Party)
            to upure @Unit ();
        };

      }

  """
  }

  val pkgs: PureCompiledPackages = SpeedyTestLib.typeAndCompile(pkg, cmdMode)

  val packageNameMap: Map[Ref.PackageName, Ref.PackageId] = Map(pkg.pkgName -> packageId)

  val List(alice, bob, charlie): List[Ref.Party] =
    List("alice", "bob", "charlie").map(Ref.Party.assertFromString)

  private def assertTTyCon(typ: Ast.Type): Ref.TypeConId =
    typ match {
      case Ast.TTyCon(tycon) => tycon
      case _ => sys.error("unexpected error")
    }

  val T: Ref.TypeConId = assertTTyCon(t"M:T")
  val I: Ref.TypeConId = assertTTyCon(t"M:I")
  val Dummy: Ref.TypeConId = assertTTyCon(t"M:Dummy")

  val TKey: Ref.TypeConId = assertTTyCon(t"M:TKey")

  object SNat {
    val T: Ref.TypeConId = assertTTyCon(t"M:Nat")
    val Z = Ref.Name.assertFromString("Z")
    val S = Ref.Name.assertFromString("S")
    def fromInt(int: Int): SValue.SVariant =
      if (int == 0)
        SValue.SVariant(T, Z, 0, SValue.SUnit)
      else
        SValue.SVariant(T, S, 1, fromInt(int - 1))
  }

  object SEither {
    val T: Ref.TypeConId = assertTTyCon(t"M:Either")

    def Left(svalue: SValue) =
      SValue.SVariant(T, n"Left", 0, svalue)

    def Right(svalue: SValue) =
      SValue.SVariant(T, n"Right", 1, svalue)
  }

  val cId: Value.ContractId = Value.ContractId.V1(crypto.Hash.hashPrivateKey("test"))
  val cId2: Value.ContractId = Value.ContractId.V1(crypto.Hash.hashPrivateKey("test2"))
  val cId3: Value.ContractId = Value.ContractId.V1(crypto.Hash.hashPrivateKey("test3"))
  val cId4: Value.ContractId = Value.ContractId.V1(crypto.Hash.hashPrivateKey("test4"))
  val cId5: Value.ContractId = Value.ContractId.V1(crypto.Hash.hashPrivateKey("test5"))

  val keySValue: SValue.SRecord = SValue.SRecord(
    TKey,
    ImmArray("maintainers", "optCid", "nat").map(Ref.Name.assertFromString),
    ArraySeq(
      SValue.SList(FrontStack(SValue.SParty(alice))),
      SValue.SOptional(None),
      SNat.fromInt(0),
    ),
  )

  val keyValue: Value.ValueRecord = Value.ValueRecord(
    None,
    ImmArray(
      None -> Value.ValueList(FrontStack(Value.ValueParty(alice))),
      None -> Value.ValueNone,
      None -> SNat.fromInt(0).toNormalizedValue,
    ),
  )

  val normalizedKeyValue: Value.ValueRecord = keyValue

  def payload(signatory: Ref.Party, observer: Ref.Party): SValue.SRecord =
    SValue.SRecord(
      T,
      ImmArray(
        "signatory",
        "observer",
        "maintainers",
        "precondition",
        "input",
        "keySize",
        "keyCidOpt",
        "viewSize",
      ).map(Ref.Name.assertFromString),
      ArraySeq(
        SValue.SParty(signatory),
        SValue.SParty(observer),
        SValue.SList(FrontStack(SValue.SParty(signatory))),
        SValue.SBool(true),
        SNat.fromInt(0),
        SValue.SInt64(0L),
        SValue.SOptional(None),
        SValue.SInt64(0L),
      ),
    )

  val defaultPayload: SValue.SRecord = payload(alice, bob)

}

trait CmdFlowRunner {
  import TestPkg.*
  protected def cmdMode: Boolean
  protected def runCmdFlow(
      pkgs: CompiledPackages,
      setup: CmdFlow[SValue] = CmdFlow.pure(SValue.SUnit),
      test: SValue => CmdFlow[SValue],
      parties: Set[Ref.Party],
      readAs: Set[Ref.Party] = Set.empty,
      packageResolution: Map[Ref.PackageName, Ref.PackageId],
      getContract: PartialFunction[Value.ContractId, FatContractInstance] = PartialFunction.empty,
      getKeys: PartialFunction[GlobalKey, Vector[FatContractInstance]] = PartialFunction.empty,
      authorizationChecker: RecordingMachineLogger => AuthorizationChecker =
        new AuthorizationCheckerLogger(_),
  )(implicit loggingContext: NamedLoggingContext): (Either[Throwable, SValue], Seq[String])
}

// Trait providing UpdateMachine implementation
trait CmdFlowRunnerWithUpdateMachine extends CmdFlowRunner {

  import TestPkg.*

  final override protected def cmdMode: Boolean = false
  final override protected def runCmdFlow(
      pkgs: CompiledPackages,
      setup: CmdFlow[SValue],
      test: SValue => CmdFlow[SValue],
      parties: Set[Ref.Party],
      readAs: Set[Ref.Party],
      packageResolution: Map[Ref.PackageName, Ref.PackageId],
      getContract: PartialFunction[Value.ContractId, FatContractInstance],
      getKeys: PartialFunction[GlobalKey, Vector[FatContractInstance]],
      authorizationChecker: RecordingMachineLogger => AuthorizationChecker,
  )(implicit loggingContext: NamedLoggingContext): (Either[Throwable, SValue], Seq[String]) = {
    val recordingLogger = new RecordingMachineLogger(MachineLogger())
    val machine = Speedy.UpdateMachine(
      compiledPackages = pkgs,
      preparationTime = Time.Timestamp.MinValue,
      initialSeeding = InitialSeeding.TransactionSeed(seed),
      expr = SExpr.SEValue(SValue.SUnit),
      committers = parties,
      readAs = readAs,
      packageResolution = packageResolution,
      limits = interpretation.Limits.Lenient,
      authorizationChecker = authorizationChecker(recordingLogger),
      iterationsBetweenInterruptions = 10000,
      interpretationConfig = interpretation.InterpretationConfig.Default,
      logger = recordingLogger,
    )
    import cats.~>

    val handler = new ~>[CmdFlow.F, Either[Throwable, *]] {
      override def apply[A](fa: CmdFlow.F[A]): Either[Throwable, A] =
        fa match {
          case CmdFlow.F.Submit(cmd) =>
            scala.util
              .Try {
                val se = pkgs.compiler.unsafeCompileCommand(cmd)
                machine.kontStack.keep(0)
                machine.kontStack.push(Speedy.KPure(Speedy.Control.Complete(_)))
                machine.setControl(
                  Speedy.Control.Expression(SExpr.SEApp(se, ArraySeq(SValue.SToken)))
                )
                SpeedyTestLib.run(
                  machine,
                  getContract =
                    recordingLogger.tracePartialFunction("queries contract", getContract),
                  getKeys = recordingLogger.tracePartialFunction("queries key", getKeys),
                )
              }
              .toEither
              .flatten
        }
    }

    setup.consume(handler) match {
      case Left(value) =>
        throw new Error(s"Setup failed with exception: $value")
      case Right(x) =>
        recordingLogger.llTrace("starts test")
        val result = test(x).consume(handler).map { x =>
          recordingLogger.llTrace("ends test")
          x
        }

        result ->
          recordingLogger.recordedMessages.dropWhile(_ != "starts test")
    }
  }
}

// Trait providing TransactionConductor implementation
trait CmdFlowRunnerWithTransactionConductor extends CmdFlowRunner {

  import TestPkg.*

  final override protected def cmdMode: Boolean = true

  final override protected def runCmdFlow(
      pkgs: CompiledPackages,
      setup: CmdFlow[SValue],
      test: SValue => CmdFlow[SValue],
      parties: Set[Ref.Party],
      readAs: Set[Ref.Party],
      packageResolution: Map[Ref.PackageName, Ref.PackageId],
      getContract: PartialFunction[Value.ContractId, FatContractInstance],
      getKeys: PartialFunction[GlobalKey, Vector[FatContractInstance]],
      authorizationChecker: RecordingMachineLogger => AuthorizationChecker,
  )(implicit loggingContext: NamedLoggingContext): (Either[Throwable, SValue], Seq[String]) = {
    val recordingLogger = new RecordingMachineLogger(MachineLogger())
    val conductor = TransactionConductor(
      compiledPackages = pkgs,
      preparationTime = Time.Timestamp.MinValue,
      initialSeeding = InitialSeeding.TransactionSeed(seed),
      committers = parties,
      readAs = readAs,
      packageResolution = packageResolution,
      limits = interpretation.Limits.Lenient,
      authorizationChecker = authorizationChecker(recordingLogger),
      iterationsBetweenInterruptions = 10000,
      interpretationConfig = interpretation.InterpretationConfig.Default,
      logger = recordingLogger,
    )

    def getContract_ = recordingLogger.tracePartialFunction("queries contract", getContract)
    def getKeys_ = recordingLogger.tracePartialFunction("queries key", getKeys)

    def wrapContract[X](contract: X) =
      (contract, crypto.Hash.HashingMethod.TypedNormalForm, (_: crypto.Hash) => true)

    def loop0[X](y: TransactionConductor.Upd.Step[X]): Either[Throwable, X] =
      y match {
        case Step.Pure(value) => Right(value)
        case Step.Error(error) => Left(SError.InterpretationError(error))
        case impure: speedy.TransactionConductor.Upd.Step.Impure[x, X] =>
          impure.fx match {
            case Upd.NeedTime =>
              Left(SpeedyTestLib.UnexpectedSResultNeedTime)
            case Upd.NeedContract(contractId, committers) =>
              getContract_.lift(contractId) match {
                case Some(coinst) =>
                  loop0(impure.resume(wrapContract(coinst)))
                case None =>
                  Left(SpeedyTestLib.UnknownContract(contractId))
              }
            case Upd.NeedPackage(pkg, context) =>
              Left(SpeedyTestLib.UnknownPackage(pkg))
            case Upd.NeedKey(key, n, canContinue, _) =>
              val (returned, hasStarted) =
                NeedKeyProgression.takeN(canContinue, n, getKeys_.lift(key).getOrElse(Vector.empty))
              loop0(impure.resume((returned.map(wrapContract), hasStarted)))
            case Upd.NeedExternalCall(extensionId, functionId, configHash, input) =>
              Left(new RuntimeException("External calls are not supported in this test"))
          }
      }

    def loop[X](y: CmdFlow.Step[X]): Either[Throwable, X] =
      y match {
        case CmdFlow.Step.Pure(value) => Right(value)
        case CmdFlow.Step.Error(error) => Left(error)
        case impure: CmdFlow.Step.Impure[x, X] =>
          val upd = impure.fx match {
            case CmdFlow.F.Submit(cmd) =>
              cmd match {
                case Command.Create(templateId, argument) =>
                  conductor.handleCommand(
                    Question.Cmd.Create(templateId, argument.asInstanceOf[SValue.SRecord])
                  )
                case Command.ExerciseTemplate(templateId, contractId, choiceId, argument) =>
                  conductor.handleCommand(
                    Question.Cmd.ExerciseTemplate(templateId, choiceId, contractId.value, argument)
                  )
                case Command.ExerciseInterface(interfaceId, contractId, choiceId, argument) =>
                  conductor.handleCommand(
                    Question.Cmd
                      .ExerciseInterface(interfaceId, choiceId, contractId.value, argument)
                  )
                case Command.ExerciseByKey(templateId, contractKey, choiceId, argument) =>
                  conductor.handleCommand(
                    Question.Cmd.ExerciseByKey(templateId, choiceId, contractKey, argument)
                  )
                case Command.CreateAndExercise(
                      templateId,
                      createArgument,
                      choiceId,
                      choiceArgument,
                    ) =>
                  for {
                    cid <- conductor.handleCommand(
                      Question.Cmd.Create(templateId, createArgument.asInstanceOf[SValue.SRecord])
                    )
                    res <- conductor.handleCommand(
                      Question.Cmd
                        .ExerciseTemplate(templateId, choiceId, asSCid(cid).value, choiceArgument)
                    )
                  } yield res
                case Command.FetchTemplate(templateId, coid) =>
                  conductor.handleCommand(Question.Cmd.FetchTemplate(templateId, coid.value))
                case Command.FetchInterface(interfaceId, coid) =>
                  conductor.handleCommand(Question.Cmd.FetchInterface(interfaceId, coid.value))
                case Command.FetchByKey(templateId, key) =>
                  conductor.handleCommand(Question.Cmd.FetchByKey(templateId, key))
                case Command.QueryNByKey(templateId, n, key) =>
                  conductor.handleCommand(
                    Question.Cmd.QueryContractKey(templateId, key, n.value.toInt)
                  )
              }
          }
          for {
            z <- scala.util.Try(loop0(upd.start)).toEither.flatten
            y <- loop(impure.resume(z.asInstanceOf[x]))
          } yield y
      }

    val flow = for {
      setupValue <- setup
      _ = recordingLogger.llTrace("starts test")
      testValue <- test(setupValue)
    } yield {
      recordingLogger.llTrace("ends test")
      testValue
    }

    loop(flow.start) ->
      recordingLogger.recordedMessages.dropWhile(_ != "starts test")
  }
}
