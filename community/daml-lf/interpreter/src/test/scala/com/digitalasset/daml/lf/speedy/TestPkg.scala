// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.{PackageId, PackageName, TypeConId}
import com.digitalasset.daml.lf.language.Ast.TTyCon
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.testing.parser.Implicits.SyntaxHelper
import com.digitalasset.daml.lf.testing.parser.ParserParameters
import com.digitalasset.daml.lf.transaction.SerializationVersion

/** Shared Daml-LF package definition and common constants used by both
  * [[EvaluationOrderTest]] and [[AuthorizationTest]].
  */
object TestPkg {
  val packageId: Ref.PackageId = Ref.PackageId.assertFromString("-pkg-")

  private def tuple2TyCon: String = {
    val Tuple2 =
      com.digitalasset.daml.lf.stablepackages.StablePackages.stablePackages.Tuple2
    s"'${Tuple2.packageId}':${Tuple2.qualifiedName}"
  }
}

class TestPkg(withKey: Boolean, languageVersion: LanguageVersion) {
  import TestPkg.{packageId, tuple2TyCon}

  val serializationVersion: SerializationVersion = SerializationVersion.assign(languageVersion)

  implicit val parserParameters: ParserParameters[this.type] =
    ParserParameters(packageId, languageVersion = languageVersion)

  val pkg: language.Ast.Package = {
    val ifKey = if (withKey) "    " else "//  "
    p"""  metadata ( 'evaluation-order-test' : '1.0.0' )
      module M {

        record @serializable MyUnit = {};

        record @serializable TKey = { maintainers : List Party, optCid : Option (ContractId Unit), nested: M:Nested };

        record @serializable Nested = { f : Option M:Nested };

        val buildNested : Int64 -> M:Nested = \(i: Int64) ->
          case (EQUAL @Int64 i 0) of
            True -> M:Nested { f = None @M:Nested }
            | _ -> M:Nested { f = Some @M:Nested (M:buildNested (SUB_INT64 i 1)) };

        val toKey : Party -> M:TKey = \(p : Party) ->
           M:TKey { maintainers = Cons @Party [p] (Nil @Party), optCid = None @(ContractId Unit), nested = M:buildNested 0 };
        val keyNoMaintainers : M:TKey = M:TKey { maintainers = Nil @Party, optCid = None @(ContractId Unit), nested = M:buildNested 0 };
        val toKeyWithCid : Party -> ContractId Unit -> M:TKey = \(p : Party) (cid : ContractId Unit) -> M:TKey { maintainers = Cons @Party [p] (Nil @Party), optCid = Some @(ContractId Unit) cid, nested = M:buildNested 0 };

        variant @serializable Either (a:*) (b:*) = Left: a | Right : b;

        interface (this : I1) =  { viewtype M:MyUnit; };

        interface (this: Person) = {
          viewtype M:MyUnit;
          method asParty: Party;
          method getCtrl: Party;
          method getName: Text;
          choice @nonConsuming Nap (self) (i : Int64): Int64
            , controllers TRACE @(List Party) "interface choice controllers" (Cons @Party [call_method @M:Person getCtrl this] (Nil @Party))
            , observers TRACE @(List Party) "interface choice observers" (Nil @Party)
            to upure @Int64 (TRACE @Int64 "choice body" i);
        } ;

        record @serializable T = { signatory : Party, observer : Party, precondition : Bool, key: M:TKey, nested: M:Nested };
        template (this: T) = {
          precondition TRACE @Bool "precondition" (M:T {precondition} this);
          signatories TRACE @(List Party) "contract signatories" (Cons @Party [M:T {signatory} this] (Nil @Party));
          observers TRACE @(List Party) "contract observers" (Cons @Party [M:T {observer} this] (Nil @Party));
          choice Choice (self) (arg: M:Either M:Nested Int64) : M:Nested,
            controllers TRACE @(List Party) "template choice controllers" (Cons @Party [M:T {signatory} this] (Nil @Party)),
            observers TRACE @(List Party) "template choice observers" (Nil @Party),
            authorizers TRACE @(List Party) "template choice authorizers" (Cons @Party [M:T {signatory} this] (Nil @Party))
            to upure @M:Nested (TRACE @M:Nested "choice body" (M:buildNested (case arg of M:Either:Right i -> i | _ -> 0)));
          choice Archive (self) (arg: Unit): Unit,
            controllers Cons @Party [M:T {signatory} this] (Nil @Party)
            to upure @Unit (TRACE @Unit "archive" ());
          choice @nonConsuming Divulge (self) (divulgee: Party): Unit,
            controllers Cons @Party [divulgee] (Nil @Party)
            to upure @Unit ();
$ifKey    key @M:TKey
$ifKey       (TRACE @M:TKey "key" (M:T {key} this))
$ifKey       (\(key : M:TKey) -> TRACE @(List Party) "maintainers" (M:TKey {maintainers} key));
        };

        record @serializable MyException = { message: Text } ;

        exception MyException = {
          message \(e: M:MyException) -> M:MyException {message} e
        };

        record @serializable TExcept = { signatory : Party, observer : Party, precondition : Bool, key: M:TKey, nested: M:Nested };
        template (this: TExcept) = {
          precondition TRACE @Bool "precondition" (M:TExcept {precondition} this);
          signatories TRACE @(List Party) "contract signatories" (Cons @Party [M:TExcept {signatory} this] (Nil @Party));
          observers TRACE @(List Party) "contract observers" (Cons @Party [M:TExcept {observer} this] (Nil @Party));
$ifKey    key @M:TKey
$ifKey       (TRACE @M:TKey "key" (M:TExcept {key} this))
$ifKey       (\(key : M:TKey) -> TRACE @(List Party) "maintainers" (throw @(List Party) @M:MyException (M:MyException {message = "thrown as part of maintainers expr"})));
        };

        record @serializable Human = { person: Party, obs: Party, ctrl: Party, precond: Bool, key: M:TKey, nested: M:Nested };
        template (this: Human) = {
          precondition TRACE @Bool "precondition" (M:Human {precond} this);
          signatories TRACE @(List Party) "contract signatories" (Cons @Party [M:Human {person} this] (Nil @Party));
          observers TRACE @(List Party) "contract observers" (Cons @Party [M:Human {obs} this] (Nil @Party));
          choice Archive (self) (arg: Unit): Unit,
            controllers Cons @Party [M:Human {person} this] (Nil @Party)
            to upure @Unit (TRACE @Unit "archive" ());
          implements M:Person {
            view = TRACE @M:MyUnit "view" (M:MyUnit {});
            method asParty = M:Human {person} this;
            method getName = "foobar";
            method getCtrl = M:Human {ctrl} this;
            };
$ifKey    key @M:TKey
$ifKey       (TRACE @M:TKey "key" (M:Human {key} this))
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

        val foldl: forall (a: *) (b: *). (a -> b -> a) -> a -> List b -> a = /\ (a: *) (b: *).
          \(f: a -> b -> a) (acc: a) (xs: List b) ->
            case xs of
              Nil -> acc
            | Cons x xs -> M:foldl @a @b f (f acc x) xs;

        val foldr: forall (a: *) (b: *). (b -> a -> a) -> a -> List b -> a = /\ (a: *) (b: *).
          \(f: b -> a -> a) (acc: a) (xs: List b) ->
            case xs of
              Nil -> acc
           | Cons x xs -> f x (M:foldr @a @b f acc xs);

      }

      module Test {
        val noParty: Option Party = None @Party;
        val someParty: Party -> Option Party = \(p: Party) -> Some @Party p;
        val noCid: Option (ContractId Unit) = None @(ContractId Unit);
        val someCid: ContractId Unit -> Option (ContractId Unit) = \(cid: ContractId Unit) -> Some @(ContractId Unit) cid;

        val run: forall (t: *). Update t -> Update Unit =
          /\(t: *). \(u: Update t) ->
            ubind x:Unit <- upure @Unit (TRACE @Unit "starts test" ())
            in ubind y:t <- u
            in upure @Unit (TRACE @Unit "ends test" ());

        val create: M:T -> Update Unit =
          \(arg: M:T) -> Test:run @(ContractId M:T) (create @M:T arg);

        val create_interface: M:Human -> Update Unit =
          \(arg: M:Human) -> Test:run @(ContractId M:Person) (create_by_interface @M:Person (to_interface @M:Person @M:Human arg));

        val exercise_by_id: Party -> ContractId M:T -> M:Either Int64 Int64 -> Update Unit =
          \(exercisingParty: Party) (cId: ContractId M:T) (argParams: M:Either Int64 Int64) ->
            let arg: Test:ExeArg = Test:ExeArg {
              id = cId,
              argParams = argParams
            }
            in ubind
              helperId: ContractId Test:Helper <- Test:createHelper exercisingParty;
              x: M:Nested <-exercise @Test:Helper Exe helperId arg
            in upure @Unit ();

        val exercise_interface_with_guard: Party -> ContractId M:Person -> Update Unit =
          \(exercisingParty: Party) (cId: ContractId M:Person) ->
            Test:run @Int64 (exercise_interface_with_guard @M:Person Nap cId 42 (\(x: M:Person) -> TRACE @Bool "interface guard" True));

        val exercise_interface: Party -> ContractId M:Person -> Update Unit =
          \(exercisingParty: Party) (cId: ContractId M:Person) ->
            Test:run @Int64 (exercise_interface @M:Person Nap cId 42);

$ifKey  val exercise_by_key: Party -> Option Party -> Option (ContractId Unit) -> Int64 -> M:Either Int64 Int64 -> Update Unit =
$ifKey    \(exercisingParty: Party) (maintainers: Option Party) (optCid: Option (ContractId Unit)) (nesting: Int64) (argParams: M:Either Int64 Int64) ->
$ifKey      let arg: Test:ExeByKeyArg = Test:ExeByKeyArg {
$ifKey        key = Test:TKeyParams {maintainers = Test:optToList @Party maintainers, optCid = optCid, nesting = nesting},
$ifKey        argParams = argParams
$ifKey      }
$ifKey      in ubind
$ifKey        helperId: ContractId Test:Helper <- Test:createHelper exercisingParty;
$ifKey        x: M:Nested <- exercise @Test:Helper ExeByKey helperId arg
$ifKey      in upure @Unit ();

        val fetch_by_id: Party -> ContractId M:T -> Update Unit =
          \(fetchingParty: Party) (cId: ContractId M:T) ->
            ubind helperId: ContractId Test:Helper <- Test:createHelper fetchingParty
            in exercise @Test:Helper FetchById helperId cId;

        val fetch_interface: Party -> ContractId M:Person -> Update Unit =
          \(fetchingParty: Party) (cId: ContractId M:Person) ->
            ubind helperId: ContractId Test:Helper <- Test:createHelper fetchingParty
            in exercise @Test:Helper FetchByInterface helperId cId;

$ifKey  val fetch_by_key: Party -> Option Party -> Option (ContractId Unit) -> Int64 -> Update Unit =
$ifKey    \(fetchingParty: Party) (maintainers: Option Party) (optCid: Option (ContractId Unit)) (nesting: Int64) ->
$ifKey       ubind helperId: ContractId Test:Helper <- Test:createHelper fetchingParty
$ifKey       in exercise @Test:Helper FetchByKey helperId (Test:TKeyParams {maintainers = Test:optToList @Party maintainers, optCid = optCid, nesting = nesting});

$ifKey  val lookup_by_key: Party -> Option Party -> Option (ContractId Unit) -> Int64 -> Update Unit =
$ifKey    \(lookingParty: Party) (maintainers: Option Party) (optCid: Option (ContractId Unit)) (nesting: Int64) ->
$ifKey       ubind helperId: ContractId Test:Helper <- Test:createHelper lookingParty
$ifKey       in exercise @Test:Helper LookupByKey helperId (Test:TKeyParams {maintainers = Test:optToList @Party maintainers, optCid = optCid, nesting = nesting});

$ifKey  val query_n_by_key: Int64 -> Party -> Option Party -> Option (ContractId Unit) -> Int64 -> Update Unit =
$ifKey    \(n: Int64) (lookingParty: Party) (maintainers: Option Party) (optCid: Option (ContractId Unit)) (nesting: Int64) ->
$ifKey       ubind helperId: ContractId Test:Helper <- Test:createHelper lookingParty
$ifKey       in exercise @Test:Helper QueryNByKey helperId ($tuple2TyCon @Int64 @Test:TKeyParams {_1 = n, _2 = Test:TKeyParams {maintainers = Test:optToList @Party maintainers, optCid = optCid, nesting = nesting}});

$ifKey  val query_n_by_key_except: Int64 -> Party -> Option Party -> Option (ContractId Unit) -> Int64 -> Update Unit =
$ifKey    \(n: Int64) (lookingParty: Party) (maintainers: Option Party) (optCid: Option (ContractId Unit)) (nesting: Int64) ->
$ifKey       ubind helperId: ContractId Test:Helper <- Test:createHelper lookingParty
$ifKey       in exercise @Test:Helper QueryNByKeyExcept helperId ($tuple2TyCon @Int64 @Test:TKeyParams {_1 = n, _2 = Test:TKeyParams {maintainers = Test:optToList @Party maintainers, optCid = optCid, nesting = nesting}});

        val createHelper: Party -> Update (ContractId Test:Helper) =
          \(party: Party) -> create @Test:Helper Test:Helper { sig = party, obs = party };

        val optToList: forall(t:*). Option t -> List t  =
          /\(t:*). \(opt: Option t) ->
            case opt of
               None -> Nil @t
             | Some x -> Cons @t [x] (Nil @t);

        record @serializable TKeyParams = { maintainers : List Party, optCid : Option (ContractId Unit), nesting: Int64 };
        val buildTKey: (Test:TKeyParams) -> M:TKey =
          \(params: Test:TKeyParams) -> M:TKey {
              maintainers = Test:TKeyParams {maintainers} params,
              optCid = Test:TKeyParams {optCid} params,
              nested = M:buildNested (Test:TKeyParams {nesting} params)
            };

        record @serializable ExeArg = {
          id: ContractId M:T,
          argParams: M:Either Int64 Int64
        };

        record @serializable ExeByKeyArg = {
          key: Test:TKeyParams,
          argParams: M:Either Int64 Int64
        };

        record @serializable Helper = { sig: Party, obs: Party };
        template (this: Helper) = {
          precondition True;
          signatories Cons @Party [Test:Helper {sig} this] (Nil @Party);
          observers Nil @Party;
          choice CreateNonvisibleKey (self) (arg: Unit): ContractId M:T,
            controllers Cons @Party [Test:Helper {obs} this] (Nil @Party),
            observers Nil @Party
             to let sig: Party = Test:Helper {sig} this
             in create @M:T M:T { signatory = sig, observer = sig, precondition = True, key = M:toKey sig, nested = M:buildNested 0 };
          choice Exe (self) (arg: Test:ExeArg): M:Nested,
            controllers Cons @Party [Test:Helper {sig} this] (Nil @Party),
            observers Nil @Party
            to
              let choiceArg: M:Either M:Nested Int64 = case (Test:ExeArg {argParams} arg) of
                  M:Either:Left n -> M:Either:Left @M:Nested @Int64 (M:buildNested n)
                | M:Either:Right n -> M:Either:Right @M:Nested @Int64 n
              in ubind
                x:Unit <- upure @Unit (TRACE @Unit "starts test" ());
                res: M:Nested <- exercise @M:T Choice (Test:ExeArg {id} arg) choiceArg;
                y:Unit <- upure @Unit (TRACE @Unit "ends test" ())
              in upure @M:Nested res;
$ifKey    choice ExeByKey (self) (arg: Test:ExeByKeyArg): M:Nested,
$ifKey      controllers Cons @Party [Test:Helper {sig} this] (Nil @Party),
$ifKey      observers Nil @Party
$ifKey      to
$ifKey        let choiceArg: M:Either M:Nested Int64 = case (Test:ExeByKeyArg {argParams} arg) of
$ifKey            M:Either:Left n -> M:Either:Left @M:Nested @Int64 (M:buildNested n)
$ifKey          | M:Either:Right n -> M:Either:Right @M:Nested @Int64 n
$ifKey       in ubind
$ifKey          x:Unit <- upure @Unit (TRACE @Unit "starts test" ());
$ifKey          res: M:Nested <- exercise_by_key @M:T Choice (Test:buildTKey (Test:ExeByKeyArg {key} arg)) choiceArg;
$ifKey          y:Unit <- upure @Unit (TRACE @Unit "ends test" ())
$ifKey        in upure @M:Nested res;
          choice FetchById (self) (cId: ContractId M:T): Unit,
            controllers Cons @Party [Test:Helper {sig} this] (Nil @Party),
            observers Nil @Party
            to Test:run @M:T (fetch_template @M:T cId);
          choice FetchByInterface (self) (cId: ContractId M:Person): Unit,
            controllers Cons @Party [Test:Helper {sig} this] (Nil @Party),
            observers Nil @Party
            to Test:run @M:Person (fetch_interface @M:Person cId);
$ifKey    choice FetchByKey (self) (params: Test:TKeyParams): Unit,
$ifKey      controllers Cons @Party [Test:Helper {sig} this] (Nil @Party),
$ifKey      observers Nil @Party
$ifKey      to let key: M:TKey = Test:buildTKey params
$ifKey         in Test:run @($tuple2TyCon (ContractId M:T) M:T) (fetch_by_key @M:T key);
$ifKey    choice LookupByKey (self) (params: Test:TKeyParams): Unit,
$ifKey      controllers Cons @Party [Test:Helper {sig} this] (Nil @Party),
$ifKey      observers Nil @Party
$ifKey      to let key: M:TKey = Test:buildTKey params
$ifKey         in Test:run @(Option (ContractId M:T)) (lookup_by_key @M:T key);

$ifKey    choice QueryNByKey (self) (paramsAndInt: ($tuple2TyCon Int64 Test:TKeyParams)): Unit,
$ifKey      controllers Cons @Party [Test:Helper {sig} this] (Nil @Party),
$ifKey      observers Nil @Party
$ifKey      to let n: Int64 = $tuple2TyCon @Int64 @Test:TKeyParams {_1} paramsAndInt
$ifKey         in let params: Test:TKeyParams = $tuple2TyCon @Int64 @Test:TKeyParams {_2} paramsAndInt
$ifKey            in let key: M:TKey = Test:buildTKey params
$ifKey               in Test:run @(List ($tuple2TyCon (ContractId M:T) M:T)) (query_n_by_key @M:T n key);

$ifKey    choice QueryNByKeyExcept (self) (paramsAndInt: ($tuple2TyCon Int64 Test:TKeyParams)): Unit,
$ifKey      controllers Cons @Party [Test:Helper {sig} this] (Nil @Party),
$ifKey      observers Nil @Party
$ifKey      to let n: Int64 = $tuple2TyCon @Int64 @Test:TKeyParams {_1} paramsAndInt
$ifKey         in let params: Test:TKeyParams = $tuple2TyCon @Int64 @Test:TKeyParams {_2} paramsAndInt
$ifKey            in let key: M:TKey = Test:buildTKey params
$ifKey               in Test:run @(List ($tuple2TyCon (ContractId M:TExcept) M:TExcept)) (query_n_by_key @M:TExcept n key);
        };

        val f: Text -> Text -> Text =
          \(x: Text) -> TRACE @(Text -> Text) x \(y: Text) -> TRACE @Text y (APPEND_TEXT x y);

        val testFold: ((Text -> Text -> Text) -> Text -> List Text -> Text) -> Update Unit =
          \(fold: (Text -> Text -> Text) -> Text -> List Text -> Text)  ->
            ubind x:Unit <- upure @Unit (TRACE @Unit "starts test" ())
            in ubind y:Text <- upure @Text (fold Test:f "0" (Cons @Text ["1", "2", "3"] (Nil @Text)))
            in upure @Unit (TRACE @Unit "ends test" ());
      }
  """
  }

  val pkgs: PureCompiledPackages = SpeedyTestLib.typeAndCompile(pkg)

  val packageNameMap: Map[PackageName, PackageId] = Map(pkg.pkgName -> packageId)

  val List(alice, bob, charlie): List[Ref.Party] =
    List("alice", "bob", "charlie").map(Ref.Party.assertFromString)

  val T: TypeConId = t"M:T" match {
    case TTyCon(tycon) => tycon
    case _ => sys.error("unexpected error")
  }

  val TExcept: TypeConId = t"M:TExcept" match {
    case TTyCon(tycon) => tycon
    case _ => sys.error("unexpected error")
  }

  val TKey: TypeConId = t"M:TKey" match {
    case TTyCon(tycon) => tycon
    case _ => sys.error("unexpected error")
  }

  val Nested: TypeConId = t"M:Nested" match {
    case TTyCon(tycon) => tycon
    case _ => sys.error("unexpected error")
  }

  val Human: TypeConId = t"M:Human" match {
    case TTyCon(tycon) => tycon
    case _ => sys.error("unexpected error")
  }

  val Person: TypeConId = t"M:Person" match {
    case TTyCon(tycon) => tycon
    case _ => sys.error("unexpected error")
  }

  val Dummy: TypeConId = t"M:Dummy" match {
    case TTyCon(tycon) => tycon
    case _ => sys.error("unexpected error")
  }
}
