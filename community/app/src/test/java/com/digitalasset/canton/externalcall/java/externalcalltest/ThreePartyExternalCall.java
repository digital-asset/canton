package com.digitalasset.canton.externalcall.java.externalcalltest;

import static com.daml.ledger.javaapi.data.codegen.json.JsonLfEncoders.apply;

import com.daml.ledger.javaapi.data.CreateAndExerciseCommand;
import com.daml.ledger.javaapi.data.CreateCommand;
import com.daml.ledger.javaapi.data.CreatedEvent;
import com.daml.ledger.javaapi.data.DamlRecord;
import com.daml.ledger.javaapi.data.ExerciseCommand;
import com.daml.ledger.javaapi.data.Identifier;
import com.daml.ledger.javaapi.data.PackageVersion;
import com.daml.ledger.javaapi.data.Party;
import com.daml.ledger.javaapi.data.Template;
import com.daml.ledger.javaapi.data.Unit;
import com.daml.ledger.javaapi.data.Value;
import com.daml.ledger.javaapi.data.codegen.Choice;
import com.daml.ledger.javaapi.data.codegen.ContractCompanion;
import com.daml.ledger.javaapi.data.codegen.ContractTypeCompanion;
import com.daml.ledger.javaapi.data.codegen.Created;
import com.daml.ledger.javaapi.data.codegen.Exercised;
import com.daml.ledger.javaapi.data.codegen.PreparedRecord;
import com.daml.ledger.javaapi.data.codegen.PrimitiveValueDecoders;
import com.daml.ledger.javaapi.data.codegen.UnknownTrailingFieldPolicy;
import com.daml.ledger.javaapi.data.codegen.Update;
import com.daml.ledger.javaapi.data.codegen.ValueDecoder;
import com.daml.ledger.javaapi.data.codegen.json.JsonLfDecoder;
import com.daml.ledger.javaapi.data.codegen.json.JsonLfDecoders;
import com.daml.ledger.javaapi.data.codegen.json.JsonLfEncoder;
import com.daml.ledger.javaapi.data.codegen.json.JsonLfEncoders;
import com.daml.ledger.javaapi.data.codegen.json.JsonLfReader;
import com.digitalasset.canton.externalcall.java.da.internal.template.Archive;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

public final class ThreePartyExternalCall extends Template {
  public static final Identifier TEMPLATE_ID =
      new Identifier("#ExternalCallTest", "ExternalCallTest", "ThreePartyExternalCall");

  public static final Identifier TEMPLATE_ID_WITH_PACKAGE_ID =
      new Identifier(
          ExternalCallInterface.PACKAGE_ID, "ExternalCallTest", "ThreePartyExternalCall");

  public static final String PACKAGE_ID = ExternalCallInterface.PACKAGE_ID;

  public static final String PACKAGE_NAME = ExternalCallInterface.PACKAGE_NAME;

  public static final PackageVersion PACKAGE_VERSION = ExternalCallInterface.PACKAGE_VERSION;

  public static final Choice<ThreePartyExternalCall, Archive, Unit> CHOICE_Archive =
      Choice.create(
          "Archive",
          Archive::toValue,
          Archive.valueDecoder(),
          PrimitiveValueDecoders.fromUnit,
          new Archive.JsonDecoder$().get(),
          JsonLfDecoders.unit,
          Archive::jsonEncoder,
          JsonLfEncoders::unit);

  public static final Choice<ThreePartyExternalCall, CallVisibleToAll, String>
      CHOICE_CallVisibleToAll =
          Choice.create(
              "CallVisibleToAll",
              CallVisibleToAll::toValue,
              CallVisibleToAll.valueDecoder(),
              PrimitiveValueDecoders.fromText,
              new CallVisibleToAll.JsonDecoder$().get(),
              JsonLfDecoders.text,
              CallVisibleToAll::jsonEncoder,
              JsonLfEncoders::text);

  public static final Choice<ThreePartyExternalCall, NestedCallWithBob, Value>
      CHOICE_NestedCallWithBob =
          Choice.create(
              "NestedCallWithBob",
              NestedCallWithBob::toValue,
              NestedCallWithBob.valueDecoder(),
              ExternalCallTestCodegenShim.rawValueDecoder(),
              new NestedCallWithBob.JsonDecoder$().get(),
              ExternalCallTestCodegenShim.unsupportedJsonDecoder("NestedCallWithBob result"),
              NestedCallWithBob::jsonEncoder,
              ExternalCallTestCodegenShim.unsupportedJsonEncoder("NestedCallWithBob result"));

  public static final Choice<ThreePartyExternalCall, BobExternalCall, String>
      CHOICE_BobExternalCall =
          Choice.create(
              "BobExternalCall",
              BobExternalCall::toValue,
              BobExternalCall.valueDecoder(),
              PrimitiveValueDecoders.fromText,
              new BobExternalCall.JsonDecoder$().get(),
              JsonLfDecoders.text,
              BobExternalCall::jsonEncoder,
              JsonLfEncoders::text);

  public static final ContractCompanion.WithoutKey<Contract, ContractId, ThreePartyExternalCall>
      COMPANION =
          new ContractCompanion.WithoutKey<>(
              ExternalCallTestCodegenShim.PACKAGE,
              "com.digitalasset.canton.externalcall.java.externalcalltest.ThreePartyExternalCall",
              TEMPLATE_ID,
              ContractId::new,
              ThreePartyExternalCall::fromJson,
              Contract::new,
              List.of(
                  CHOICE_Archive,
                  CHOICE_CallVisibleToAll,
                  CHOICE_NestedCallWithBob,
                  CHOICE_BobExternalCall),
              ThreePartyExternalCall.templateValueDecoder());

  public final String alice;

  public final String bob;

  public final String charlie;

  public ThreePartyExternalCall(String alice, String bob, String charlie) {
    this.alice = alice;
    this.bob = bob;
    this.charlie = charlie;
  }

  @Override
  public Update<Created<ContractId>> create() {
    return new Update.CreateUpdate<>(
        new CreateCommand(TEMPLATE_ID, this.toValue()), x -> x, ContractId::new);
  }

  @Override
  public CreateAnd createAnd() {
    return new CreateAnd(this);
  }

  @Override
  protected ContractCompanion.WithoutKey<Contract, ContractId, ThreePartyExternalCall>
      getCompanion() {
    return COMPANION;
  }

  public static ValueDecoder<ThreePartyExternalCall> valueDecoder() {
    return ContractCompanion.valueDecoder(COMPANION);
  }

  @Override
  public DamlRecord toValue() {
    ArrayList<DamlRecord.Field> fields = new ArrayList<>(3);
    fields.add(new DamlRecord.Field("alice", new Party(this.alice)));
    fields.add(new DamlRecord.Field("bob", new Party(this.bob)));
    fields.add(new DamlRecord.Field("charlie", new Party(this.charlie)));
    return new DamlRecord(fields);
  }

  private static ValueDecoder<ThreePartyExternalCall> templateValueDecoder() {
    return ValueDecoder.create(
        (value$, policy$) -> {
          PreparedRecord preparedRecord$ =
              PrimitiveValueDecoders.checkAndPrepareRecord(3, 0, value$, policy$);
          List<DamlRecord.Field> fields$ = preparedRecord$.getExpectedFields();
          return new ThreePartyExternalCall(
              PrimitiveValueDecoders.fromParty.decode(fields$.get(0).getValue(), policy$),
              PrimitiveValueDecoders.fromParty.decode(fields$.get(1).getValue(), policy$),
              PrimitiveValueDecoders.fromParty.decode(fields$.get(2).getValue(), policy$));
        });
  }

  public static JsonLfDecoder<ThreePartyExternalCall> jsonDecoder() {
    return JsonLfDecoders.record(
        Arrays.asList("alice", "bob", "charlie"),
        name -> {
          switch (name) {
            case "alice":
              return JsonLfDecoders.JavaArg.at(0, JsonLfDecoders.party);
            case "bob":
              return JsonLfDecoders.JavaArg.at(1, JsonLfDecoders.party);
            case "charlie":
              return JsonLfDecoders.JavaArg.at(2, JsonLfDecoders.party);
            default:
              return null;
          }
        },
        args ->
            new ThreePartyExternalCall(
                JsonLfDecoders.cast(args[0]),
                JsonLfDecoders.cast(args[1]),
                JsonLfDecoders.cast(args[2])));
  }

  public static ThreePartyExternalCall fromJson(String json) throws JsonLfDecoder.Error {
    return jsonDecoder().decode(new JsonLfReader(json), UnknownTrailingFieldPolicy.STRICT);
  }

  public static ThreePartyExternalCall fromJson(String json, UnknownTrailingFieldPolicy policy)
      throws JsonLfDecoder.Error {
    return jsonDecoder().decode(new JsonLfReader(json), policy);
  }

  @Override
  public JsonLfEncoder jsonEncoder() {
    return JsonLfEncoders.record(
        JsonLfEncoders.Field.of("alice", apply(JsonLfEncoders::party, alice)),
        JsonLfEncoders.Field.of("bob", apply(JsonLfEncoders::party, bob)),
        JsonLfEncoders.Field.of("charlie", apply(JsonLfEncoders::party, charlie)));
  }

  public static final class ContractId
      extends com.daml.ledger.javaapi.data.codegen.ContractId<ThreePartyExternalCall>
      implements Exercises<ExerciseCommand> {
    public ContractId(String contractId) {
      super(contractId);
    }

    @Override
    protected ContractTypeCompanion<
            ? extends com.daml.ledger.javaapi.data.codegen.Contract<ContractId, ?>,
            ContractId,
            ThreePartyExternalCall,
            ?>
        getCompanion() {
      return COMPANION;
    }
  }

  public static final class Contract
      extends com.daml.ledger.javaapi.data.codegen.Contract<ContractId, ThreePartyExternalCall> {
    public Contract(
        ContractId id, ThreePartyExternalCall data, Set<String> signatories, Set<String> observers) {
      super(id, data, signatories, observers);
    }

    @Override
    protected ContractCompanion<Contract, ContractId, ThreePartyExternalCall> getCompanion() {
      return COMPANION;
    }

    public static Contract fromCreatedEvent(CreatedEvent event) {
      return COMPANION.fromCreatedEvent(event);
    }
  }

  public interface Exercises<Cmd>
      extends com.daml.ledger.javaapi.data.codegen.Exercises.Archivable<Cmd> {
    default Update<Exercised<Unit>> exerciseArchive(Archive arg) {
      return makeExerciseCmd(CHOICE_Archive, arg);
    }

    @Override
    default Update<Exercised<Unit>> exerciseArchive() {
      return exerciseArchive(new Archive());
    }

    default Update<Exercised<String>> exerciseCallVisibleToAll(CallVisibleToAll arg) {
      return makeExerciseCmd(CHOICE_CallVisibleToAll, arg);
    }

    default Update<Exercised<String>> exerciseCallVisibleToAll(String input) {
      return exerciseCallVisibleToAll(new CallVisibleToAll(input));
    }

    default Update<Exercised<Value>> exerciseNestedCallWithBob(NestedCallWithBob arg) {
      return makeExerciseCmd(CHOICE_NestedCallWithBob, arg);
    }

    default Update<Exercised<Value>> exerciseNestedCallWithBob(String input1, String input2) {
      return exerciseNestedCallWithBob(new NestedCallWithBob(input1, input2));
    }

    default Update<Exercised<String>> exerciseBobExternalCall(BobExternalCall arg) {
      return makeExerciseCmd(CHOICE_BobExternalCall, arg);
    }

    default Update<Exercised<String>> exerciseBobExternalCall(String input) {
      return exerciseBobExternalCall(new BobExternalCall(input));
    }
  }

  public static final class CreateAnd extends com.daml.ledger.javaapi.data.codegen.CreateAnd
      implements Exercises<CreateAndExerciseCommand> {
    CreateAnd(Template createArguments) {
      super(createArguments);
    }

    @Override
    protected ContractTypeCompanion<
            ? extends com.daml.ledger.javaapi.data.codegen.Contract<ContractId, ?>,
            ContractId,
            ThreePartyExternalCall,
            ?>
        getCompanion() {
      return COMPANION;
    }
  }
}
