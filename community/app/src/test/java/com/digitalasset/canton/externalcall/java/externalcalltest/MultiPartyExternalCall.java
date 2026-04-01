package com.digitalasset.canton.externalcall.java.externalcalltest;

import static com.daml.ledger.javaapi.data.codegen.json.JsonLfEncoders.apply;

import com.daml.ledger.javaapi.data.CreateAndExerciseCommand;
import com.daml.ledger.javaapi.data.CreateCommand;
import com.daml.ledger.javaapi.data.CreatedEvent;
import com.daml.ledger.javaapi.data.DamlCollectors;
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

public final class MultiPartyExternalCall extends Template {
  public static final Identifier TEMPLATE_ID =
      new Identifier("#ExternalCallTest", "ExternalCallTest", "MultiPartyExternalCall");

  public static final Identifier TEMPLATE_ID_WITH_PACKAGE_ID =
      new Identifier(
          ExternalCallInterface.PACKAGE_ID, "ExternalCallTest", "MultiPartyExternalCall");

  public static final String PACKAGE_ID = ExternalCallInterface.PACKAGE_ID;

  public static final String PACKAGE_NAME = ExternalCallInterface.PACKAGE_NAME;

  public static final PackageVersion PACKAGE_VERSION = ExternalCallInterface.PACKAGE_VERSION;

  public static final Choice<MultiPartyExternalCall, Archive, Unit> CHOICE_Archive =
      Choice.create(
          "Archive",
          Archive::toValue,
          Archive.valueDecoder(),
          PrimitiveValueDecoders.fromUnit,
          new Archive.JsonDecoder$().get(),
          JsonLfDecoders.unit,
          Archive::jsonEncoder,
          JsonLfEncoders::unit);

  public static final Choice<MultiPartyExternalCall, BothSignatoriesCall, Value>
      CHOICE_BothSignatoriesCall =
          Choice.create(
              "BothSignatoriesCall",
              BothSignatoriesCall::toValue,
              BothSignatoriesCall.valueDecoder(),
              ExternalCallTestCodegenShim.rawValueDecoder(),
              new BothSignatoriesCall.JsonDecoder$().get(),
              ExternalCallTestCodegenShim.unsupportedJsonDecoder("BothSignatoriesCall result"),
              BothSignatoriesCall::jsonEncoder,
              ExternalCallTestCodegenShim.unsupportedJsonEncoder(
                  "BothSignatoriesCall result"));

  public static final Choice<MultiPartyExternalCall, SameCallTwice, Value>
      CHOICE_SameCallTwice =
          Choice.create(
              "SameCallTwice",
              SameCallTwice::toValue,
              SameCallTwice.valueDecoder(),
              ExternalCallTestCodegenShim.rawValueDecoder(),
              new SameCallTwice.JsonDecoder$().get(),
              ExternalCallTestCodegenShim.unsupportedJsonDecoder("SameCallTwice result"),
              SameCallTwice::jsonEncoder,
              ExternalCallTestCodegenShim.unsupportedJsonEncoder("SameCallTwice result"));

  public static final Choice<MultiPartyExternalCall, SingleExternalCall, String>
      CHOICE_SingleExternalCall =
          Choice.create(
              "SingleExternalCall",
              SingleExternalCall::toValue,
              SingleExternalCall.valueDecoder(),
              PrimitiveValueDecoders.fromText,
              new SingleExternalCall.JsonDecoder$().get(),
              JsonLfDecoders.text,
              SingleExternalCall::jsonEncoder,
              JsonLfEncoders::text);

  public static final ContractCompanion.WithoutKey<Contract, ContractId, MultiPartyExternalCall>
      COMPANION =
          new ContractCompanion.WithoutKey<>(
              ExternalCallTestCodegenShim.PACKAGE,
              "com.digitalasset.canton.externalcall.java.externalcalltest.MultiPartyExternalCall",
              TEMPLATE_ID,
              ContractId::new,
              MultiPartyExternalCall::fromJson,
              Contract::new,
              List.of(
                  CHOICE_Archive,
                  CHOICE_BothSignatoriesCall,
                  CHOICE_SameCallTwice,
                  CHOICE_SingleExternalCall),
              MultiPartyExternalCall.templateValueDecoder());

  public final String signatory1;

  public final String signatory2;

  public final List<String> observers_;

  public MultiPartyExternalCall(String signatory1, String signatory2, List<String> observers_) {
    this.signatory1 = signatory1;
    this.signatory2 = signatory2;
    this.observers_ = observers_;
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
  protected ContractCompanion.WithoutKey<Contract, ContractId, MultiPartyExternalCall>
      getCompanion() {
    return COMPANION;
  }

  public static ValueDecoder<MultiPartyExternalCall> valueDecoder() {
    return ContractCompanion.valueDecoder(COMPANION);
  }

  @Override
  public DamlRecord toValue() {
    ArrayList<DamlRecord.Field> fields = new ArrayList<>(3);
    fields.add(new DamlRecord.Field("signatory1", new Party(this.signatory1)));
    fields.add(new DamlRecord.Field("signatory2", new Party(this.signatory2)));
    fields.add(
        new DamlRecord.Field(
            "observers_",
            this.observers_.stream().collect(DamlCollectors.toDamlList(Party::new))));
    return new DamlRecord(fields);
  }

  private static ValueDecoder<MultiPartyExternalCall> templateValueDecoder() {
    return ValueDecoder.create(
        (value$, policy$) -> {
          PreparedRecord preparedRecord$ =
              PrimitiveValueDecoders.checkAndPrepareRecord(3, 0, value$, policy$);
          List<DamlRecord.Field> fields$ = preparedRecord$.getExpectedFields();
          String signatory1 =
              PrimitiveValueDecoders.fromParty.decode(fields$.get(0).getValue(), policy$);
          String signatory2 =
              PrimitiveValueDecoders.fromParty.decode(fields$.get(1).getValue(), policy$);
          List<String> observers_ =
              PrimitiveValueDecoders.fromList(PrimitiveValueDecoders.fromParty)
                  .decode(fields$.get(2).getValue(), policy$);
          return new MultiPartyExternalCall(signatory1, signatory2, observers_);
        });
  }

  public static JsonLfDecoder<MultiPartyExternalCall> jsonDecoder() {
    return JsonLfDecoders.record(
        Arrays.asList("signatory1", "signatory2", "observers_"),
        name -> {
          switch (name) {
            case "signatory1":
              return JsonLfDecoders.JavaArg.at(0, JsonLfDecoders.party);
            case "signatory2":
              return JsonLfDecoders.JavaArg.at(1, JsonLfDecoders.party);
            case "observers_":
              return JsonLfDecoders.JavaArg.at(2, JsonLfDecoders.list(JsonLfDecoders.party));
            default:
              return null;
          }
        },
        args ->
            new MultiPartyExternalCall(
                JsonLfDecoders.cast(args[0]),
                JsonLfDecoders.cast(args[1]),
                JsonLfDecoders.cast(args[2])));
  }

  public static MultiPartyExternalCall fromJson(String json) throws JsonLfDecoder.Error {
    return jsonDecoder().decode(new JsonLfReader(json), UnknownTrailingFieldPolicy.STRICT);
  }

  public static MultiPartyExternalCall fromJson(String json, UnknownTrailingFieldPolicy policy)
      throws JsonLfDecoder.Error {
    return jsonDecoder().decode(new JsonLfReader(json), policy);
  }

  @Override
  public JsonLfEncoder jsonEncoder() {
    return JsonLfEncoders.record(
        JsonLfEncoders.Field.of("signatory1", apply(JsonLfEncoders::party, signatory1)),
        JsonLfEncoders.Field.of("signatory2", apply(JsonLfEncoders::party, signatory2)),
        JsonLfEncoders.Field.of(
            "observers_", apply(JsonLfEncoders.list(JsonLfEncoders::party), observers_)));
  }

  public static final class ContractId
      extends com.daml.ledger.javaapi.data.codegen.ContractId<MultiPartyExternalCall>
      implements Exercises<ExerciseCommand> {
    public ContractId(String contractId) {
      super(contractId);
    }

    @Override
    protected ContractTypeCompanion<
            ? extends com.daml.ledger.javaapi.data.codegen.Contract<ContractId, ?>,
            ContractId,
            MultiPartyExternalCall,
            ?>
        getCompanion() {
      return COMPANION;
    }
  }

  public static final class Contract
      extends com.daml.ledger.javaapi.data.codegen.Contract<ContractId, MultiPartyExternalCall> {
    public Contract(
        ContractId id,
        MultiPartyExternalCall data,
        Set<String> signatories,
        Set<String> observers) {
      super(id, data, signatories, observers);
    }

    @Override
    protected ContractCompanion<Contract, ContractId, MultiPartyExternalCall> getCompanion() {
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

    default Update<Exercised<Value>> exerciseBothSignatoriesCall(BothSignatoriesCall arg) {
      return makeExerciseCmd(CHOICE_BothSignatoriesCall, arg);
    }

    default Update<Exercised<Value>> exerciseBothSignatoriesCall(String input1, String input2) {
      return exerciseBothSignatoriesCall(new BothSignatoriesCall(input1, input2));
    }

    default Update<Exercised<Value>> exerciseSameCallTwice(SameCallTwice arg) {
      return makeExerciseCmd(CHOICE_SameCallTwice, arg);
    }

    default Update<Exercised<Value>> exerciseSameCallTwice(String input) {
      return exerciseSameCallTwice(new SameCallTwice(input));
    }

    default Update<Exercised<String>> exerciseSingleExternalCall(SingleExternalCall arg) {
      return makeExerciseCmd(CHOICE_SingleExternalCall, arg);
    }

    default Update<Exercised<String>> exerciseSingleExternalCall(String input) {
      return exerciseSingleExternalCall(new SingleExternalCall(input));
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
            MultiPartyExternalCall,
            ?>
        getCompanion() {
      return COMPANION;
    }
  }
}
