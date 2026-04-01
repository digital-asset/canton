package com.digitalasset.canton.externalcall.java.externalcalltest;

import static com.daml.ledger.javaapi.data.codegen.json.JsonLfEncoders.apply;

import com.daml.ledger.javaapi.data.ContractFilter;
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

public final class ExternalCallContract extends Template {
  public static final Identifier TEMPLATE_ID =
      new Identifier("#ExternalCallTest", "ExternalCallTest", "ExternalCallContract");

  public static final Identifier TEMPLATE_ID_WITH_PACKAGE_ID =
      new Identifier(ExternalCallInterface.PACKAGE_ID, "ExternalCallTest", "ExternalCallContract");

  public static final String PACKAGE_ID = ExternalCallInterface.PACKAGE_ID;

  public static final String PACKAGE_NAME = ExternalCallInterface.PACKAGE_NAME;

  public static final PackageVersion PACKAGE_VERSION = ExternalCallInterface.PACKAGE_VERSION;

  public static final Choice<ExternalCallContract, Archive, Unit> CHOICE_Archive =
      Choice.create(
          "Archive",
          Archive::toValue,
          Archive.valueDecoder(),
          PrimitiveValueDecoders.fromUnit,
          new Archive.JsonDecoder$().get(),
          JsonLfDecoders.unit,
          Archive::jsonEncoder,
          JsonLfEncoders::unit);

  public static final Choice<ExternalCallContract, CallExternal, String> CHOICE_CallExternal =
      Choice.create(
          "CallExternal",
          CallExternal::toValue,
          CallExternal.valueDecoder(),
          PrimitiveValueDecoders.fromText,
          new CallExternal.JsonDecoder$().get(),
          JsonLfDecoders.text,
          CallExternal::jsonEncoder,
          JsonLfEncoders::text);

  public static final Choice<ExternalCallContract, CallMultiple, Value> CHOICE_CallMultiple =
      Choice.create(
          "CallMultiple",
          CallMultiple::toValue,
          CallMultiple.valueDecoder(),
          ExternalCallTestCodegenShim.rawValueDecoder(),
          new CallMultiple.JsonDecoder$().get(),
          ExternalCallTestCodegenShim.unsupportedJsonDecoder("CallMultiple result"),
          CallMultiple::jsonEncoder,
          ExternalCallTestCodegenShim.unsupportedJsonEncoder("CallMultiple result"));

  public static final Choice<ExternalCallContract, NestedExternalCall, Value>
      CHOICE_NestedExternalCall =
          Choice.create(
              "NestedExternalCall",
              NestedExternalCall::toValue,
              NestedExternalCall.valueDecoder(),
              ExternalCallTestCodegenShim.rawValueDecoder(),
              new NestedExternalCall.JsonDecoder$().get(),
              ExternalCallTestCodegenShim.unsupportedJsonDecoder("NestedExternalCall result"),
              NestedExternalCall::jsonEncoder,
              ExternalCallTestCodegenShim.unsupportedJsonEncoder("NestedExternalCall result"));

  public static final Choice<ExternalCallContract, DelegatedExternalCall, String>
      CHOICE_DelegatedExternalCall =
          Choice.create(
              "DelegatedExternalCall",
              DelegatedExternalCall::toValue,
              DelegatedExternalCall.valueDecoder(),
              PrimitiveValueDecoders.fromText,
              new DelegatedExternalCall.JsonDecoder$().get(),
              JsonLfDecoders.text,
              DelegatedExternalCall::jsonEncoder,
              JsonLfEncoders::text);

  public static final Choice<ExternalCallContract, CallInLeafOnly, String>
      CHOICE_CallInLeafOnly =
          Choice.create(
              "CallInLeafOnly",
              CallInLeafOnly::toValue,
              CallInLeafOnly.valueDecoder(),
              PrimitiveValueDecoders.fromText,
              new CallInLeafOnly.JsonDecoder$().get(),
              JsonLfDecoders.text,
              CallInLeafOnly::jsonEncoder,
              JsonLfEncoders::text);

  public static final Choice<ExternalCallContract, LeafExternalCall, String>
      CHOICE_LeafExternalCall =
          Choice.create(
              "LeafExternalCall",
              LeafExternalCall::toValue,
              LeafExternalCall.valueDecoder(),
              PrimitiveValueDecoders.fromText,
              new LeafExternalCall.JsonDecoder$().get(),
              JsonLfDecoders.text,
              LeafExternalCall::jsonEncoder,
              JsonLfEncoders::text);

  public static final Choice<ExternalCallContract, CallAtAllLevels, Value>
      CHOICE_CallAtAllLevels =
          Choice.create(
              "CallAtAllLevels",
              CallAtAllLevels::toValue,
              CallAtAllLevels.valueDecoder(),
              ExternalCallTestCodegenShim.rawValueDecoder(),
              new CallAtAllLevels.JsonDecoder$().get(),
              ExternalCallTestCodegenShim.unsupportedJsonDecoder("CallAtAllLevels result"),
              CallAtAllLevels::jsonEncoder,
              ExternalCallTestCodegenShim.unsupportedJsonEncoder("CallAtAllLevels result"));

  public static final Choice<ExternalCallContract, MiddleLevelCall, Value>
      CHOICE_MiddleLevelCall =
          Choice.create(
              "MiddleLevelCall",
              MiddleLevelCall::toValue,
              MiddleLevelCall.valueDecoder(),
              ExternalCallTestCodegenShim.rawValueDecoder(),
              new MiddleLevelCall.JsonDecoder$().get(),
              ExternalCallTestCodegenShim.unsupportedJsonDecoder("MiddleLevelCall result"),
              MiddleLevelCall::jsonEncoder,
              ExternalCallTestCodegenShim.unsupportedJsonEncoder("MiddleLevelCall result"));

  public static final ContractCompanion.WithoutKey<Contract, ContractId, ExternalCallContract>
      COMPANION =
          new ContractCompanion.WithoutKey<>(
              ExternalCallTestCodegenShim.PACKAGE,
              "com.digitalasset.canton.externalcall.java.externalcalltest.ExternalCallContract",
              TEMPLATE_ID,
              ContractId::new,
              ExternalCallContract::fromJson,
              Contract::new,
              List.of(
                  CHOICE_Archive,
                  CHOICE_CallExternal,
                  CHOICE_CallMultiple,
                  CHOICE_NestedExternalCall,
                  CHOICE_DelegatedExternalCall,
                  CHOICE_CallInLeafOnly,
                  CHOICE_LeafExternalCall,
                  CHOICE_CallAtAllLevels,
                  CHOICE_MiddleLevelCall),
              ExternalCallContract.templateValueDecoder());

  public final String signatory_;

  public final List<String> observers_;

  public ExternalCallContract(String signatory_, List<String> observers_) {
    this.signatory_ = signatory_;
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
  protected ContractCompanion.WithoutKey<Contract, ContractId, ExternalCallContract>
      getCompanion() {
    return COMPANION;
  }

  public static ValueDecoder<ExternalCallContract> valueDecoder() {
    return ContractCompanion.valueDecoder(COMPANION);
  }

  @Override
  public DamlRecord toValue() {
    ArrayList<DamlRecord.Field> fields = new ArrayList<>(2);
    fields.add(new DamlRecord.Field("signatory_", new Party(this.signatory_)));
    fields.add(
        new DamlRecord.Field(
            "observers_",
            this.observers_.stream().collect(DamlCollectors.toDamlList(Party::new))));
    return new DamlRecord(fields);
  }

  private static ValueDecoder<ExternalCallContract> templateValueDecoder() {
    return ValueDecoder.create(
        (value$, policy$) -> {
          PreparedRecord preparedRecord$ =
              PrimitiveValueDecoders.checkAndPrepareRecord(2, 0, value$, policy$);
          List<DamlRecord.Field> fields$ = preparedRecord$.getExpectedFields();
          String signatory_ =
              PrimitiveValueDecoders.fromParty.decode(fields$.get(0).getValue(), policy$);
          List<String> observers_ =
              PrimitiveValueDecoders.fromList(PrimitiveValueDecoders.fromParty)
                  .decode(fields$.get(1).getValue(), policy$);
          return new ExternalCallContract(signatory_, observers_);
        });
  }

  public static JsonLfDecoder<ExternalCallContract> jsonDecoder() {
    return JsonLfDecoders.record(
        Arrays.asList("signatory_", "observers_"),
        name -> {
          switch (name) {
            case "signatory_":
              return JsonLfDecoders.JavaArg.at(0, JsonLfDecoders.party);
            case "observers_":
              return JsonLfDecoders.JavaArg.at(1, JsonLfDecoders.list(JsonLfDecoders.party));
            default:
              return null;
          }
        },
        args -> new ExternalCallContract(JsonLfDecoders.cast(args[0]), JsonLfDecoders.cast(args[1])));
  }

  public static ExternalCallContract fromJson(String json) throws JsonLfDecoder.Error {
    return jsonDecoder().decode(new JsonLfReader(json), UnknownTrailingFieldPolicy.STRICT);
  }

  public static ExternalCallContract fromJson(String json, UnknownTrailingFieldPolicy policy)
      throws JsonLfDecoder.Error {
    return jsonDecoder().decode(new JsonLfReader(json), policy);
  }

  @Override
  public JsonLfEncoder jsonEncoder() {
    return JsonLfEncoders.record(
        JsonLfEncoders.Field.of("signatory_", apply(JsonLfEncoders::party, signatory_)),
        JsonLfEncoders.Field.of(
            "observers_", apply(JsonLfEncoders.list(JsonLfEncoders::party), observers_)));
  }

  public static ContractFilter<Contract> contractFilter() {
    return ContractFilter.of(COMPANION);
  }

  public static final class ContractId
      extends com.daml.ledger.javaapi.data.codegen.ContractId<ExternalCallContract>
      implements Exercises<ExerciseCommand> {
    public ContractId(String contractId) {
      super(contractId);
    }

    @Override
    protected ContractTypeCompanion<
            ? extends com.daml.ledger.javaapi.data.codegen.Contract<ContractId, ?>,
            ContractId,
            ExternalCallContract,
            ?>
        getCompanion() {
      return COMPANION;
    }

    public ExternalCallInterface.ContractId toInterface(
        ExternalCallInterface.INTERFACE_ interfaceCompanion) {
      return new ExternalCallInterface.ContractId(this.contractId);
    }
  }

  public static final class Contract
      extends com.daml.ledger.javaapi.data.codegen.Contract<ContractId, ExternalCallContract> {
    public Contract(
        ContractId id, ExternalCallContract data, Set<String> signatories, Set<String> observers) {
      super(id, data, signatories, observers);
    }

    @Override
    protected ContractCompanion<Contract, ContractId, ExternalCallContract> getCompanion() {
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

    default Update<Exercised<String>> exerciseCallExternal(CallExternal arg) {
      return makeExerciseCmd(CHOICE_CallExternal, arg);
    }

    default Update<Exercised<String>> exerciseCallExternal(
        String extensionId, String functionId, String configHash, String input) {
      return exerciseCallExternal(new CallExternal(extensionId, functionId, configHash, input));
    }

    default Update<Exercised<Value>> exerciseCallMultiple(CallMultiple arg) {
      return makeExerciseCmd(CHOICE_CallMultiple, arg);
    }

    default Update<Exercised<Value>> exerciseCallMultiple(
        String input1, String input2, String input3) {
      return exerciseCallMultiple(new CallMultiple(input1, input2, input3));
    }

    default Update<Exercised<Value>> exerciseNestedExternalCall(NestedExternalCall arg) {
      return makeExerciseCmd(CHOICE_NestedExternalCall, arg);
    }

    default Update<Exercised<Value>> exerciseNestedExternalCall(
        String innerActor, String input1, String input2) {
      return exerciseNestedExternalCall(new NestedExternalCall(innerActor, input1, input2));
    }

    default Update<Exercised<String>> exerciseDelegatedExternalCall(
        DelegatedExternalCall arg) {
      return makeExerciseCmd(CHOICE_DelegatedExternalCall, arg);
    }

    default Update<Exercised<String>> exerciseDelegatedExternalCall(
        String actor, String input) {
      return exerciseDelegatedExternalCall(new DelegatedExternalCall(actor, input));
    }

    default Update<Exercised<String>> exerciseCallInLeafOnly(CallInLeafOnly arg) {
      return makeExerciseCmd(CHOICE_CallInLeafOnly, arg);
    }

    default Update<Exercised<String>> exerciseCallInLeafOnly(String input) {
      return exerciseCallInLeafOnly(new CallInLeafOnly(input));
    }

    default Update<Exercised<String>> exerciseLeafExternalCall(LeafExternalCall arg) {
      return makeExerciseCmd(CHOICE_LeafExternalCall, arg);
    }

    default Update<Exercised<String>> exerciseLeafExternalCall(String input) {
      return exerciseLeafExternalCall(new LeafExternalCall(input));
    }

    default Update<Exercised<Value>> exerciseCallAtAllLevels(CallAtAllLevels arg) {
      return makeExerciseCmd(CHOICE_CallAtAllLevels, arg);
    }

    default Update<Exercised<Value>> exerciseCallAtAllLevels(String input) {
      return exerciseCallAtAllLevels(new CallAtAllLevels(input));
    }

    default Update<Exercised<Value>> exerciseMiddleLevelCall(MiddleLevelCall arg) {
      return makeExerciseCmd(CHOICE_MiddleLevelCall, arg);
    }

    default Update<Exercised<Value>> exerciseMiddleLevelCall(String input) {
      return exerciseMiddleLevelCall(new MiddleLevelCall(input));
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
            ExternalCallContract,
            ?>
        getCompanion() {
      return COMPANION;
    }

    public ExternalCallInterface.CreateAnd toInterface(
        ExternalCallInterface.INTERFACE_ interfaceCompanion) {
      return new ExternalCallInterface.CreateAnd(COMPANION, this.createArguments);
    }
  }
}
