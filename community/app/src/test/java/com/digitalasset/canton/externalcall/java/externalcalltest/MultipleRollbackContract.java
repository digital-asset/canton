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

public final class MultipleRollbackContract extends Template {
  public static final Identifier TEMPLATE_ID =
      new Identifier("#ExternalCallTest", "ExternalCallTest", "MultipleRollbackContract");

  public static final Identifier TEMPLATE_ID_WITH_PACKAGE_ID =
      new Identifier(
          ExternalCallInterface.PACKAGE_ID, "ExternalCallTest", "MultipleRollbackContract");

  public static final String PACKAGE_ID = ExternalCallInterface.PACKAGE_ID;

  public static final String PACKAGE_NAME = ExternalCallInterface.PACKAGE_NAME;

  public static final PackageVersion PACKAGE_VERSION = ExternalCallInterface.PACKAGE_VERSION;

  public static final Choice<MultipleRollbackContract, Archive, Unit> CHOICE_Archive =
      Choice.create(
          "Archive",
          Archive::toValue,
          Archive.valueDecoder(),
          PrimitiveValueDecoders.fromUnit,
          new Archive.JsonDecoder$().get(),
          JsonLfDecoders.unit,
          Archive::jsonEncoder,
          JsonLfEncoders::unit);

  public static final Choice<MultipleRollbackContract, MultipleRollbackScopes, Value>
      CHOICE_MultipleRollbackScopes =
          Choice.create(
              "MultipleRollbackScopes",
              MultipleRollbackScopes::toValue,
              MultipleRollbackScopes.valueDecoder(),
              ExternalCallTestCodegenShim.rawValueDecoder(),
              new MultipleRollbackScopes.JsonDecoder$().get(),
              ExternalCallTestCodegenShim.unsupportedJsonDecoder(
                  "MultipleRollbackScopes result"),
              MultipleRollbackScopes::jsonEncoder,
              ExternalCallTestCodegenShim.unsupportedJsonEncoder(
                  "MultipleRollbackScopes result"));

  public static final Choice<MultipleRollbackContract, NestedRollbackScopes, Value>
      CHOICE_NestedRollbackScopes =
          Choice.create(
              "NestedRollbackScopes",
              NestedRollbackScopes::toValue,
              NestedRollbackScopes.valueDecoder(),
              ExternalCallTestCodegenShim.rawValueDecoder(),
              new NestedRollbackScopes.JsonDecoder$().get(),
              ExternalCallTestCodegenShim.unsupportedJsonDecoder("NestedRollbackScopes result"),
              NestedRollbackScopes::jsonEncoder,
              ExternalCallTestCodegenShim.unsupportedJsonEncoder("NestedRollbackScopes result"));

  public static final ContractCompanion.WithoutKey<Contract, ContractId, MultipleRollbackContract>
      COMPANION =
          new ContractCompanion.WithoutKey<>(
              ExternalCallTestCodegenShim.PACKAGE,
              "com.digitalasset.canton.externalcall.java.externalcalltest.MultipleRollbackContract",
              TEMPLATE_ID,
              ContractId::new,
              MultipleRollbackContract::fromJson,
              Contract::new,
              List.of(CHOICE_Archive, CHOICE_MultipleRollbackScopes, CHOICE_NestedRollbackScopes),
              MultipleRollbackContract.templateValueDecoder());

  public final String owner;

  public MultipleRollbackContract(String owner) {
    this.owner = owner;
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
  protected ContractCompanion.WithoutKey<Contract, ContractId, MultipleRollbackContract>
      getCompanion() {
    return COMPANION;
  }

  public static ValueDecoder<MultipleRollbackContract> valueDecoder() {
    return ContractCompanion.valueDecoder(COMPANION);
  }

  @Override
  public DamlRecord toValue() {
    ArrayList<DamlRecord.Field> fields = new ArrayList<>(1);
    fields.add(new DamlRecord.Field("owner", new Party(this.owner)));
    return new DamlRecord(fields);
  }

  private static ValueDecoder<MultipleRollbackContract> templateValueDecoder() {
    return ValueDecoder.create(
        (value$, policy$) -> {
          PreparedRecord preparedRecord$ =
              PrimitiveValueDecoders.checkAndPrepareRecord(1, 0, value$, policy$);
          List<DamlRecord.Field> fields$ = preparedRecord$.getExpectedFields();
          return new MultipleRollbackContract(
              PrimitiveValueDecoders.fromParty.decode(fields$.get(0).getValue(), policy$));
        });
  }

  public static JsonLfDecoder<MultipleRollbackContract> jsonDecoder() {
    return JsonLfDecoders.record(
        Arrays.asList("owner"),
        name -> {
          switch (name) {
            case "owner":
              return JsonLfDecoders.JavaArg.at(0, JsonLfDecoders.party);
            default:
              return null;
          }
        },
        args -> new MultipleRollbackContract(JsonLfDecoders.cast(args[0])));
  }

  public static MultipleRollbackContract fromJson(String json) throws JsonLfDecoder.Error {
    return jsonDecoder().decode(new JsonLfReader(json), UnknownTrailingFieldPolicy.STRICT);
  }

  public static MultipleRollbackContract fromJson(String json, UnknownTrailingFieldPolicy policy)
      throws JsonLfDecoder.Error {
    return jsonDecoder().decode(new JsonLfReader(json), policy);
  }

  @Override
  public JsonLfEncoder jsonEncoder() {
    return JsonLfEncoders.record(
        JsonLfEncoders.Field.of("owner", apply(JsonLfEncoders::party, owner)));
  }

  public static final class ContractId
      extends com.daml.ledger.javaapi.data.codegen.ContractId<MultipleRollbackContract>
      implements Exercises<ExerciseCommand> {
    public ContractId(String contractId) {
      super(contractId);
    }

    @Override
    protected ContractTypeCompanion<
            ? extends com.daml.ledger.javaapi.data.codegen.Contract<ContractId, ?>,
            ContractId,
            MultipleRollbackContract,
            ?>
        getCompanion() {
      return COMPANION;
    }
  }

  public static final class Contract
      extends com.daml.ledger.javaapi.data.codegen.Contract<ContractId, MultipleRollbackContract> {
    public Contract(
        ContractId id,
        MultipleRollbackContract data,
        Set<String> signatories,
        Set<String> observers) {
      super(id, data, signatories, observers);
    }

    @Override
    protected ContractCompanion<Contract, ContractId, MultipleRollbackContract> getCompanion() {
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

    default Update<Exercised<Value>> exerciseMultipleRollbackScopes(
        MultipleRollbackScopes arg) {
      return makeExerciseCmd(CHOICE_MultipleRollbackScopes, arg);
    }

    default Update<Exercised<Value>> exerciseMultipleRollbackScopes(String input) {
      return exerciseMultipleRollbackScopes(new MultipleRollbackScopes(input));
    }

    default Update<Exercised<Value>> exerciseNestedRollbackScopes(NestedRollbackScopes arg) {
      return makeExerciseCmd(CHOICE_NestedRollbackScopes, arg);
    }

    default Update<Exercised<Value>> exerciseNestedRollbackScopes(String input) {
      return exerciseNestedRollbackScopes(new NestedRollbackScopes(input));
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
            MultipleRollbackContract,
            ?>
        getCompanion() {
      return COMPANION;
    }
  }
}
