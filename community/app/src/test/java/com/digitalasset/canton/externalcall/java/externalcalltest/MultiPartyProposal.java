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

public final class MultiPartyProposal extends Template {
  public static final Identifier TEMPLATE_ID =
      new Identifier("#ExternalCallTest", "ExternalCallTest", "MultiPartyProposal");

  public static final Identifier TEMPLATE_ID_WITH_PACKAGE_ID =
      new Identifier(ExternalCallInterface.PACKAGE_ID, "ExternalCallTest", "MultiPartyProposal");

  public static final String PACKAGE_ID = ExternalCallInterface.PACKAGE_ID;

  public static final String PACKAGE_NAME = ExternalCallInterface.PACKAGE_NAME;

  public static final PackageVersion PACKAGE_VERSION = ExternalCallInterface.PACKAGE_VERSION;

  public static final Choice<MultiPartyProposal, Archive, Unit> CHOICE_Archive =
      Choice.create(
          "Archive",
          Archive::toValue,
          Archive.valueDecoder(),
          PrimitiveValueDecoders.fromUnit,
          new Archive.JsonDecoder$().get(),
          JsonLfDecoders.unit,
          Archive::jsonEncoder,
          JsonLfEncoders::unit);

  public static final Choice<MultiPartyProposal, Accept, MultiPartyExternalCall.ContractId>
      CHOICE_Accept =
          Choice.create(
              "Accept",
              Accept::toValue,
              Accept.valueDecoder(),
              ValueDecoder.create(
                  (value, policy) ->
                      new MultiPartyExternalCall.ContractId(
                          value.asContractId().orElseThrow().getValue())),
              new Accept.JsonDecoder$().get(),
              JsonLfDecoders.contractId(MultiPartyExternalCall.ContractId::new),
              Accept::jsonEncoder,
              JsonLfEncoders::contractId);

  public static final ContractCompanion.WithoutKey<Contract, ContractId, MultiPartyProposal>
      COMPANION =
          new ContractCompanion.WithoutKey<>(
              ExternalCallTestCodegenShim.PACKAGE,
              "com.digitalasset.canton.externalcall.java.externalcalltest.MultiPartyProposal",
              TEMPLATE_ID,
              ContractId::new,
              MultiPartyProposal::fromJson,
              Contract::new,
              List.of(CHOICE_Archive, CHOICE_Accept),
              MultiPartyProposal.templateValueDecoder());

  public final String proposer;

  public final String acceptor;

  public final List<String> observers_;

  public MultiPartyProposal(String proposer, String acceptor, List<String> observers_) {
    this.proposer = proposer;
    this.acceptor = acceptor;
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
  protected ContractCompanion.WithoutKey<Contract, ContractId, MultiPartyProposal>
      getCompanion() {
    return COMPANION;
  }

  public static ValueDecoder<MultiPartyProposal> valueDecoder() {
    return ContractCompanion.valueDecoder(COMPANION);
  }

  @Override
  public DamlRecord toValue() {
    ArrayList<DamlRecord.Field> fields = new ArrayList<>(3);
    fields.add(new DamlRecord.Field("proposer", new Party(this.proposer)));
    fields.add(new DamlRecord.Field("acceptor", new Party(this.acceptor)));
    fields.add(
        new DamlRecord.Field(
            "observers_",
            this.observers_.stream().collect(DamlCollectors.toDamlList(Party::new))));
    return new DamlRecord(fields);
  }

  private static ValueDecoder<MultiPartyProposal> templateValueDecoder() {
    return ValueDecoder.create(
        (value$, policy$) -> {
          PreparedRecord preparedRecord$ =
              PrimitiveValueDecoders.checkAndPrepareRecord(3, 0, value$, policy$);
          List<DamlRecord.Field> fields$ = preparedRecord$.getExpectedFields();
          String proposer =
              PrimitiveValueDecoders.fromParty.decode(fields$.get(0).getValue(), policy$);
          String acceptor =
              PrimitiveValueDecoders.fromParty.decode(fields$.get(1).getValue(), policy$);
          List<String> observers_ =
              PrimitiveValueDecoders.fromList(PrimitiveValueDecoders.fromParty)
                  .decode(fields$.get(2).getValue(), policy$);
          return new MultiPartyProposal(proposer, acceptor, observers_);
        });
  }

  public static JsonLfDecoder<MultiPartyProposal> jsonDecoder() {
    return JsonLfDecoders.record(
        Arrays.asList("proposer", "acceptor", "observers_"),
        name -> {
          switch (name) {
            case "proposer":
              return JsonLfDecoders.JavaArg.at(0, JsonLfDecoders.party);
            case "acceptor":
              return JsonLfDecoders.JavaArg.at(1, JsonLfDecoders.party);
            case "observers_":
              return JsonLfDecoders.JavaArg.at(2, JsonLfDecoders.list(JsonLfDecoders.party));
            default:
              return null;
          }
        },
        args ->
            new MultiPartyProposal(
                JsonLfDecoders.cast(args[0]),
                JsonLfDecoders.cast(args[1]),
                JsonLfDecoders.cast(args[2])));
  }

  public static MultiPartyProposal fromJson(String json) throws JsonLfDecoder.Error {
    return jsonDecoder().decode(new JsonLfReader(json), UnknownTrailingFieldPolicy.STRICT);
  }

  public static MultiPartyProposal fromJson(String json, UnknownTrailingFieldPolicy policy)
      throws JsonLfDecoder.Error {
    return jsonDecoder().decode(new JsonLfReader(json), policy);
  }

  @Override
  public JsonLfEncoder jsonEncoder() {
    return JsonLfEncoders.record(
        JsonLfEncoders.Field.of("proposer", apply(JsonLfEncoders::party, proposer)),
        JsonLfEncoders.Field.of("acceptor", apply(JsonLfEncoders::party, acceptor)),
        JsonLfEncoders.Field.of(
            "observers_", apply(JsonLfEncoders.list(JsonLfEncoders::party), observers_)));
  }

  public static final class ContractId
      extends com.daml.ledger.javaapi.data.codegen.ContractId<MultiPartyProposal>
      implements Exercises<ExerciseCommand> {
    public ContractId(String contractId) {
      super(contractId);
    }

    @Override
    protected ContractTypeCompanion<
            ? extends com.daml.ledger.javaapi.data.codegen.Contract<ContractId, ?>,
            ContractId,
            MultiPartyProposal,
            ?>
        getCompanion() {
      return COMPANION;
    }
  }

  public static final class Contract
      extends com.daml.ledger.javaapi.data.codegen.Contract<ContractId, MultiPartyProposal> {
    public Contract(
        ContractId id, MultiPartyProposal data, Set<String> signatories, Set<String> observers) {
      super(id, data, signatories, observers);
    }

    @Override
    protected ContractCompanion<Contract, ContractId, MultiPartyProposal> getCompanion() {
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

    default Update<Exercised<MultiPartyExternalCall.ContractId>> exerciseAccept(Accept arg) {
      return makeExerciseCmd(CHOICE_Accept, arg);
    }

    default Update<Exercised<MultiPartyExternalCall.ContractId>> exerciseAccept() {
      return exerciseAccept(new Accept());
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
            MultiPartyProposal,
            ?>
        getCompanion() {
      return COMPANION;
    }
  }
}
