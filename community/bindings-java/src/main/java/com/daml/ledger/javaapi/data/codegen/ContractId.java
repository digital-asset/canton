// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data.codegen;

import com.daml.ledger.javaapi.data.ExerciseCommand;
import com.daml.ledger.javaapi.data.Value;
import java.util.Objects;

/**
 * This class is used as a super class for all concrete ContractIds generated by the java codegen
 * with the following properties:
 *
 * <pre>
 * Foo.ContractId fooCid = new Foo.ContractId("test");
 * Bar.ContractId barCid = new Bar.ContractId("test");
 * ContractId&lt;Foo&gt; genericFooCid = new ContractId&lt;&gt;("test");
 * ContractId&lt;Foo&gt; genericBarCid = new ContractId&lt;&gt;("test");
 *
 * fooCid.equals(genericFooCid) == true;
 * genericFooCid.equals(fooCid) == true;
 *
 * fooCid.equals(barCid) == false;
 * barCid.equals(fooCid) == false;
 * </pre>
 *
 * Due to erase, we cannot distinguish ContractId&lt;Foo&gt; from ContractId&lt;Bar&gt;, thus:
 *
 * <pre>
 * fooCid.equals(genericBarCid) == true
 * genericBarCid.equals(fooCid) == true
 *
 * genericFooCid.equals(genericBarCid) == true
 * genericBarCid.equals(genericFooCid) == true
 * </pre>
 *
 * @param <T> A template type
 */
public class ContractId<T> implements Exercises<ExerciseCommand> {
  public final String contractId;

  public ContractId(String contractId) {
    this.contractId = contractId;
  }

  public final Value toValue() {
    return new com.daml.ledger.javaapi.data.ContractId(contractId);
  }

  @Override
  public <A, R> Update<Exercised<R>> makeExerciseCmd(
      Choice<?, ? super A, R> choice, A choiceArgument) {
    var command =
        new ExerciseCommand(
            getCompanion().TEMPLATE_ID,
            contractId,
            choice.name,
            choice.encodeArg.apply(choiceArgument));
    return new Update.ExerciseUpdate<>(command, x -> x, choice.returnTypeDecoder);
  }

  // overridden by every code generator, but decoding abstractly can e.g.
  // produce a ContractId<Foo> that is not a Foo.ContractId
  /**
   * <strong>INTERNAL API</strong>: this is meant for use by {@link ContractId} and <em>should not
   * be referenced directly</em>. Applications should refer to generated {@code exercise} methods
   * instead.
   *
   * @hidden
   */
  protected ContractTypeCompanion<
          ? extends Contract<? extends ContractId<T>, ?>, ? extends ContractId<T>, T, ?>
      getCompanion() {
    throw new UnsupportedOperationException(
        "Cannot exercise on a contract ID type without code-generated exercise methods");
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null
        || !(getClass().isAssignableFrom(o.getClass())
            || o.getClass().isAssignableFrom(getClass()))) return false;
    ContractId<?> that = (ContractId<?>) o;
    return contractId.equals(that.contractId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(contractId);
  }

  @Override
  public String toString() {
    return "ContractId(" + contractId + ')';
  }
}
