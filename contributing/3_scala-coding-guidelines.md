Scala Coding Guidelines
=======================

If you are not strongly familiar with programming in Scala, we recommend reading
[Twitter's Effective Scala guide](http://twitter.github.io/effectivescala).
We do not follow all recommendations literally, but it is a good starting point to understand common pitfalls.

Below, you'll find our own guidelines for some very important topics.
Please make sure you read and understand them.

1. [Error Handling](#error-handling)
2. [Logging](#logging)
3. [Serialization](#serialization)
4. [Code Documentation](#code-documentation)
5. [Cats](#cats)
6. [TODO comments](#todo-comments)
7. [Internal Classes versus API Classes](#internal-classes-versus-api-classes)
8. [sbt](#sbt)

# Error Handling

## Rules by Error Kind
Generally, we distinguish the following kinds of errors.

**User error:** the user provides invalid input to the system. Canton must fail gracefully.
* Make sure the error is logged (with INFO). Try to log it exactly once (on the server)
  using APIs such as `CantonError.logOnCreation` or `CantonError.log`.
* Use `Left(...)` to pass the error information so that Canton can output a descriptive error message to the user.
* Do not throw an exception.
* Do not crash or otherwise degrade the system.

**Internal error:** malfunction due to a bug in the code. Canton must inform the user and minimize the damage.
* Log an error at least once. (More than once is ok.) Include a stack trace into the log message.
  Use utility methods such as `ErrorUtil.internalError`.
* By default, throw an exception to abort the underlying request and (if necessary) stop the underlying component.
  If in doubt, it is better to crash than to continue running with a corrupt state.
* That said, try to not crash unnecessarily.
  For example, if an exception occurs while rendering a log message, it makes sense to catch and log the exception and keep going.
* Do not report internal errors with `Left(...)` as this will clutter the API without improving resilience.

**Operational error:** malfunction of some other component (e.g. database).
Canton must inform the user about the impact of the error and strive for automatic recovery.
* If the error is recoverable, proceed as for user errors.
* If the error is fatal, proceed as for internal errors.

**Malicious or Faulty Counterparty** some other entity in the distributed system has sent some invalid data.
Canton must be resilient to such situations.
* Always validate input coming from untrusted sources. On failure, create an instance of `BaseAlarm` and call `report` or `reported`.
* Also, if input validation fails, strive for graceful degradation so that an attacker cannot abuse input validations to run a denial of service attack.

## Exceptions to the General Rules

In the following situations, it is ok to throw exceptions on user errors / recoverable operational errors:
* In tests
* Most 3rd party libraries (gRPC, JDBC, PureConfig) use exceptions for all kinds of errors.
  If such exceptions are caused by user errors / recoverable operational errors,
  catch such exceptions and translate them to `Left(...)`.
* In the Canton Console, we use exceptions to indicate user errors.
  More precisely, an error during the execution of a console command should be signalled using `ConsoleEnvironment.raiseError`.
  This will properly find the call site, log at ERROR level and throw a `CommandFailure` exception.

## Rules About Exceptions

**Throwing exceptions:**
* Recovery from unexpected exceptions is ad hoc.
  Therefore, use exceptions only as a **last resort,** as unexpected exceptions can crash the node or make it unusable.
* Log an error when throwing an exception, provided a logger is in scope.
  (Because the exception may be silently discarded.)

**Avoiding unexpected exceptions:**
* If a method has a precondition (e.g. `foo(n)` throws an exception if `n < 0`), prefix its name with `try`.
    * Exception: This rule does not apply to console commands (i.e., members of `c.d.c.console.command` and `ConsoleMacros`).
* If a method calls `tryFoo(...)` and the precondition of `tryFoo(...)` is always met,
  call it through `checked` as a convention to mark the call as safe, i.e., `checked { tryFoo(...) }`.
  Leave a comment explaining why the precondition is always met.
* If a method calls `tryFoo(...)` and the precondition may be violated, prefix the name of the calling method with `try`.
* Strive for a design with few and simple preconditions, i.e., avoid methods with prefix `try`.

**Handling exceptions:**
* Log an error when catching an exception.
* Do not discard a `Try` or `Future`, because that would silently discard any embedded exception.
  Use methods such as `FutureUtil.doNotAwait` instead to ensure that embedded exceptions get logged.
* In general match exceptions in Scala with `NonFatal`.
  If you catch a fatal throwable (e.g. `OutOfMemoryError`), make sure to throw it again.
* When receiving a function parameter, carefully plan if any exception thrown by the function
  1. should be caught and discarded (so the recipient of the function keeps running) versus
  2. should crash the recipient as well.

  Each approach has its pros and cons so you need to choose carefully.
* Make sure that any framework that initiates a computation (`App`, gRPC service implementations, execution contexts, ...)
  has an appropriate default throwable handler in place.

## Object invariants

To enforce object invariants, you may want to throw exceptions in constructors, like here:

```scala
final case class MyStructure(i: Int) {
  require(i >= 42, s"Parameter should not be smaller than 42, found: $i")
}
```

However, if the parameter `i` comes from the user, then an invalid value would be a user error.
According to the guidelines, you shouldn't throw an exception in that case.

Instead, use a private constructor and a factory method to enforce the object invariant:
```scala
final case class MyStructure private (i: Int)
object MyStructure {
  def create(i: Int): Either[String, MyStructure] = {
    if(i >= 42) Right(MyStructure(i))
    else Left(s"Parameter should not be smaller than 42, found: $i")
  }
  def tryCreate(i: Int): MyStructure = create.valueOr(err => throw new IllegalArgumentException(err))
}
```

## Error Codes
Error codes are identifiers (UPPER_CAPS_STRINGS) that uniquely identify Canton errors in a machine-readable way which stays persistent across versions.
Eventually, we would like all the user facing warning, error messages and log messages with WARN and ERROR, to have an error code associated with them.
The goal is to arrive at a user manual that documents each error code and has a mapping of 'error code' -> 'what you need to do' (compare e.g. [this](https://www.cisco.com/c/en/us/td/docs/net_mgmt/converged_network_solutions_center_subscriber_provisioning/2-0/user/guide/app_err.html) and [our current error code section in the user manual](https://docs.daml.com/canton/reference/error_codes.html)).
Concretely, in our code this takes the form of `BaseCantonError` and `ErrorCode` - see the documentation there for more information.

# Logging

Choose log levels as follows:
* ERROR is to be used for unexpected problems that compromise availability and/or data integrity.
    * An unexpected exception has been observed.
    * The contract store is corrupt.
    * The database is permanently unavailable.
    * A participant is receiving non-increasing timestamps from a sequencer.
    * Special case: If a console command has failed, the **console** must log an error as well.
      (Because a failed console command is typically triggered by a bug in the console script.)
      The server must log this with INFO level.
      (Because there is no bug in the server.)
* WARN is to be used for expected problems that lead to some degradation but can still be recovered from.
    * A participant got disconnected from a synchronizer. Trying to reconnect...
    * Database transaction failed. Retrying...
    * A request has timed out.
    * A node failed to validate a request coming from an untrusted source.
* INFO is to be used for any event that is somewhat remarkable, in particular configuration changes.
    * A participant has registered a new Daml archive.
    * A participant has rejected a command.
    * A participant has detected that a transaction uses an inactive contract.
* DEBUG everything else that supports a post-mortem analysis of production systems.
    * A command has been submitted.
    * A command has been accepted.
* TRACE messages that are only needed for debugging tests.

Note that we can lower the log levels of libraries if they are too noisy.
Have a look at `rewrite-appender.xml` (change the log level of individual messages), `logback.xml` (production logging), and `logback-test.xml` (test logging).

Also make sure to create meaningful log messages:
* Always log the entry and exit point of an external API request. (See `ApiRequestLogger` as an example.)
* Emit the log message before performing an action, not after. I.e. "Starting server..." instead of "Server started".
* If available, include request id, command id, ... of the request.
* Use `NamedLogging` and ensure that the logger name identifies the underlying node (e.g. by including the participant id in the logger name).
* Try to provide at least some context information.
  I.e. instead of "Fetched contract XCVALDLJDFJDF2 from store",
  report "Fetched contract XCVALDLJDFJDF2 **of type PingPong.Pong** from store".

For security reasons:
* Never log confidential data to a log file.
  Don't dump passwords.
  Don't dump full command payloads or Daml transactions.
  Treat the logfile as an insecure store (because it is).

# Serialization

There are many ways to implement serialization, however,
please refer to the following "reference classes" when working on serialization;
and stick to that style for consistency in the code base.

* `InformeeMessage`: a simple example for getting started with serialization
* `SerializableContract`: use of an `UntypedVersionedMessage` wrapper when a class can be serialized to an anonymous binary format (`ByteString` or `Array[Byte]`)
* `ViewCommonData`: a more complex example demonstrating
    * memoized serialization, which is required if we need to compute a signature or cryptographic hash of a class
    * use of an `UntypedVersionedMessage` wrapper when a class can be serialized to an anonymous binary format (`ByteString` or `Array[Byte]`)
* `ConfirmationResponse`: a more complex example demonstrating
    * handling of object invariants (i.e., the construction of an instance may fail with an exception)
    * memoized serialization, which is required if we need to compute a signature or cryptographic hash of a class
* `Informee`: simple example of serializing a trait with different subclasses

## Chimney

The [Chimney](https://chimney.readthedocs.io/en/stable/) library can be handy for mapping between ScalaPB generated classes and the corresponding internal classes.
However, as Chimney uses compiler macros, the use of Chimney can make it hard to understand the data flow between the classes being mapped.
(If you run "Find Usage", you won't necessarily find the usage of the field by Chimney.)
This can be an obstacle to security reviews and therefore a source of vulnerabilities.

To avoid this, please adhere to the following simple rules:

1. Encode or decode ScalaPB classes to their corresponding internal class right at the API boundary (e.g. in `GrpcMyService`)
   or when accessing the database (e.g. in `DbMyStore`).
2. Similarly, do not mix ScalaPB classes with internal classes. I.e., do not create a class
   ```
   class MyMixedClass(
     val field1: MyInternalClass,
     val field2: MyScalaPBGeneratedClass,
   )
   ```
3. Use Chimney only in cases where Chimney can perform the required mapping (semi-)automatically.
   Conversely, if Chimney requires a lot of customization, please define the mapping without using Chimney.

## Deserialization

### Deserialization Validation

The deserialization method `fromByteString` takes a protocol version as a parameter and validates that the protocol
version is compatible with the one of the message.

For cases when this validation is not applicable, use one of these methods instead:

- `fromTrustedByteString`
- `fromTrustedByteArray`
- `readFromTrustedFile`
- `tryReadFromTrustedFile`

**But whenever you use a trusted deserialization method ensure that there is a proper justification for using it!**\
See the details below.

### Deserialization Validation Details

Consider the Protobuf message for versioned data structures
```
message UntypedVersionedMessage {
  oneof wrapper {
    bytes data = 1;
  }
  int32 version = 2;
}
```
where the `version` determines *how* the `data` is going to be deserialized by our tooling. Note that the `version`
integer can be any number and can also be easily modified with or without a malicious intent.

The deserialization validation enforces that this `version` number is compatible with the protocol version of the
synchronizer. The `HasProtocolVersionedWrapper`'s **deserialization method `fromByteString` implements this validation,
and should be your default choice**.

Using ***trusted deserialization methods*** is tolerable **only** in the following circumstances:

- when the message does not cross trust boundaries
  > For example, when two participants exchange a message then it crosses trust boundaries because participants do not trust
  each other. Conversely, no trust boundaries are crossed when a sequencer (de)serializes a message to/from its persistent
  store or when an administrator interacts with the node it administrates because that is assumed to be within the same
  trust boundary.

- for cases where a validation is not applicable because the `data`　message field is always (de)serialized with a
  fixed (hard-coded) Protobuf version
  > An example of this is the `SequencedEvent` instance as part of a `SignedContent` which (de)serializes its underlying
  data with a fixed Protobuf version.

- when there is circular dependency
  > As the validation depends on the synchronizer protocol version, any deserialization of configuration data which contains
  the synchronizer protocol version (while maybe bootstrapping a node) cannot use the validation (circular dependency).
  For example, the deserialization of `SynchronizerParameters` cannot be validated for that reason.

To summarize, using ***trusted deserialization methods*** is only tolerable in the following cases:
- admin console commands
- deserialization from the database or a file
- deserialization using a hard-coded protocol version
- circular dependency on the synchronizer protocol version

# Code Documentation

We use ScalaDoc to document public APIs.
ScalaDoc generation is verified within our build pipeline so you may find your build failing due to incorrect ScalaDocs.

Our Rules:

* Public APIs that are or could be used by other teams or components must have very comprehensive ScalaDoc comments
  so that clients can understand the behavior of the API without analysing the implementation.
* Any potential surprises (e.g. error cases, side effects) must be explained in the ScalaDoc
  so that clients can be aware of them without analysing the implementation.
* The same rules apply to internal APIs, but some familiarity with the implementation may be assumed.
* Complex object invariants of a field should be explained in the ScalaDoc of the field
  so that new contributors don't have to analyse every write access of the field.
* APIs used internally only with a straightforward semantics don't need to be documented.

Further info can be found in [scaladoc.md](scaladoc.md).

# Cats

We use [cats](https://typelevel.org/cats) to provide useful abstractions on functional programming, in particular
around `Future` and `Either`.

## Imports

Avoid importing all implicits with `import cats.implicits._` because this brings into scope too many instances
that shadow our own type class implementations, in particular for `Show`.

It usually suffices to import the specific syntax extension methods; instances are typically found automatically.
For example,

```scala
import cats.syntax.foldable.*
```

brings for example syntax for methods `sequence_` and `traverse_` for containers with a `Foldable` type class instance.
The scala compiler typically finds the type class instances automatically.
For further information on cats imports see [this guide](https://eed3si9n.com/herding-cats/import-guide.html#a+la+carte+style).

**Exceptions:**

* The `NonEmpty` instances must be imported with `import com.daml.nonempty.catsinstances.*`.
  Types such as `NonEmpty[List[X]]` must first be converted to `NonEmptyF[List, X]` using the `.toNEF` method.
  This ensures that scalac's type inference searches for correct type class instances.
  The method `.fromNEF` converts a `NonEmptyF[List, X]` to a `NonEmpty[List[X]]`.
* The `Parallel` instances for `Future` must be imported with `import com.digitalasset.canton.util.FutureInstances.*`.

## Controlling parallelism with Futures

When using Cats with future-like `Applicative` instances (`Future`, `FutureUnlessShutdown`, `EitherT[Future, _, _]`, ...),
Cats itself does not specify whether the computations run sequentially or concurrently,
and this may even change with different versions of Cats.
Our code therefore explicitly specifies whether parallel or sequential execution is desired:
Using the plain `traverse` and `traverse_` methods makes compilation fail.
For parallel execution, use the corresponding methods from `Parallel` such as `parTraverse` and `parTraverse_`.
For sequential execution, use the methods in `MonadUtil`.
You can use `traverse` and friends on the singleton containers `Option` and `Either` though
because there is no question of parallelism.

**Examples:**

```scala
(xs: Seq[A]).parTraverse(f)
MonadUtil.sequentialTraverse(xs)(f)
(xO: Option[A]).traverse(f)
```

# TODO Comments
TODO comments should have the form `TODO(#<issue-number>): <description>`, where
`<issue-number>` is the reference number of a GitHub issue.

A script is used to aggregate TODOs and detect problems such as TODOs without an issue number or referencing a closed issue.
The script uses a more complex pattern than described above to cover existing TODOs from the codebase,
but the pattern above should be used for all new TODOs.

# Internal Classes versus API Classes

We try to avoid using internal classes in user facing APIs (currently, the console and configuration),
because any change to internal classes (even renamings) could break our user facing API.

If a data structure needs to be exposed to a user facing API,
define a copy of the classes in one of the dedicated
packages (`com.digitalasset.canton.config` or `com.digitalasset.canton.admin.api.client.data`) and
provide transformations between the internal class and the corresponding API class.
The [Chimney](https://chimney.readthedocs.io/en/stable/) library can be helpful to provide those transformations.

We keep the user facing APIs stable by never changing API classes.
When an internal class is changed, its transformations need to be changed as well, as otherwise the codebase will no longer compile.

Examples of classes having internal and API versions are:

- `StaticSynchronizerParameters`
- `DynamicSynchronizerParameters`
- refined durations such as `NonNegativeInt`

# sbt

## Project dependencies

As best practice, always declare all the dependencies of an sbt target explicitly (e.g. via
``libraryDependencies += <required-dep>`` setting)
, even though some are inherited transitively from other projects (via ``dependsOn``).

For example, consider that ``community-base`` has ``circe-core`` as compile-time dependency and ``ledger-api-core``
depends on ``community-base``.
If ``ledger-api-core`` code uses ``circe-core``, it should declare it as dependency as well, even though it is already
available transitively via ``community-base``.
