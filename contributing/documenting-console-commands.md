Documenting Console Commands
============================

This guides explains how to create user facing documentation for Console commands.
The documentation is defined by using Scala annotations.
Our build infrastructure then converts them into the text output by the `help` commands
as well as the [reference documentation](https://docs.daml.com/canton/reference/console.html).

# Providing Documentation

To provide the documentation for a console command -- be it a top-level command such as `connect` or an instance specific command
such as `participant1 help` -- use the annotations from `Help.scala`.
In order for any help to be displayed, you must provide the `@Summary` annotation for your command.

**IMPORTANT:** all annotations must be given as literal values.
For example, `Summary("some summary")` works, while `Summary(" some summary ".trim)` will blow up during documentation generation.
In particular, topics must be literals of the form `Seq(string-literal1, string-literal2, ... string-literalN)`.
Most likely, the CI will catch such errors while running `build_docs`.
You can test locally by running `sbt packageDocs` or `sbt packageDocsWithExistingRelease`.

In the unlikely event that you create commands that are neither console macros nor attached to a synchronizer member (ParticipantReference, SequencerReference, MediatorReference)
you must exercise caution to ensure that the documentation of such commands appears in the help texts and reference documentation.

Example:
```scala
@Help.Summary(
      "Macro to connect a participant to a locally configured synchronizer given by sequencer reference"
    )
    @Help.Description("""
        The arguments are:
          sequencer - A local sequencer reference
          alias - The name you will be using to refer to this synchronizer. Cannot be changed anymore.
          manualConnect - Whether this connection should be handled manually and also excluded from automatic re-connect.
          maxRetryDelayMillis - Maximal amount of time (in milliseconds) between two connection attempts.
          priority - The priority of the synchronizer. The higher the more likely a synchronizer will be used.
          synchronize - A timeout duration indicating how long to wait for all topology changes to have been effected on all local nodes.
        """)
    def connect_local(
        sequencer: SequencerReference,
        alias: SynchronizerAlias,
        manualConnect: Boolean = false,
        maxRetryDelayMillis: Option[Long] = None,
        priority: Int = 0,
        synchronize: Option[NonNegativeDuration] = Some(
          consoleEnvironment.commandTimeouts.bounded
        ),
    ): Unit
```

# Further Reading

Further information on docs processing can be found in [this `README`](../docs-open/README.md).
