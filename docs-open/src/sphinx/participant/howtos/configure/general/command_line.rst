..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _command-line-arguments:

Use the Canton Command Line
===========================
Canton supports a variety of command line arguments. Please run ``bin/canton --help`` to see all of them. Here,
we explain the most relevant ones.

Selecting a Configuration
-------------------------
Canton requires a configuration file to run. There is no default topology configuration built in and
therefore, the user needs to at least define what kind of node (synchronizer or participant) and how many
they want to run in the given process. Sample configuration files can be found in our release package,
under the ``examples`` directory.

When starting Canton, configuration files can be provided using

.. code-block:: bash

    bin/canton --config conf_filename -c conf_filename2

which will start Canton by merging the content of ``conf_filename2`` into ``conf_filename``.
Both options ``-c`` and ``--config`` are equivalent.
If several configuration files assign values to the same key, the *last* value is taken.
The section on :ref:`static configuration <static_configuration>` explains how to write a configuration file.

You can also specify config parameters on the command line, alone or along with configuration files, to specify missing
parameters or to overwrite others. This can be useful for providing simple short config info.
Config parameters can be provided using ``-C``:

.. code-block:: bash

     bin/canton --config conf_filename -C canton.participants.participant1.storage.type=memory

Run Modes
---------
You can run Canton in various modes, depending on the desired environment and task.

Interactive Console
~~~~~~~~~~~~~~~~~~~
The default method to run Canton is in the interactive mode. The process will start
:ref:`a command line interface <canton_console>` (REPL) which allows to conveniently
operate, modify and inspect the Canton application.

In this mode, all errors will be reported as ``CommandExecutionException`` to the console, but Canton will remain running.

The interactive console can be started together with a script, using the ``--bootstrap=...`` option. The script uses
the same syntax as the console.

The interactive mode is useful for development, education and expert use.

Daemon
~~~~~~
If the console is undesired such as in server operation, Canton can be started in daemon mode

.. code-block:: bash

    bin/canton daemon --config ...

All configured entities will be automatically started and will resume operation.

A failure to connect to the database storage will lead the process to exit with a non-zero exit code. This
can be turned off using:

.. literalinclude:: CANTON/community/app/src/test/resources/documentation-snippets/no-fail-fast.conf

Any failures encountered while running the bootstrap script will immediately shutdown
the Canton process with a non-zero exit code.

Nodes started in daemon mode can be administrated by setting up
a :ref:`remote console <canton_remote_console>` that provides the interactive user experience,
while the nodes run in a separate process.

Sandbox
~~~~~~~
During development, it can be useful to run a lightweight ledger with a given DAR.
For this purpose, you can run Canton in sandbox mode, which runs the :externalref:`Daml Sandbox <sandbox-manual>`.

.. code-block:: bash

    bin/canton sandbox --dar Main.dar ...

Headless Script Mode
~~~~~~~~~~~~~~~~~~~~
For testing and scripting purposes, Canton can also start in headless script mode:

.. code-block:: bash

    bin/canton run <script-path> --config ...

In this case, commands are specified in a script rather than executed interactively. Any errors with the script or
during command execution should cause the Canton process to exit with a non-zero exit code. When the script completes
all components are stopped.

Interactive Server Process using Screen
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
In some situations, we find it convenient to run even a server process interactively.
For server use on Linux / OSX, this can be accomplished by using the
`screen <https://linux.die.net/man/1/screen>`__ command:

.. code-block:: bash

    screen -S canton -d -m ./bin/canton -c ...

will start the Canton process in a screen session named ``canton`` which does not terminate on user-logout and therefore
allows to inspect the Canton process whenever necessary.

A previously started process can be joined using

.. code-block:: bash

    screen -r canton

and an active screen session can be detached using CTRL-A + D (in sequence). Be careful and avoid typing CTRL-D, as it
will terminate the session. The screen session will continue to run even if you log out of the machine.

.. _jvm_arguments:

Java Virtual Machine Arguments
------------------------------
The ``bin/canton`` application is a convenient wrapper to start a Java virtual machine running the Canton process.
The wrapper supports providing additional JVM options using the ``JAVA_OPTS`` environment variable or
using the ``-D`` command line option.

For example, you can configure the heap size as follows:

.. code-block:: bash

    JAVA_OPTS="-Xmx2G" ./bin/canton --config ...

There are several log related options that can be specified. Refer to :ref:`Logging <logging>` for more details.
