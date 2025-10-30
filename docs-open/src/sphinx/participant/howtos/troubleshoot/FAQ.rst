..
   Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. wip::
    Merge with troubleshooting_guide and review if still accurate.

.. _faq:

Canton Frequently Asked Questions
=================================

This section covers other questions that frequently arise when using Canton. If your question is not answered here,
consider searching the `Daml forum <https://discuss.daml.com>`_ and creating a post if you can't find the answer.

Log Messages
------------

.. _database_task_queue_full:

Database task queue full
~~~~~~~~~~~~~~~~~~~~~~~~

If you see the log message::

    java.util.concurrent.RejectedExecutionException:
    Task slick.basic.BasicBackend$DatabaseDef$@... rejected from slick.util.AsyncExecutorWithMetrics$$...
    [Running, pool size = 25, active threads = 25, queued tasks = 1000, completed tasks = 181375]

It is likely that the database task queue is full. You can check this by inspecting the log message: if the logged
``queued tasks`` is equal to the limit for the database task queue, then the task queue is full. This error message does
not indicate that anything is broken, and the task will be retried after a delay.

If the error occurs frequently, consider increasing the size of the task queue:

.. literalinclude:: CANTON/scripts/canton-testing/config/participants.conf
   :start-after: user-manual-entry-begin: DbTaskQueueSize
   :end-before: user-manual-entry-end: DbTaskQueueSize

A higher queue size can lead to better performance, because it avoids the overhead of retrying tasks;
on the flip side, a higher queue size comes with higher memory usage.

.. _serialization_exception:

Serialization Exception
~~~~~~~~~~~~~~~~~~~~~~~

In some situations, you might observe the following log message in your log file or in the database log file:

.. code::

    2022-08-18 09:32:39,150 [⋮] INFO  c.d.c.r.DbStorageSingle - Detected an SQLException. SQL state: 40001, error code: 0
        org.postgresql.util.PSQLException: ERROR: could not serialize access due to concurrent update

This message is normally harmless and indicates that two concurrent queries tried to update a database row and due to
the isolation level used, one of them failed. Currently, there are a few places where we use such queries.
The Postgres manual will tell you that an application should just retry this query. This is what Canton does.

Canton's general strategy with database errors is to retry retryable errors until the query succeeds. If the retry does
not succeed within a few seconds, a warning is emitted, but the query is still retried.

This means that even if you turn off the database under full load for several hours, under normal circumstances Canton will immediately recover once database access has been restored. There is no reason to be concerned functionally with respect to this message. As long as the message is
logged on INFO level, everything is running fine.

However, if the message starts to appear often in your log files, you might want to check the database query
latencies, number of database connections and the database load, as this might indicate an overloaded database.

Console Commands
----------------

I received an error saying that the SynchronizerAlias I used was too long. Where I can see the limits of String types in Canton?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Generally speaking, you don't need to worry about too-long Strings as Canton will exit in a safe manner, and return an error message
specifying the String you gave, its length and the maximum length allowed in the context the error occurred.
Nonetheless, `the known subclasses of LengthLimitedStringWrapper <https://docs.daml.com/__VERSION__/canton/scaladoc/com/digitalasset/canton/config/RequireTypes$$LengthLimitedStringWrapper.html>`__ and
`the type aliases defined in the companion object of LengthLimitedString <https://docs.daml.com/__VERSION__/canton/scaladoc/com/digitalasset/canton/config/RequireTypes$$LengthLimitedString$.html>`__ list the limits of String types in Canton.

Bootstrap Scripts
-----------------

Why do you have an additional new line between each line in your example scripts?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When we write ``participant1 start`` the scala compiler translates this into ``participant1.start()``.
This works great in the console when each line is parsed independently.
However with a script all of its content is parsed at once, and in which case if there is anything on the line
following ``participant1 start`` it will assume it is an argument for ``start`` and fail.
An additional newline prevents this.
Adding parenthesis would also work.

How can I use nested import statements to split my script into multiple files?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Ammonite supports splitting scripts into several files using two mechanisms. The old one is
``interp.load.module(..)``. The new one is ``import $file.<fname>``. The former will compile the module as a whole,
which means that variables defined in one module cannot be used in another one as they are not available during
compilation. The ``import $file.`` syntax however will make all variables accessible in the importing script. However,
it only works with relative paths as e.g. ``../path/to/foo/bar.sc`` needs to be converted into
``import $file.^.path.to.foo.bar`` and it only works if the script file is named with suffix ``.sc``.

How do I write data to a file and how do I read it back?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- Canton uses `Protobuf <https://developers.google.com/protocol-buffers/>`__ for serialization
  and as a result, you can leverage Protobuf to write objects to a file.
  Here is a basic example:

.. literalinclude:: CANTON/community/app/src/test/scala/com/digitalasset/canton/integration/tests/DumpIntegrationTest.scala
   :start-after: architecture-handbook-entry-begin: DumpLastSequencedEventToFile
   :end-before: architecture-handbook-entry-end: DumpLastSequencedEventToFile
   :dedent:

- You can also dump several objects to the same file:

.. literalinclude:: CANTON/community/app/src/test/scala/com/digitalasset/canton/integration/tests/DumpIntegrationTest.scala
   :start-after: architecture-handbook-entry-begin: DumpAllSequencedEventsToFile
   :end-before: architecture-handbook-entry-end: DumpAllSequencedEventsToFile
   :dedent:

- Some classes do not have a (public) ``toProto*`` method, but they can be serialized to a
  `ByteString <https://developers.google.com/protocol-buffers/docs/reference/java/com/google/protobuf/ByteString>`__
  instead. You can dump the corresponding instances as follows:

.. literalinclude:: CANTON/community/app/src/test/scala/com/digitalasset/canton/integration/tests/DumpIntegrationTest.scala
   :start-after: architecture-handbook-entry-begin: DumpAcsCommitmentToFile
   :end-before: architecture-handbook-entry-end: DumpAcsCommitmentToFile
   :dedent:

Why is Canton complaining about my database version?
----------------------------------------------------

Postgres
~~~~~~~~

Canton is tested with the following PostgreSQL releases:  12, 13, 14, and 15.
Newer PostgreSQL releases may not have been tested. The recommended version
is 12. Canton WARNs when a higher version is encountered. By default,
Canton does not start when the PostgreSQL version is below 10.

Oracle
~~~~~~

Canton Enterprise additionally supports using Oracle for storage. Only Oracle 19 has been tested, so by default Canton
will not start if the Oracle version is not 19.

Note that Canton's version checks use the ``v$$version`` table so, for the version check to succeed,
this table must exist and the database user must have ``SELECT`` privileges on the table.

Using non-standard database versions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Canton's database version checks can be disabled with the following config option:

.. literalinclude:: CANTON/community/app/src/test/resources/documentation-snippets/non-standard-config.conf

Note that this will disable all "standard config" checks, not just those for the database.

.. _how-do-i-enable-unsupported-features:

How do I enable unsupported features?
-------------------------------------

Some alpha / beta features require you to explicitly enable them. However, please note that none of them
are supported by us in our commercial product and that turning them on will very much likely break your
system:

.. literalinclude:: CANTON/community/app/src/test/resources/documentation-snippets/dev-version-support.conf

.. _how-do-i-enable-alpha-features:

How do I enable alpha features?
---------------------------------
For early access to new features, you need to enable alpha version support. An alpha protocol version allows the use of features with initial availability.
Note that IA features are not generally supported. Customers using Daml Enterprise may be able to get commercial support for IA features
by explicit agreement with Digital Asset. Please contact your relationship manager to discuss any such arrangement.

If you have enabled `dev version support`, you can use the alpha protocol versions without any additional configuration.

.. todo::
    `#22917: Fix broken literalinclude <https://github.com/DACH-NY/canton/issues/22917>`_
    literalinclude:: CANTON/community/app/src/test/resources/documentation-snippets/beta-version-support.conf


How to troubleshoot included configuration files?
----------------------------------------------------

If Canton is unable to find included configuration files, please read the section on :ref:`including configuration files <include_configuration>`
and the `HOCON specification <https://github.com/lightbend/config/blob/master/HOCON.md#include-semantics-locating-resources>`__.
Additionally, you may run Canton with ``-Dconfig.trace=loads`` to get trace information when the configuration is parsed.
