..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _support_checklist:

Support Checklist
=================

So that we can successfully provide support, and to ensure that you can troubleshoot the system
efficiently on your own, we recommend that you follow the checklist below to ensure that you have access to
all the necessary information and tools to resolve an issue and that you have established the necessary administrative
access to your production system.

The :externalref:`troubleshooting guide <troubleshooting_guide>` goes into detail on how to use the information and tools
provided in this checklist.

Please note that the below list is used to prepare for the worst-case scenario and generally refers to good
practices for being prepared for any kind of issue.

.. _support_remote_console:

Remote Console Access
---------------------

* **Admin Console Access**

    You are able to connect to each Canton node in your setup using a :externalref:`remote console <canton_remote_console>`.

* **Check Node Health**

    Using the admin console, you can run `node.health.status` to check the health of the node.

* **Download the Health Dump**

    Using the admin console, you can run `node.health.dump()` to download a :externalref:`health dump <creating_dumps>` of the node.
    Note that for versions prior to 2.9, you might need to add a ``timeout=10.minutes`` argument to the command.

        * Confirm that you can download the health dump.
        * Confirm that you can upload the health dump to a support ticket.

* **Smoke Test**

    Using the admin console on a participant, you can ping the participant to run an end-to-end check:
    ``participant.health.ping(participant, id="READY-FOR-PRODUCTION")``

* **gRPC Ledger API Access**

    Using the console on a participant, you can access the gRPC Ledger API for further troubleshooting.
    For this, you need to be able to enable the flag ``canton.features.enable-testing-commands = yes``
    on your remote console.

        * Confirm access by successfully running the command ``participant.ledger_api.acs.of_party(participant.id.adminParty)``.

.. _support_logging:

Logging
-------

* **Access to Readable Logs**

    You are able to download the native Canton logs from your production system. This is relevant as the logs are highly
    structured and contain information that is sometimes omitted by third-party log aggregators.

        * The logs can be read :externalref:`using lnav<lnav>`.
        * They include the trace-id (``tid:``)
        * They include at least the log-level INFO.
        * Confirm that you can upload the log files to a support ticket.

    Note that you can set up Canton to create rolling log files which are included in the health dump. Both the standard
    and the default Canton JSON logging format are fine.
    If you need custom loggers, please add them as an extension to the built-in `logback.xml <https://github.com/digital-asset/canton/blob/main/community/app/src/main/resources/logback.xml>`_.

* **Attention to WARN and ERROR logs**

    You are monitoring the logs for WARN and ERROR messages. Canton logs WARN on degradations and ERROR on fatal failures.
    There should never be an ERROR in the logs emitted by the Canton loggers.

* **Finding Commands in the Logs**

    You are able to find the commands that were submitted to Canton in the logs.
    Verify that you can find the previous ping in your logs looking for ``READY-FOR-PRODUCTION`` (which
    was the argument passed as ``id``). You should see several log entries mentioning ``Phase 1 started: Submitting commands for interpretation``,
    one for each Daml transaction of the ping command.

* **Log File Permanence**

    You have a strategy in place to ensure that the logs are not lost when the node is restarted.

        * Restart your node and confirm that the logs of the ping command are still available.

* **Log Level Change**

    You are able to change the log level of the Canton loggers using the remote console command ``node.health.set_log_level(Level.DEBUG)``.

        * Confirm that the log level changes to DEBUG and change it back again.

* **Command Id / Trace Id in Application Logs**

.. todo:: `#22917: Fix broken ref <https://github.com/DACH-NY/canton/issues/22917>`_
    Your applications provide a trace-id or at least the command-id in their logs that can be correlated with the Canton logs.
    Please consult our ref:`guide on how to add end-to-end tracing <open-tracing-ledger-api-client>` to your application.
    * Verify that you can find the trace-id or command-id in your application logs and correlate it with the Canton logs.

.. _support_metrics:

Metrics
-------

* **Access to Metrics**

    You are able to extract key metrics from your production system, in particular the JVM metrics.

.. _support_configuration:

Configuration and Patching
--------------------------

* **Access to Configuration Files**

    You are able to edit and change the configuration files of your production system if necessary.

* **Patch Release Process**

    You have a well-defined procedure for applying patch releases to your production system.

Database Access
---------------

* **Access to the Database**

    You have a well-defined procedure for database access to run queries if necessary or access database statistics.

* **Ability to Snapshot**

    You are able to take a snapshot of the database and restore it to a different environment for troubleshooting
    and recovery procedure testing.

  **Established and Tested Backup Procedure**

    In worst case, you are able to restore the system to a working state using your automated backups.
