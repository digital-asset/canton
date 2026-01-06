..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _error_codes:

Error Codes
###########

Overview
********


.. _gRPC status codes: https://grpc.github.io/grpc/core/md_doc_statuscodes.html
.. _gRPC status code: https://grpc.github.io/grpc/core/md_doc_statuscodes.html
.. _rich gRPC error model: https://cloud.google.com/apis/design/errors#error_details
.. _standard gRPC description: https://grpc.github.io/grpc-java/javadoc/io/grpc/Status.html#getDescription--


The goal of all error messages in Daml is to enable users, developers, and operators to act independently on the encountered errors, either manually or with an automated process.

Most errors are a result of request processing.
Each error is logged and returned to the user as a failed gRPC response
containing the status code, an optional status message and optional metadata. We further enhance this by providing:

- improved consistency of the returned errors across API endpoints

- richer error payload format with clearly distinguished machine-readable parts to facilitate
  automated error handling strategies

- complete inventory of all error codes with an explanation, suggested resolution and
  other useful information


Glossary
********

Error
        Represents an occurrence of a failure.
        Consists of:

        - an `error code id`,

        - a `gRPC status code`_ (determined by its error category),

        - an `error category`,

        - a `correlation id`,

        - a human readable message,

        - and optional additional metadata.

        You can think of it as an
        instantiation of an error code.

Error Code
             Represents a class of failures.
             Identified by its error code id (we may use `error code` and `error code id` interchangeably in this document).
             Belongs to a single error category.

Error Category
                 A broad categorization of error codes that you can base your error handling strategies on.
                 Maps to exactly one `gRPC status code`_.
                 We recommend dealing with errors based on their error category.
                 However, if the error category alone is too generic
                 you can act on a particular error code.

Correlation Id
                  A value that allows the user to clearly identify the request,
                  such that the operator can lookup any log information associated with this error.
                  We use the request's submission id for correlation id.


Anatomy of an Error
*******************


Errors returned to users contain a `gRPC status code`_, a description, and additional machine-readable information
represented in the `rich gRPC error model`_.


Error Description
=================

We use the `standard gRPC description`_ that additionally adheres to our custom message format:

.. code-block:: java

    <ERROR_CODE_ID>(<CATEGORY_ID>,<CORRELATION_ID_PREFIX>):<HUMAN_READABLE_MESSAGE>

The constituent parts are:

  - ``<ERROR_CODE_ID>`` - a unique non-empty string containing at most 63 characters:
    upper-case letters, underscores or digits.
    Identifies corresponding error code id.

  - ``<CATEGORY_ID>`` - a small integer identifying the corresponding error category.

  - ``<CORRELATION_ID_PREFIX>`` - a string aimed at identifying originating request.
    Absence of one is indicated by value ``0``.
    If present, it is the first 8 characters of the corresponding request's submission id.
    Full correlation id can be found in error's additional machine readable information
    (see `Additional Machine-Readable Information`_).

  - ``:`` - a colon serves as a separator for the machine and human readable parts of the error description.

  - ``<HUMAN_READABLE_MESSAGE>`` - a message targeted at a human reader.
    Should never be parsed by applications, as the description might change
    in future releases to improve clarity.

In a concrete example an error description might look like this:

.. code-block:: java

    TRANSACTION_NOT_FOUND(11,12345): Transaction not found, or not visible.


Additional Machine-Readable Information
=======================================

We use following error details:

 - A mandatory ``com.google.rpc.ErrorInfo`` containing `error code id`.

 - A mandatory ``com.google.rpc.RequestInfo`` containing (not-truncated) correlation id
   (or ``0`` if correlation id is not available).

 - An optional ``com.google.rpc.RetryInfo`` containing retry interval with milliseconds resolution.

 - An optional ``com.google.rpc.ResourceInfo`` containing information about the resource the failure is based on.
   Any request that fails due to some well-defined resource issues (such as contract, contract key, package, party, template, synchronizer, etc.) contains these.
   Particular resources are implementation-specific and vary across ledger implementations.

Many errors will include more information,
but there is no guarantee that additional information will be preserved across versions.


Prevent Security Leaks in Error Codes
=====================================

For any error that could leak information to an attacker, the system returns an error message via the API that
contains no valuable information. The log file contains the full error message.


Work With Error Codes
=====================

This example shows how a user can extract the relevant error information:

.. code-block:: scala

    object SampleClientSide {

      import com.google.rpc.ResourceInfo
      import com.google.rpc.{ErrorInfo, RequestInfo, RetryInfo}
      import io.grpc.StatusRuntimeException
      import scala.jdk.CollectionConverters._

      def example(): Unit = {
        try {
          DummmyServer.serviceEndpointDummy()
        } catch {
          case e: StatusRuntimeException =>
            // Converting to a status object.
            val status = io.grpc.protobuf.StatusProto.fromThrowable(e)

            // Extracting gRPC status code.
            assert(status.getCode == io.grpc.Status.Code.ABORTED.value())
            assert(status.getCode == 10)

            // Extracting error message, both
            // machine oriented part: "MY_ERROR_CODE_ID(2,full-cor):",
            // and human oriented part: "A user oriented message".
            assert(status.getMessage == "MY_ERROR_CODE_ID(2,full-cor): A user oriented message")

            // Getting all the details
            val rawDetails: Seq[com.google.protobuf.Any] = status.getDetailsList.asScala.toSeq

            // Extracting error code id, error category id and optionally additional metadata.
            assert {
              rawDetails.collectFirst {
                case any if any.is(classOf[ErrorInfo]) =>
                  val v = any.unpack(classOf[ErrorInfo])
                  assert(v.getReason == "MY_ERROR_CODE_ID")
                  assert(v.getMetadataMap.asScala.toMap == Map("category" -> "2", "foo" -> "bar"))
              }.isDefined
            }

            // Extracting full correlation id, if present.
            assert {
              rawDetails.collectFirst {
                case any if any.is(classOf[RequestInfo]) =>
                  val v = any.unpack(classOf[RequestInfo])
                  assert(v.getRequestId == "full-correlation-id-123456790")
              }.isDefined
            }

            // Extracting retry information if the error is retryable.
            assert {
              rawDetails.collectFirst {
                case any if any.is(classOf[RetryInfo]) =>
                  val v = any.unpack(classOf[RetryInfo])
                  assert(v.getRetryDelay.getSeconds == 123, v.getRetryDelay.getSeconds)
                  assert(v.getRetryDelay.getNanos == 456 * 1000 * 1000, v.getRetryDelay.getNanos)
              }.isDefined
            }

            // Extracting resource if the error pertains to some well defined resource.
            assert {
              rawDetails.collectFirst {
                case any if any.is(classOf[ResourceInfo]) =>
                  val v = any.unpack(classOf[ResourceInfo])
                  assert(v.getResourceType == "CONTRACT_ID")
                  assert(v.getResourceName == "someContractId")
              }.isDefined
            }
        }
      }
    }




Error Codes In Canton Operations
********************************

Almost all errors and warnings that can be generated by a Canton-based system are annotated with error codes of the
form **SOMETHING_NOT_SO_GOOD_HAPPENED(x,c)**. The upper case string with underscores denotes the unique error id. The parentheses include
key additional information. The id together with the extra information is referred to as ``error-code``. The **x** represents
the :ref:`ErrorCategory <error_category>` used to classify the error, and the **c** represents the first 8 characters of the
correlation id associated with this request, or 0 if no correlation id is given.

The majority of errors in Canton-based systems are a result of request processing and are logged and returned to the user as described above. In other cases, errors occur due to background processes (i.e. network connection issues/transaction confirmation
processing). Such errors are only logged.

Generally, we use the following log levels on the server:

- INFO to log user errors where the error leads to a failure of the request but the system remains healthy.
- WARN to log degradations of the system or point out unusual behavior.
- ERROR to log internal errors where the system does not behave properly and immediate attention is required.

On the client side, failures are considered to be errors and logged as such.

.. _error_category:

Error Categories
****************
The error categories allow you to group errors such that application logic can be built to automatically
deal with errors and decide whether to retry a request or escalate to the operator.

A full list of error categories is documented :ref:`here <error-categories-inventory>`.

.. _machine_readable_information:

Machine Readable Information
****************************
Every error on the API is constructed to allow automated and manual error handling. First, the error category maps to exactly one gRPC status code. Second, every `error description <https://grpc.github.io/grpc-java/javadoc/io/grpc/Status.html#getDescription-->`__
(of the corresponding ``StatusRuntimeException.Status``) starts with the error information (**SOMETHING_NOT_SO_GOOD_HAPPENED(CN,x)**), separated from a human readable description
by a colon (":"). The rest of the description is targeted to humans and should never be parsed by applications, as the description
might change in future releases to improve clarity.

In addition to the status code and the description, the `gRPC rich error model <https://cloud.google.com/apis/design/errors#error_details>`__
is used to convey additional, machine-readable information to the application.

Therefore, to support automatic error processing, an application may:

- parse the error information from the beginning of the description to obtain the error-id, the error category and the component.
- use the gRPC-code to get the set of possible error categories.
- if present, use the ``ResourceInfo`` included as ``Status.details``. Any request that fails due to some well-defined resource issues (contract, contract key, package, party, template, synchronizer) will contain these, calling out on what resource the failure is based on.
- use the ``RetryInfo`` to determine the recommended retry interval (or make this decision based on the category / gRPC code).
- use the ``RequestInfo.id`` as the :ref:`correlation-id <tracing>`, included as ``Status.details``.
- use the ``ErrorInfo.reason`` as error-id and ``ErrorInfo.metadata("category")`` as error category, included as ``Status.details``.

All this information is included in errors that are generated by components under our control and included as ``Status.details``. As with Daml error codes described above, many
errors include more information, but there is no guarantee that additional information will be preserved
across versions.

Generally, automated error handling can be done on any level (e.g. load balancer using gRPC status codes, application
using ``ErrorCategory`` or human reacting to error IDs). In most cases it is advisable to deal with errors on a per-category
basis and deal with error IDs in very specific situations which are application-dependent. For example, a command failure
with the message "CONTRACT_NOT_FOUND" may be an application failure in case the given application is the only actor on
the contracts, whereas a "CONTRACT_NOT_FOUND" message is to be expected in a case where multiple independent actors operate
on the ledger state.

Example
*******

If an application submits a Daml transaction that exceeds the size limits enforced on a synchronizer, the command
will be rejected. Using the logs of one of our test cases, the participant node will log the following message:

.. code::

    2022-04-26 11:37:54,584 [GracefulRejectsIntegrationTestDefault-env-execution-context-30] INFO  c.d.c.p.p.TransactionProcessingSteps:participant=participant1/synchronizer=da tid:13617c1bda402e54e016a6a17637cb20 - SEQUENCER_REQUEST_FAILED(2,13617c1b): Failed to send command err-context:{location=TransactionProcessingSteps.scala:449, sendError=RequestInvalid(Batch size (85134 bytes) is exceeding maximum size (27000 bytes) for synchronizer da::12201253c344...)}

The machine-readable part of the error message appears as ``SEQUENCER_REQUEST_FAILED(2,13617c1b)``, mentioning the error id ``SEQUENCER_REQUEST_FAILED``, the category `ContentionOnSharedResources` with ``id=2``, and the correlation identifier ``13617c1b``. Please note that there is no guarantee on the name of the logger that is emitting the given error, as this name is internal and subject to change. The human-readable part of the log message should not be parsed, as we might subsequently improve the text.

The client will receive the error information as a Grpc error:

.. code::

    2022-04-26 11:37:54,923 [ScalaTest-run-running-GracefulRejectsIntegrationTestDefault] ERROR  c.d.c.i.EnterpriseEnvironmentDefinition$$anon$3 - Request failed for participant1.
        GrpcRequestRefusedByServer: ABORTED/SEQUENCER_REQUEST_FAILED(2,13617c1b): Failed to send command
        Request: SubmitAndWaitTransactionTree(actAs = participant1::1220baa5cd30..., commandId = '', workflowId = '', submissionId = '', deduplicationPeriod = None(), ledgerId = 'participant1', commands= ...)
        CorrelationId: 13617c1bda402e54e016a6a17637cb20
        RetryIn: 1 second
        Context: HashMap(participant -> participant1, test -> GracefulRejectsIntegrationTestDefault, synchronizer -> da, sendError -> RequestInvalid(Batch size (85134 bytes) is exceeding maximum size (27000 bytes) for synchronizer da::12201253c344...), definite_answer -> true)

Note that the second log is created by Daml tooling that prints the Grpc Status into the log files during tests. The
actual Grpc error would be received by the application and would not be logged by the participant node in the given form.

.. _error-categories-inventory:

Error Categories Inventory
**************************

The error categories allow you to group errors such that application logic can be built
in a sensible way to automatically deal with errors and decide whether to retry
a request or escalate to the operator.

..
   This file is manually generated using ErrorCategoryInventoryDocsGenApp the checked in
   This was done as the daml variant is not maintained and due for removal
.. include:: ./canton-error-categories-inventory.rst.inc


Error Codes Inventory
*********************

..
    Dynamically generated content:
.. generatedinclude:: error_codes.rst.inc
