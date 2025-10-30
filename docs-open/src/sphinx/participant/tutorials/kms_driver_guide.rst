..
   Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _kms_driver_guide:

Canton KMS Driver developer guide
=================================

Introduction
------------

The Canton protocol relies on cryptographic operations such as
asymmetric encryption and digital signatures. To maximize the
operational security of a Canton node the corresponding private keys should not
be stored or processed in cleartext. A Key Management System (KMS) or Hardware
Security Module (HSM) allows you to perform such cryptographic operations where
the private key resides securely inside the KMS/HSM. All nodes in Canton can
make use of a KMS.

AWS KMS and Google Cloud KMS are supported as of Canton v2.7. To broaden the
support of other KMSs and HSMs, Canton v2.9 introduced a plug-in approach, called
KMS Drivers, which allows the implementation of custom integrations. This
document explains the APIs that must be implemented for a KMS Driver,
provides a guide on the implementation, and describes how to configure Canton to
run with a KMS Driver. Currently, the KMS Driver is only available in a Scala
implementation.

You can find a `"mock" KMS Driver implementation <https://github.com/digital-asset/canton/blob/main/community/mock-kms-driver/src/main/scala/com/digitalasset/canton/crypto/kms/mock/v1/MockKmsDriver.scala>`_ built on a software-based crypto library in the Canton community repository.

KMS Driver API
--------------

The two main APIs that are required for a KMS Driver are:

- Driver Factory: Implements how a driver is instantiated; and the main entry
   point for Canton to load a driver.
- KMS Driver: Offers cryptographic operations based on the KMS.

The stable APIs are versioned with a single major version number. A breaking
change to either the factory or driver APIs results in a new major version
of those APIs. The current and only version is **v1**, which is part of the
module path for the respective API interfaces.

KMS Driver Factory API v1
~~~~~~~~~~~~~~~~~~~~~~~~~

The factory consists of two interfaces: a generic v1 DriverFactory and a
specific v1 KmsDriverFactory.

The ``v1.DriverFactory`` defines the following aspects for a generic driver:

-  The type of the driver
-  A name that uniquely identifies the driver
-  The version of the API the driver implements and optional build
   information (driver version number or commit hash)
-  A driver-specific configuration object with configuration parser and
   writer
-  A create method that instantiates a driver with that factory

Concretely the interface is defined as the following in Scala:

.. literalinclude:: CANTON/community/kms-driver-api/src/main/scala/com/digitalasset/canton/driver/api/v1/DriverFactory.scala
    :language: Scala


``v1.KmsDriverFactory`` is a specialization of the generic DriverFactory which
defines the driver type as a KmsDriver and the API version as 1.

.. literalinclude:: CANTON/community/kms-driver-api/src/main/scala/com/digitalasset/canton/crypto/kms/driver/api/KmsDriverFactory.scala
    :language: Scala

Driver Configuration Reading & Parsing
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Canton uses `pureconfig <https://pureconfig.github.io/>`__ configuration library
to read its configuration. Given that the configuration of the driver can be
embedded inside a Canton configuration file, the factory of the driver needs to
specify the type of configuration, as well as configuration readers and writers.
A ``ConfigType`` can be a case class or as basic as a ``Map[String, String]``.
In the latter case, the config reader and writer can be defined as:

- ``val configReader = pureconfig.ConfigReader.mapReader[String]``
- ``val configWriter = pureconfig.ConfigWriter.mapWriter[String]``

KMS Driver API v1
~~~~~~~~~~~~~~~~~

The main part of the API is the ``v1.KmsDriver`` API that defines the following
operations for a KMS Driver:

-  Key Generation: Asymmetric signing and encryption key pairs as well
   as symmetric encryption keys
-  Supported Key and Algorithm Specifications: The specs
   supported by the driver, for example, RSA 2048 keys and RSA OAEP
   SHA256 asymmetric encryption.
-  Signing: Sign data with a key from the KMS and the specified
   algorithm.
-  Asymmetric Decryption: Decrypt a ciphertext with a private key from
   the KMS and the specified algorithm.
-  Symmetric Encryption and Decryption: Encrypt or decrypt with a
   symmetric encryption key from the KMS. The default symmetric
   encryption algorithm of the KMS is used.
-  Key Management: Get the public key of a private key stored in the
   KMS, check if a key exists, and delete a key.
-  Health: Return the health of the KMS Driver instance.

The API is designed as an asynchronous API using Futures. An
OpenTelemetry trace context is passed from Canton into the KMS Driver
operations to be able to link a Canton request to operations in the KMS.
The driver is responsible for propagating the trace context into the
KMS.

Concretely the Scala interface is defined as the following:

.. literalinclude:: CANTON/community/kms-driver-api/src/main/scala/com/digitalasset/canton/crypto/kms/driver/api/KmsDriver.scala
    :language: Scala

Error Handling and Health
~~~~~~~~~~~~~~~~~~~~~~~~~

In case the driver experiences an error the ``Future`` of the operation should be
failed with a ``KmsDriverException``. When the exception's flag retryable is
true the caller side, (that is, Canton) performs a retry with exponential
backoff. This behavior is suitable for transient errors, such as network issues,
resource exhaustion, etc.

In case of permanent errors, a non-retryable exception should be thrown, which
either fails the current operation from where the cryptographic operation is
called or causes a fatal error in the Canton node.

The driver should report its health through the health method. A Canton node
periodically queries the health of the driver and reports it as part of the
node's overall health.

Develop and Test a KMS Driver
-----------------------------

Set Up API Dependency
~~~~~~~~~~~~~~~~~~~~~

The Canton KMS Driver API is published as an artifact on Digital Asset's JFrog
Artifactory:

https://digitalasset.jfrog.io/ui/repos/tree/General/canton-kms-driver-api

You must have a Canton enterprise license and account to access the artifact.
You may need to configure your build system to authenticate with a personal
access token towards JFrog Artifactory.

In your build system of choice, you need to depend on the API as a regular
Maven-style artifact with:

-  organization: com.digitalasset.canton
-  artifact: kms-driver-api
-  version: the Canton release version, e.g., 3.3.0

Implement the API and Build the Driver
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Implement the ``v1.KmsDriverFactory`` by specifying the driver's name,
configuration type with readers/writers, and the create method to instantiate a
driver by creating a new class instance of your ``KmsDriver`` implementation.
Specify the fully qualified name of the factory class in the file:

``src/main/resources/META-INF/services/com.digitalasset.canton.crypto.kms.driver.api.v1.KmsDriverFactory``

and in the following file for the base driver factory (without v1):

``src/main/resources/META-INF/services/com.digitalasset.canton.crypto.kms.driver.api.KmsDriverFactory``

The major part of the implementation is the ``v1.KmsDriver`` specific to
the KMS/HSM to be integrated with. The supported key and algorithm
specifications can be defined statically depending on the capabilities of the
underlying KMS/HSM. To ensure the best compatibility with other Canton nodes,
all currently specified key and algorithm specifications should be supported.

Any credentials required by the underlying KMS/HSM can either be passed
through the Canton configuration file as part of the driver-specific
configuration, where secrets can be resolved from the environment, or retrieved by the driver directly from the environment or any other
driver-specific means.

Bundle your driver into a self-contained jar, that is, with all required
libraries included in the jar. That way you only need a single driver jar when
starting Canton with your KMS Driver.

KMS Driver Testing
~~~~~~~~~~~~~~~~~~

The reusable test suite for KMS Drivers is published at
`canton-kms-driver-testing
<https://digitalasset.jfrog.io/ui/repos/tree/General/canton-kms-driver-testing>`__.
Configure your build system to depend on this maven artifact in the test scope
of your project (e.g. for sbt append % Test to limit the dependency to the test
scope).

KmsDriverTest
^^^^^^^^^^^^^

The main part of the test suite is the ``KmsDriverTest`` that tests the
functionality of a driver against the ``KmsDriver`` API.

In the simplest form the specific driver test class extends the
``KmsDriverTest`` and allows the generation of new keys as part of the test:

.. literalinclude:: CANTON/community/aws-kms-driver/src/test/scala/com/digitalasset/canton/nightly/AwsKmsDriverTest.scala
    :language: Scala
    :start-after: user-manual-entry-begin: AwsKmsDriverTest
    :end-before: user-manual-entry-end: AwsKmsDriverTest

Generating new keys can be expensive when running tests during
development, particularly with cloud-based KMSs. To mitigate this, the test
suite can also be configured to use predefined keys to test most parts (except
key generation) of the KMS Driver API:

.. literalinclude:: CANTON/community/aws-kms-driver/src/test/scala/com/digitalasset/canton/nightly/AwsKmsDriverTest.scala
    :language: Scala
    :start-after: user-manual-entry-begin: AwsKmsDriverWithPredefinedKeysTest
    :end-before: user-manual-entry-end: AwsKmsDriverWithPredefinedKeysTest

For each supported signing/encryption key specification an existing key alias/ID
can be configured as part of the predefined keys maps. When running the test
suite the generation of new keys is not allowed.

KmsDriverFactoryTest
^^^^^^^^^^^^^^^^^^^^

The test suite for the KMS Driver factory is structured similarly to the above:

.. literalinclude:: CANTON/community/aws-kms-driver/src/test/scala/com/digitalasset/canton/nightly/AwsKmsDriverTest.scala
    :language: Scala
    :start-after: user-manual-entry-begin: AwsKmsDriverFactoryTest
    :end-before: user-manual-entry-end: AwsKmsDriverFactoryTest

The ``KmsDriverFactory`` can write the driver-specific configuration with a
confidential flag being true, which means any sensitive information in the
configuration such as credentials should be omitted from the written
configuration. A specific test case should be added if your driver-specific
configuration contains any confidential information, asserting that the
sensitive information is omitted.

Run Canton with a KMS Driver
----------------------------

Configure Canton to run with a KMS Driver, for example, for a
participant participant1:

.. literalinclude:: CANTON/enterprise/app/src/test/resources/aws-kms-driver.conf
    :language: none
    :start-after: user-manual-entry-begin: AwsKmsDriverConfig
    :end-before: user-manual-entry-end: AwsKmsDriverConfig

Run Canton with your driver jar on its class path:

``java -cp driver.jar:canton.jar com.digitalasset.canton.CantonEnterpriseApp -c canton.conf # further canton arguments``

Where canton.jar depends on the Canton version, e.g.,
``lib/canton-enterprise-3.3.3.jar``. The ``canton.conf`` is a configuration file
that needs to configure at least one of the nodes to use the driver KMS as
outlined above. Run a ping for example with
``participant1.health.ping(participant1)`` to validate that the participant can
use the configured KMS and driver.
