..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _kms_driver_config:

Configure a Driver-based KMS
============================

Canton allows integration with a variety of KMS and HSM solutions through a KMS Driver.
This approach enables you to connect Canton to an external key manager by building your own integration layer.

Configuring Canton to run with a KMS Driver is done similarly to other KMS providers by specifying:

.. code::

    type = driver
    name = <name_of_driver>

For example, for a Participant named `participant1`:

.. literalinclude:: CANTON/community/app/src/test/resources/aws-kms-driver.conf
    :language: none
    :start-after: user-manual-entry-begin: AwsKmsDriverConfig
    :end-before: user-manual-entry-end: AwsKmsDriverConfig

- ``type`` specifies which KMS to use; in this case, a driver.
- ``name`` is a uniquely identifying name configured for the driver.
- KMS driver-specific configuration can be passed in through the ``config`` field.

In addition to this configuration, you must also provide a `.jar` file that implements the required API and acts
as the bridge between Canton and the target KMS.

Run Canton with your driver `.jar` on its class path:

``java -cp driver.jar:canton.jar com.digitalasset.canton.CantonCommunityApp -c canton.conf # further canton arguments``

For guidance on developing and deploying your own KMS Driver in Canton, refer to the
:ref:`Canton KMS Driver developer guide <kms_driver_guide>`. This guide includes instructions for building a
custom driver, details on the necessary APIs, and steps to configure Canton to use the driver.
