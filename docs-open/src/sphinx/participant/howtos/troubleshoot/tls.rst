..
   Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _troubleshoot-tls:

Troubleshoot TLS
================

TLS can be configured using the parameters described :ref:`here <tls-configuration>`.

If you are having trouble setting up SSL/TLS, you can enable SSL debugging, increase netty logging,
or generate test keys and certificates to validate your configuration.

Enable debug logging for ssl
------------------------------

You can enable SSL debugging by adding the following flag to the command line when starting Canton:

.. code-block:: bash

    bin/canton -Djavax.net.debug=all

This will print verbose SSL-related information to the console, including details of the handshake process.
It is recommended to use this flag only when troubleshooting, as the output can be very verbose and
may impact performance of your application.

Enable debug logging for netty
------------------------------

The error messages on TLS issues provided by the networking library ``netty`` are less than optimal.
If you are struggling with setting up TLS, please enable ``DEBUG`` logging on the ``io.netty`` logger.
This can typically be done by adding the following line to your logback logging configuration:

.. code-block:: xml

     <logger name="io.grpc.netty.shaded.io.netty.handler.ssl" level="DEBUG"/>

Generate test keys and certificates
-----------------------------------

If you need to generate test TLS certificates to test your configuration, you can use the following OpenSSL script:

.. literalinclude:: CANTON/community/app/src/pack/config/tls/gen-test-certs.sh
   :start-after: architecture-handbook-entry-begin: GenTestCerts
   :end-before: architecture-handbook-entry-end: GenTestCerts

If you'd prefer to manually generate your own set of keys and certificates, the commands used in this process
are documented here:

.. literalinclude:: CANTON/community/app/src/pack/config/tls/certs-common.sh
   :start-after: architecture-handbook-entry-begin: GenTestCertsCmds
   :end-before: architecture-handbook-entry-end: GenTestCertsCmds
