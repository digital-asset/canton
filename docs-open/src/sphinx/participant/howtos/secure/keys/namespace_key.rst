..
   Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. wip::

    This document is a work in progress. The feature will be developed and released in the future, following the
    procedures described in the document.

.. _namespace-key-management:

Namespace Key Management
========================

Online Root Namespace Key
-------------------------

By default, the Canton node creates a root namespace key during the initialization of the node.
Operators can manually create an intermediate key on the node to authorize topology transactions. Only the intermediate key can be changed.
Therefore, it is important to keep the root key secure.

If a Canton node is configured to use a :ref:`KMS system <kms>`, then the root key can be administratively isolated,
using the KMS Administration controls to remove node access to the root key. As the root key is only
required when authorizing a new intermediate key or revoking an existing authorization, it is sufficient
to just selectively allow the node to access the key only whenever such operations are performed.

Only the root key can be isolated. All the other keys are essential for Canton to operate correctly. Optionally,
the intermediate key can also be isolated but will have to be turned on and off as needed during administrative
operations such as uploading DARs or adding parties.

.. todo:: `#25262: Improve how to identify and disable root keys <https://github.com/DACH-NY/canton/issues/25262>`_

    * Explain how to identify the root key (look up what the root key is and how to find it in the KMS)
    * Explain how to disable access to the root key in Cloud KMS.

.. _namespace-root-key-offline:

Offline Root Namespace Key
--------------------------

A more secure way to manage the root namespace key is to use an offline root namespace key. In this case, the root
namespace key is stored in a secure, possibly air gapped location, inaccessible from the Canton node.

Where the root key is stored depends on your organization's security requirements. For the remainder of this
guide, assume that there are two sites, the online site of the node with its intermediate key and the offline
site of the root key. The offline root key is used to sign a delegation to the node's intermediate key, which is then used to
authorize topology transactions.

The offline root key procedure is supported by a set of utility scripts, which can be found in the Canton ``scripts`` directory (``scripts/offline-root-key``).


Channel for Key Exchange
~~~~~~~~~~~~~~~~~~~~~~~~

There must be a channel or method which allows to exchange the public intermediate key and the signed
intermediate namespace delegations between the online and offline sites. The channel must be trusted in terms of
authenticity, but not confidentiality, as no secret information is exchanged. This means you need to ensure that
the data is not tampered with during transport, but the data itself does not need to be encrypted.

This can be done using multiple methods, depending on whether the sites are air-gapped or connected through a network.
Possible examples are: secure file transfer, QR codes, physical storage medium.

Assuming that such a trusted channel exists, the following steps are required to set up the offline root namespace key:

1. Configure Node To Expect External Root Key
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Before the first start-up, the Canton node must be configured not to initialize automatically. This is done by setting

.. literalinclude:: CANTON/community/app/src/pack/examples/10-offline-root-namespace-init/manual-init-example.conf
  :start-after: [start-docs-entry: manual init]
  :end-before: [end-docs-entry: manual init]
  :caption: Manual init config

The node can then be started with this configuration. It starts the Admin API, but halts the
startup process and wait for the initialization of the node identity together with the necessary topology transactions.

2. Export Public Key of Node
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Assuming you have access to the remote console of the node, create a new signing key to use as the intermediate key:

.. snippet:: offline_root_namespace_key
    .. success:: val key = participant1.keys.secret.generate_signing_key(name = "NamespaceDelegation", usage = com.digitalasset.canton.crypto.SigningKeyUsage.NamespaceOnly)
    .. success:: val intermediateKeyPath = better.files.File.newTemporaryFile(prefix = "intermediate-key.pub").pathAsString
    .. success:: participant1.keys.public.download_to(key.id, intermediateKeyPath)

This creates a file with the public key of the intermediate key.

The supported key specifications are listed in the follow protobuf definition:

.. literalinclude:: CANTON/community/base/src/main/protobuf/com/digitalasset/canton/crypto/v30/crypto.proto
  :start-after: [start-docs-entry: signing key spec proto]
  :end-before: [end-docs-entry: signing key spec proto]
  :caption: Signing key specifications

The synchronizer the participant node intends to connect to might restrict further the list of supported key specifications.
To obtain this information from the synchronizer directly, run the following command on the synchronizer Public API.

.. code-block::

    grpcurl -d '{}' <sequencer_endpoint> com.digitalasset.canton.sequencer.api.v30.SequencerConnectService/GetSynchronizerParameters

If a console is not accessible, you can use either a bootstrap script or ``grpccurl`` against the Admin API to
invoke the commands.

3. Share Public Key of Node with Offline Site
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Next, the intermediate public key must be transported to the offline site as described above. Ensure
that the public key is not tampered with during transport.

4. Generate Root Key and The Root Certificate
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Using OpenSSL
*************

Ensure that the necessary scripts are available on the secure site. These scripts are included in the Canton release
packages at ``scripts/offline-root-key``.
An example demonstrating usage of those scripts using ``openssl`` to generate keys and sign certificates is available at ``examples/10-offline-root-namespace-init``.
Run the next set of commands from the ``examples/10-offline-root-namespace-init`` directory.

Start by initializing variables used in the snippets below

.. literalinclude:: CANTON/community/app/src/pack/examples/10-offline-root-namespace-init/openssl-example.sh
  :start-after: [start-docs-entry: script variables]
  :end-before: [end-docs-entry: script variables]
  :caption: Script variables

.. code-block:: bash

    mkdir -p tmp/certs
    OUTPUT_DIR="tmp/certs"
    CANTON_NAMESPACE_DELEGATION_PUB_KEY=<Path to the intermediate key downloaded from Canton. ``intermediateKeyPath`` in this example>

As well as setting the path to the protobuf image containing the required protobuf definitions to generate certificates.

.. literalinclude:: CANTON/community/app/src/pack/examples/10-offline-root-namespace-init/utils.sh
  :start-after: [start-docs-entry: offline root key proto image]
  :end-before: [end-docs-entry: offline root key proto image]
  :caption: Protobuf paths
  :dedent: 4

Then, generate the root key in the secure environment and extract the public key:

.. literalinclude:: CANTON/community/app/src/pack/examples/10-offline-root-namespace-init/openssl-example.sh
  :start-after: [start-docs-entry: generate openssl root key pair]
  :end-before: [end-docs-entry: generate openssl root key pair]
  :caption: Generate key pair with openssl

Then, create the self-signed root namespace delegation, which is effectively a self-signed certificate used
as the trust anchor of the given namespace:

.. literalinclude:: CANTON/community/app/src/pack/examples/10-offline-root-namespace-init/openssl-example.sh
  :start-after: [start-docs-entry: prepare root cert]
  :end-before: [end-docs-entry: prepare root cert]
  :caption: Prepare root cert

Note that the root public key must be in the ``x509 SPKI DER`` format. For more information on Canton's supported key
formats, please refer to the following :ref:`tables <canton_supported_key_formats>`.
This generates two files, ``root-delegation.prep`` and ``root-delegation.hash``.
``.prep`` files contain unsigned topology transactions serialized to bytes.
If you really want to be sure what you are signing, inspect the ``prepare-certs.sh`` script to see how it generates the topology transaction and how it computes
the hash. Next, the hash needs to be signed.

If you are using openssl, the following command can be used to sign the hash:

.. literalinclude:: CANTON/community/app/src/pack/examples/10-offline-root-namespace-init/openssl-example.sh
  :start-after: [start-docs-entry: sign root cert]
  :end-before: [end-docs-entry: sign root cert]
  :caption: Sign root cert

Finally, assemble the signature and the prepared transaction. For more information on the supported signature formats,
please refer to the following :ref:`table <canton_supported_signature_formats>`:

.. literalinclude:: CANTON/community/app/src/pack/examples/10-offline-root-namespace-init/openssl-example.sh
  :start-after: [start-docs-entry: assemble root cert]
  :end-before: [end-docs-entry: assemble root cert]
  :caption: Assemble root cert

This creates a so-called signed topology transaction.

Using GCP KMS
*************

If you are using GCP KMS, you can use KMS CLI (https://cloud.google.com/kms/docs/create-validate-signatures)
with the following commands to generate the key:

.. code-block::

    gcloud kms keyrings create key-ring --location location (APPROXIMATE)

And the following command can be used to generate the signature:

.. code-block::

    gcloud kms asymmetric-sign \
        --version key-version \
        --key <root-key> \
        --keyring key-ring \
        --location location \
        --digest-algorithm digest-algorithm \
        --input-file root-delegation.prep \
        --signature-file root-delegation.signature

5. Create the Intermediate Certificate
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If the root key and the self-signed root delegation are available, you can create the intermediate certificate. The
steps are very similar to the root certificate, but the target is the public key of the intermediate key,
and the ``--intermediate-delegation`` flag is used instead of ``--root-delegation``.

.. literalinclude:: CANTON/community/app/src/pack/examples/10-offline-root-namespace-init/openssl-example.sh
  :start-after: [start-docs-entry: prepare delegation cert]
  :end-before: [end-docs-entry: prepare delegation cert]
  :caption: Prepare delegation cert

Verify that the generated topology transaction (printed to stdout) is correct and refers to the correct keys.
Once verified, the generated hash needs to be signed:

.. literalinclude:: CANTON/community/app/src/pack/examples/10-offline-root-namespace-init/openssl-example.sh
  :start-after: [start-docs-entry: sign delegation cert]
  :end-before: [end-docs-entry: sign delegation cert]
  :caption: Sign delegation cert

Again, the signature and the prepared transaction can be assembled:

.. literalinclude:: CANTON/community/app/src/pack/examples/10-offline-root-namespace-init/openssl-example.sh
  :start-after: [start-docs-entry: assemble delegation cert]
  :end-before: [end-docs-entry: assemble delegation cert]
  :caption: Assemble delegation cert

7. Copy the Certificates to the Online Site
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The generated certificates (never the root private key) need to be transferred to the online site. The public keys are
included in the certificates and don't need to be transported separately. You need to transfer both certificates,
the root delegation and the intermediate delegation, to the online site.

.. _import_certificates_to_node:

8. Import the Certificates to the Node
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

On the target site, import the certificates into the waiting node using the console command

.. snippet:: offline_root_namespace_key
    .. hidden:: better.files.File("community/app/src/pack/scripts/offline-root-key/root_namespace_buf_image.json.gz").deleteOnExit(swallowIOExceptions = true)
    .. hidden:: if(!better.files.File("tmp/certs").exists) { better.files.File("tmp/certs").createDirectories() }
    .. hidden:: val processLogger = scala.sys.process.ProcessLogger(logger.info(_), logger.warn(_))
    .. hidden:: scala.sys.process.Process(Seq("community/app/src/pack/examples/10-offline-root-namespace-init/openssl-example.sh", intermediateKeyPath), cwd = better.files.File.currentWorkingDirectory.toJava, extraEnv = "OUTPUT_DIR" -> better.files.File("tmp/certs").pathAsString).!(processLogger)
    .. success:: participant1.topology.init_id(identifier = "participant1", delegationFiles = Seq("tmp/certs/root_namespace.cert", "tmp/certs/intermediate_namespace.cert"))
    .. success:: participant1.health.status

Alternatively, the Admin API can be used directly via ``grpccurl`` to initialize the node.

Pre-Generated Certificates
--------------------------

The certificates can also be provided directly via the node's configuration file if they've been generated beforehand.
In this scenario, instead of generating the intermediate key via the node's ``generate_signing_key`` command as described above,
they key must be generated on a KMS and its public key material downloaded. The same scripts can then be used to generate the certificate,
with the exception that the intermediate public key will not be in the Canton format but in a DER format and should therefore be set with ``--target-pub-key``.
Once the certificates are available, you must register the intermediate KMS key by running:

.. literalinclude:: CANTON/community/app/src/test/scala/com/digitalasset/canton/integration/tests/topology/TopologyManagementHelper.scala
   :language: scala
   :start-after: user-manual-entry-begin: ManualRegisterKmsIntermediateNamespaceKey
   :end-before: user-manual-entry-end: ManualRegisterKmsIntermediateNamespaceKey
   :dedent:

and then :ref:`import the certificates to the node <import_certificates_to_node>`.

This configuration directive has no effect once the node is initialized and can subsequently be removed.

Delegation Restrictions
-----------------------

You can further restrict the kind of topology transactions a delegation can authorize.
The ``prepare-certs`` script exposes a ``--delegation-restrictions`` flag for that purpose.

.. literalinclude:: CANTON/community/app/src/pack/examples/10-offline-root-namespace-init/openssl-restricted-key-example.sh
  :start-after: [start-docs-entry: prepare restricted key cert]
  :end-before: [end-docs-entry: prepare restricted key cert]
  :caption: Prepare delegation with signing restrictions

The delegation can then be signed and assembled as before. Once the signed certificate is available, load it onto the node:

.. _load_single_cert_from_file:

.. literalinclude:: CANTON/community/app/src/pack/examples/10-offline-root-namespace-init/restricted-key.canton
  :start-after: [start-docs-entry: load cert from file]
  :end-before: [end-docs-entry: load cert from file]
  :caption: Load restricted key certificate onto node

Rotate the Intermediate Key
---------------------------

Create new Intermediate Key
~~~~~~~~~~~~~~~~~~~~~~~~~~~

In order to create another intermediate key, we follow the same steps as before. Create the key on the online site
and export it.

Follow the same steps to create a new intermediate delegation for the new intermediate key:

* copy to secure site
* generate the intermediate delegation (skip self-signed root delegation as it has already been generated)
* copy the certificate to the node site

The new intermediate delegation can then be imported into the node as shown :ref:`here <load_single_cert_from_file>`.

Once the new delegation has been imported, the old intermediate key can be revoked.

Revoking the Intermediate Key
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To revoke the intermediate key, the root key needs to be used to sign a revocation transaction. The revocation
transaction is prepared in the same way as the intermediate delegation:

.. literalinclude:: ../../../../../../../community/app/src/pack/examples/10-offline-root-namespace-init/openssl-revoke-delegation-example.sh
  :start-after: [start-docs-entry: prepare revoked key cert]
  :end-before: [end-docs-entry: prepare revoked key cert]
  :caption: Prepare revocation certificate

The generated hash needs to be signed and then subsequently assembled into a certificate:

.. literalinclude:: ../../../../../../../community/app/src/pack/examples/10-offline-root-namespace-init/openssl-revoke-delegation-example.sh
  :start-after: [start-docs-entry: sign revoked key cert]
  :end-before: [end-docs-entry: sign revoked key cert]
  :caption: Assemble revocation certificate

On the node site, the revocation certificate can be imported using:

.. literalinclude:: ../../../../../../../community/app/src/pack/examples/10-offline-root-namespace-init/revoke-namespace-delegation.canton
  :start-after: [start-docs-entry: load cert from file]
  :end-before: [end-docs-entry: load cert from file]
  :caption: Load revocation certificate onto node

Rotating the Root Namespace Key
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You cannot rotate the root namespace key. If you need to discontinue the usage of the namespace, you need to create a new namespace, new parties and
participants in that new namespace, and transfer the contracts to the new parties.
