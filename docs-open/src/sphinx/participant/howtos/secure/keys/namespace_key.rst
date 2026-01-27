..
   Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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

.. literalinclude:: CANTON/community/app/src/test/resources/manual-init-example.conf
  :start-after: [start-docs-entry: manual init]
  :end-before: [end-docs-entry: manual init]
  :caption: Manual init config
  :dedent: 4

The node can then be started with this configuration. It starts the Admin API, but halts the
startup process and wait for the initialization of the node identity together with the necessary topology transactions.

2. Export Public Key of Node
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Start by creating a temporary directory where we'll store keys and certificates during the initialization process.

.. code-block:: bash

    mkdir -p /tmp/canton/certs

Assuming you have access to the remote console of the node, create a new signing key to use as the intermediate key:

.. snippet:: offline_root_namespace_key
    .. hidden:: if(!better.files.File("/tmp/canton/certs").exists) { better.files.File("/tmp/canton/certs").createDirectories() }
    .. success:: val intermediateKey = participant1.keys.secret.generate_signing_key(name = "NamespaceDelegation", usage = com.digitalasset.canton.crypto.SigningKeyUsage.NamespaceOnly)
    .. success:: val intermediateKeyPath = better.files.File("/tmp/canton/certs/intermediate_key.pub").pathAsString
    .. success:: participant1.keys.public.download_to(intermediateKey.id, intermediateKeyPath)

This creates a file with the public key of the intermediate key.

The supported key specifications are listed in the follow protobuf definition:

.. literalinclude:: CANTON/community/base/src/main/protobuf/com/digitalasset/canton/crypto/v30/crypto.proto
  :start-after: [start-docs-entry: signing key spec proto]
  :end-before: [end-docs-entry: signing key spec proto]
  :caption: Signing key specifications

The synchronizer the participant node intends to connect to might restrict further the list of supported key specifications.
To obtain this information from the synchronizer directly, run the following command against the synchronizer Public API.

.. code-block::

    grpcurl -d '{}' <sequencer_endpoint> com.digitalasset.canton.sequencer.api.v30.SequencerConnectService/GetSynchronizerParameters

3. Share Public Key of Node with Offline Site
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Next, the intermediate public key must be transported to the offline site as described above. Ensure
that the public key is not tampered with during transport.

4. Generate Root Key and The Root Certificate
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Ensure that the necessary scripts are available on the secure site. These scripts are included in the Canton release
packages at ``scripts/offline-root-key``. This directory is self-contained and can be inspected / copied over to the secure site
to generate and sign the certificates required to initialize the node and rotate delegations.

The following tools are required for running those scripts:

- `openssl <https://docs.openssl.org/3.5/man1/openssl/>`_: CLI tool used to detect key formats and encode data to base64. It's also used in the example scripts (see below) to generate keys and signatures.
- `buf <https://buf.build/product/cli>`_: CLI tool used to convert between protobuf binary and JSON formats. Used to build and visualize transactions before signing them.
- `xxd <https://linux.die.net/man/1/xxd>`_: Linux command to convert between bytes and hexadecimal representation
- `gunzip <https://linux.die.net/man/1/gunzip>`_: Linux command to decompress gzipped files. Used to inspect the buf image and provide better error reporting.
- `jq <https://jqlang.org/>`_: CLI tool used to manipulate JSON data, used to build and display protobuf messages in JSON format.

Canton support several signing key specifications. See this :ref:`table <canton_supported_signature_formats>` for an exhaustive list.
In this page we'll demonstrate the initialization process with an ED25519 root key.

Using OpenSSL
*************

From this point forward, all commands must be run from the ``scripts/offline-root-key`` directory for this example.

.. note::

    This section generates keys with OpenSSL which stores the private key unencrypted on disk.
    This is NOT a secure way to store private keys. For production deployments, make sure to secure the private key or use a KMS to manage it.

Generate the root key in the secure environment and extract the public key:

.. snippet:: offline_root_namespace_key
    .. hidden:: if(!better.files.File("/tmp/canton/certs").exists) { better.files.File("/tmp/canton/certs").createDirectories() }
    .. shell(cwd=community/app/src/pack/scripts/offline-root-key):: openssl genpkey -algorithm Ed25519 -outform DER -out "/tmp/canton/certs/root_private_key.der"
    .. shell(cwd=community/app/src/pack/scripts/offline-root-key):: openssl pkey -in "/tmp/canton/certs/root_private_key.der" -pubout -outform DER -out "/tmp/canton/certs/root_public_key.der"

Then, create the self-signed root namespace delegation, which is effectively a self-signed certificate used
as the trust anchor of the given namespace:

.. snippet:: offline_root_namespace_key
    .. shell(cwd=community/app/src/pack/scripts/offline-root-key):: ./prepare-cert.sh --root-delegation --root-pub-key "/tmp/canton/certs/root_public_key.der" --target-pub-key "/tmp/canton/certs/root_public_key.der" --output "/tmp/canton/certs/root_namespace"

Note that the root public key must be in the ``x509 SPKI DER`` format. For more information on Canton's supported key
formats, please refer to the following :ref:`tables <canton_supported_key_formats>`.
This generates two files, ``root-delegation.prep`` and ``root-delegation.hash``.
``.prep`` files contain unsigned topology transactions serialized to bytes.
If you really want to be sure what you are signing, inspect the ``prepare-cert.sh`` script to see how it generates the topology transaction and how it computes
the hash. Next, the hash needs to be signed.

Sign the hash:

.. snippet:: offline_root_namespace_key
    .. shell(cwd=community/app/src/pack/scripts/offline-root-key):: openssl pkeyutl -rawin -inkey "/tmp/canton/certs/root_private_key.der" -keyform DER -sign -in "/tmp/canton/certs/root_namespace.hash" -out "/tmp/canton/certs/root_namespace.signature"

Finally, assemble the signature and the prepared transaction:

.. snippet:: offline_root_namespace_key
    .. shell(cwd=community/app/src/pack/scripts/offline-root-key):: ./assemble-cert.sh --prepared-transaction "/tmp/canton/certs/root_namespace.prep" --signature "/tmp/canton/certs/root_namespace.signature" --signature-algorithm "ed25519" --output "/tmp/canton/certs/root_namespace"

The signature algorithm depends on the root key specification. See the ``assemble-cert.sh`` script for how it matches the value of ``signature-algorithm`` argument to canton signature and format:

.. literalinclude:: CANTON/community/app/src/pack/scripts/offline-root-key/assemble-cert.sh
    :dedent: 4
    :start-after: [start-doc-entry: algo spec]
    :end-before: [end-doc-entry: algo spec]
    :caption: Signature algorithms


This creates a so-called signed topology transaction.

Using GCP KMS
*************

Use the KMS CLI (https://cloud.google.com/kms/docs/create-validate-signatures)
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

.. snippet:: offline_root_namespace_key
    .. shell(cwd=community/app/src/pack/scripts/offline-root-key):: ./prepare-cert.sh --intermediate-delegation --root-pub-key "/tmp/canton/certs/root_public_key.der" --canton-target-pub-key "/tmp/canton/certs/intermediate_key.pub" --output "/tmp/canton/certs/intermediate_namespace"

Verify that the generated topology transaction (printed to stdout) is correct and refers to the correct keys.
Once verified, the generated hash needs to be signed:

.. snippet:: offline_root_namespace_key
    .. shell(cwd=community/app/src/pack/scripts/offline-root-key):: openssl pkeyutl -rawin -inkey "/tmp/canton/certs/root_private_key.der" -keyform DER -sign -in "/tmp/canton/certs/intermediate_namespace.hash" -out "/tmp/canton/certs/intermediate_namespace.signature"

Again, the signature and the prepared transaction can be assembled:

.. snippet:: offline_root_namespace_key
    .. shell(cwd=community/app/src/pack/scripts/offline-root-key):: ./assemble-cert.sh --prepared-transaction "/tmp/canton/certs/intermediate_namespace.prep" --signature "/tmp/canton/certs/intermediate_namespace.signature" --signature-algorithm "ed25519" --output "/tmp/canton/certs/intermediate_namespace"

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
    .. success:: participant1.topology.init_id(identifier = "participant1", delegationFiles = Seq("/tmp/canton/certs/root_namespace.cert", "/tmp/canton/certs/intermediate_namespace.cert"))
    .. success:: participant1.health.status

Alternatively, the Admin API can be used directly via ``grpccurl`` to initialize the node.

Pre-Generated Certificates
--------------------------

The certificates can also be provided directly via the node's configuration file if they've been generated beforehand.
In this scenario, instead of generating the intermediate key via the node's ``generate_signing_key`` command as described above,
it key must be generated on a KMS and its public key material downloaded. See the :ref:`KMS documentation <external_key_management>` for details.
The same scripts can then be used to generate the certificate,
with the exception that the intermediate public key will not be in the Canton format but in a DER format and should therefore be set with ``--target-pub-key``.
Once the certificates are available, they can be configured on the node as such:

.. code-block::

    canton.participants.mynode.init.node-identity = {
        type = external
        prefix = "mynodename" // optional prefix, random string generated otherwise
        certificates = ["root-delegation.cert", "intermediate-delegation.cert"]
    }

This configuration directive has no effect once the node is initialized and can subsequently be removed.

Delegation Restrictions
-----------------------

You can further restrict the kind of topology transactions a delegation can authorize.
The ``prepare-cert`` script exposes a ``--delegation-restrictions`` flag for that purpose.

For example, to create a delegation that can only sign namespace delegations, let's first create a new key for that delegation:

.. snippet:: offline_root_namespace_key
    .. success:: val keyWithRestrictionsPath = "/tmp/canton/certs/restricted_key.pub"
    .. success:: val keyWithRestrictions = participant1.keys.secret.generate_signing_key(name = "RestrictedKey", usage = Set(SigningKeyUsage.Namespace))
    .. success:: participant1.keys.public.download_to(keyWithRestrictions.id, keyWithRestrictionsPath)

Then prepare, sign and assemble it as we did previously, except we use the ``--delegation-restrictions`` flag on the prepare script this time:

.. snippet:: offline_root_namespace_key
    .. shell(cwd=community/app/src/pack/scripts/offline-root-key):: ./prepare-cert.sh --delegation-restrictions PARTY_TO_PARTICIPANT,PARTY_TO_KEY_MAPPING --root-pub-key "/tmp/canton/certs/root_public_key.der" --canton-target-pub-key "/tmp/canton/certs/restricted_key.pub" --output "/tmp/canton/certs/restricted_key_namespace"
    .. shell(cwd=community/app/src/pack/scripts/offline-root-key):: openssl pkeyutl -rawin -inkey "/tmp/canton/certs/root_private_key.der" -keyform DER -sign -in "/tmp/canton/certs/restricted_key_namespace.hash" -out "/tmp/canton/certs/restricted_key_namespace.signature"
    .. shell(cwd=community/app/src/pack/scripts/offline-root-key):: ./assemble-cert.sh --prepared-transaction "/tmp/canton/certs/restricted_key_namespace.prep" --signature "/tmp/canton/certs/restricted_key_namespace.signature" --signature-algorithm "ed25519" --output "/tmp/canton/certs/restricted_key_namespace"

Once the signed certificate is available, load it onto the node:

.. _load_single_cert_from_file:

.. snippet:: offline_root_namespace_key
    .. success:: participant1.topology.transactions.load_single_from_file("/tmp/canton/certs/restricted_key_namespace.cert", TopologyStoreId.Authorized)

Rotate the Intermediate Key
---------------------------

Rotating the intermediate key involves creating a new key, and revoking the current one.

Create new Intermediate Key
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Creating a new key follow the same steps as before:

.. snippet:: offline_root_namespace_key
    .. success:: val newIntermediateKey = participant1.keys.secret.generate_signing_key(
                    name = "NewIntermediateKey",
                    usage = Set(SigningKeyUsage.Namespace)
                 )
    .. success:: participant1.keys.public.download_to(newIntermediateKey.id, "/tmp/canton/certs/new_intermediate_key.pub")

Export delegation to revoke
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. snippet:: offline_root_namespace_key
    .. success:: val delegationToRevokePath = s"/tmp/canton/certs/delegation_to_revoke.tx"
    .. success:: participant1.topology.namespace_delegations.list(TopologyStoreId.Authorized).find(_.item.target == intermediateKey).get.toTopologyTransaction.writeToFile(delegationToRevokePath)

Generate revocation and new intermediate certificates
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Prepare revocation and new delegation certificates:

.. snippet:: offline_root_namespace_key
    .. shell(cwd=community/app/src/pack/scripts/offline-root-key):: ./prepare-cert.sh --revoke-delegation "/tmp/canton/certs/delegation_to_revoke.tx" --output "/tmp/canton/certs/revoked_delegation"
    .. shell(cwd=community/app/src/pack/scripts/offline-root-key):: ./prepare-cert.sh --intermediate-delegation --root-pub-key "/tmp/canton/certs/root_public_key.der" --canton-target-pub-key "/tmp/canton/certs/new_intermediate_key.pub" --output "/tmp/canton/certs/new_delegation"

Sign revocation and new delegation certificates:

.. snippet:: offline_root_namespace_key
    .. shell(cwd=community/app/src/pack/scripts/offline-root-key):: openssl pkeyutl -rawin -inkey "/tmp/canton/certs/root_private_key.der" -keyform DER -sign -in "/tmp/canton/certs/revoked_delegation.hash" -out "/tmp/canton/certs/revoked_delegation.signature"
    .. shell(cwd=community/app/src/pack/scripts/offline-root-key):: openssl pkeyutl -rawin -inkey "/tmp/canton/certs/root_private_key.der" -keyform DER -sign -in "/tmp/canton/certs/new_delegation.hash" -out "/tmp/canton/certs/new_delegation.signature"

Assemble revocation and new delegation certificates:

.. snippet:: offline_root_namespace_key
    .. shell(cwd=community/app/src/pack/scripts/offline-root-key):: ./assemble-cert.sh --prepared-transaction "/tmp/canton/certs/revoked_delegation.prep" --signature "/tmp/canton/certs/revoked_delegation.signature" --signature-algorithm "ed25519" --output "/tmp/canton/certs/revoked_delegation"
    .. shell(cwd=community/app/src/pack/scripts/offline-root-key):: ./assemble-cert.sh --prepared-transaction "/tmp/canton/certs/new_delegation.prep" --signature "/tmp/canton/certs/new_delegation.signature" --signature-algorithm "ed25519" --output "/tmp/canton/certs/new_delegation"

Observe the delegations before the revocation:

.. snippet:: offline_root_namespace_key
    .. success:: participant1.topology.namespace_delegations.list(store = TopologyStoreId.Authorized)

We should see 3 NamespaceDelegations:

- The root delegation
- The intermediate delegation (about to be rotated)
- The restricted delegation

Load the revocation and new delegation certificates onto the node:

.. snippet:: offline_root_namespace_key
    .. success:: participant1.topology.transactions.load_single_from_files(Seq("/tmp/canton/certs/revoked_delegation.cert", "/tmp/canton/certs/new_delegation.cert"), TopologyStoreId.Authorized)

Observe again after and see the previous intermediate delegation has been replaced by a new one:

.. snippet:: offline_root_namespace_key
    .. success:: participant1.topology.namespace_delegations.list(store = TopologyStoreId.Authorized)

Rotating the Root Namespace Key
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You cannot rotate the root namespace key. If you need to discontinue the usage of the namespace, you need to create a new namespace, new parties and
participants in that new namespace, and transfer the contracts to the new parties.
