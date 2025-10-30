..
   Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
..
   SPDX-License-Identifier: Apache-2.0

.. _crypto-schemes:

Supported Cryptographic Schemes And Formats
===========================================

Within Canton, we use the cryptographic primitives of signing, symmetric encryption, and asymmetric encryption,
with the following supported cryptographic schemes and formats.
For asymmetric signing and encryption, each scheme is divided into a key and an algorithm specification.

.. _canton_supported_schemes:

.. note::

   **Legend for the following tables:**

   - **S** = Supported
   - **P¹** = Partially supported — only supports signature verification, not signing with a private key
   - **P²** = Partially supported — only supports encryption, not decryption with a private key
   - Values in brackets (**[<scheme>]**) indicate the configuration strings to use in Canton

.. table:: Supported Asymmetric Encryption and Signing Key Specifications

    +-----------------------------------------+-----------+----------+-----------------------------+
    | **Key Spec**                            |   JCE     |   KMS    | **Purpose**                 |
    +=========================================+===========+==========+=============================+
    | EC-Curve25519 `[ec-curve-25519]`        |     S     |   P¹     | Signing                     |
    +-----------------------------------------+-----------+----------+-----------------------------+
    | EC-P256 `[ec-p-256]`                    |     S     |   S      | Signing, Encryption         |
    +-----------------------------------------+-----------+----------+-----------------------------+
    | EC-P384 `[ec-p-384]`                    |     S     |   S      | Signing                     |
    +-----------------------------------------+-----------+----------+-----------------------------+
    | EC-Secp256k1 `[ec-secp-256k-1]`         |     S     |   S      | Signing                     |
    +-----------------------------------------+-----------+----------+-----------------------------+
    | RSA-2048 `[rsa-2048]`                   |     S     |   S      | Encryption                  |
    +-----------------------------------------+-----------+----------+-----------------------------+

.. table:: Supported Signing Algorithm Specifications

    +-----------------------------------------+-----------+----------+----------------------------------+
    | **Algorithm**                           |   JCE     |   KMS    | **Supported Key Specs**          |
    +=========================================+===========+==========+==================================+
    | Ed25519 `[ed-25519]`                    |     S     |   P¹     | EC-Curve25519                    |
    +-----------------------------------------+-----------+----------+----------------------------------+
    | EC-DSA-SHA256 `[ec-dsa-sha-256]`        |     S     |   S      | EC-P256, EC-Secp256k1            |
    +-----------------------------------------+-----------+----------+----------------------------------+
    | EC-DSA-SHA384 `[ec-dsa-sha-384]`        |     S     |   S      | EC-P384                          |
    +-----------------------------------------+-----------+----------+----------------------------------+

.. table:: Supported Asymmetric Encryption Algorithm Specifications

    +---------------------------------------------------------------------+-----------+----------+--------------------------+
    | **Algorithm**                                                       |   JCE     |   KMS    | **Supported Key Specs**  |
    +=====================================================================+===========+==========+==========================+
    | ECIES-HMAC-SHA256-AES128-CBC `[ecies-hkdf-hmac-sha-256-aes-128-cbc]`|     S     |   P²     | EC-P256                  |
    +---------------------------------------------------------------------+-----------+----------+--------------------------+
    | RSA-OAEP-SHA256 `[rsa-oaep-sha-256]`                                |     S     |   S      | RSA-2048                 |
    +---------------------------------------------------------------------+-----------+----------+--------------------------+

.. _canton_default_schemes:

.. table:: Default Cryptographic Schemes

   +-----------------------------------------------+--------------------------------------------+-------------------------------+
   | **Crypto provider**                           | JCE Default Scheme                         | KMS Default Scheme            |
   +===============================================+============================================+===============================+
   | Signing key specification                     | EC-Curve25519                              | EC-P256                       |
   +-----------------------------------------------+--------------------------------------------+-------------------------------+
   | Signing algorithm specification               | Ed25519                                    | EC-DSA-SHA256                 |
   +-----------------------------------------------+--------------------------------------------+-------------------------------+
   | Asymmetric encryption key specification       | EC-P256                                    | RSA-2048                      |
   +-----------------------------------------------+--------------------------------------------+-------------------------------+
   | Asymmetric encryption algorithm               | ECIES with HMAC-SHA256 and AES128-CBC      | RSA with OAEP and SHA-256     |
   +-----------------------------------------------+--------------------------------------------+-------------------------------+
   | Symmetric encryption scheme [1]_              | AES128-GCM                                 | Same as JCE                   |
   +-----------------------------------------------+--------------------------------------------+-------------------------------+
   | Hash Algorithm [2]_                           | SHA-256                                    | Same as JCE                   |
   +-----------------------------------------------+--------------------------------------------+-------------------------------+
   | PBKDF [3]_                                    | Argon2id                                   | Same as JCE                   |
   +-----------------------------------------------+--------------------------------------------+-------------------------------+

.. [1] Default and only supported scheme; not configurable.
.. [2] Default and only supported hash algorithm; not configurable.
.. [3] Default and only supported PBKDF; not configurable.

.. _key_configuration_external_keys:

.. table:: Key configuration for external keys with a Key Management Service (KMS)

    +-------------------+-----------------------------------------------------------------------+-----------------------------------------------------+
    | **Provider**      | **SIGNING**                                                           | **ENCRYPTION**                                      |
    +===================+=======================================================================+=====================================================+
    | AWS               | - **Key Purpose:** `SIGN_VERIFY`                                      | - **Key Purpose:** `ENCRYPT_DECRYPT`                |
    |                   | - **Key Algorithms:** `ECC_NIST_P256` or `ECC_NIST_P384`              | - **Key Algorithm:** `RSA_2048`                     |
    +-------------------+-----------------------------------------------------------------------+-----------------------------------------------------+
    | GCP               | - **Key Purpose:** `ASYMMETRIC_SIGN`                                  | - **Key Purpose:** `ASYMMETRIC_DECRYPT`             |
    |                   | - **Key Algorithms:** `EC_SIGN_P256_SHA256` or `EC_SIGN_P384_SHA384`  | - **Key Algorithm:** `RSA_DECRYPT_OAEP_2048_SHA256` |
    +-------------------+-----------------------------------------------------------------------+-----------------------------------------------------+
    | Driver            | - Must be compatible with `EC_P256_SHA256`  or `EC_P384_SHA384`       | - Must be compatible with  `RSA_OAEP_2048_SHA256`   |
    +-------------------+-----------------------------------------------------------------------+-----------------------------------------------------+

.. _canton_supported_key_formats:

.. table:: Supported Cryptographic Key Formats by Key Type

    +--------------------+----------------------+------------------------------------------------------+
    | **Key Type**       | Format (Config)      | Format (gRPC / Protobuf)                             |
    +====================+======================+======================================================+
    | Public Key         | `der-x-509-spki`     | `CRYPTO_KEY_FORMAT_DER_X509_SUBJECT_PUBLIC_KEY_INFO` |
    +--------------------+----------------------+------------------------------------------------------+
    | Private Key        | `der-pkcs-8-pki`     | `CRYPTO_KEY_FORMAT_DER_PKCS8_PRIVATE_KEY_INFO`       |
    +--------------------+----------------------+------------------------------------------------------+
    | Symmetric Key [4]_ | `raw`                | `CRYPTO_KEY_FORMAT_RAW`                              |
    +--------------------+----------------------+------------------------------------------------------+

.. [4] Symmetric keys are only used internally. A node operator will not operate on symmetric keys; this entry is provided for reference only. The only key format a node operator is expected to interact with is `X.509` public keys.

.. table:: Commands and Flags to Export Keys in Supported Formats

    +---------------+---------------------------------------+----------------+---------------------------+---------------------------+-----------------------------+--------------------------------------------------------------------+
    | **Key Type**  | **Supported Format**                  | **OpenSSL**    | **GCP KMS**               | **AWS KMS**               | **Java.security**           | **Python (cryptography)**                                          |
    +===============+=======================================+================+===========================+===========================+=============================+====================================================================+
    | Public Key    | DER SPKI (X.509 SubjectPublicKeyInfo) | `-outform DER` | keys default to DER SPKI  | keys default to DER SPKI  | keys default to DER SPKI    | `public_bytes(Encoding.DER, PublicFormat.SubjectPublicKeyInfo)`    |
    +---------------+---------------------------------------+----------------+---------------------------+---------------------------+-----------------------------+------------------------+-------------------------------------------+
    | Private Key   | DER PKCS#8 PrivateKeyInfo             | `-outform DER` | n/a                       | n/a                       | keys default to DER PKCS#8  | `private_bytes(Encoding.DER, PrivateFormat.PKCS8)`                 |
    +---------------+---------------------------------------+----------------+---------------------------+---------------------------+-----------------------------+------------------------+-------------------------------------------+
    | Symmetric Key | raw bytes                             |  n/a           | n/a                       | n/a                       | keys default to raw bytes   | keys default to raw bytes                                          |
    +---------------+---------------------------------------+----------------+---------------------------+---------------------------+-----------------------------+------------------------+-------------------------------------------+

.. _canton_supported_signature_formats:

.. table:: Supported Cryptographic Signature Formats by Signing Algorithm Specifications

    +---------------------------------------+---------------------------+----------------------------------------+
    | **Signing Algorithm Specifications**  | Signature Format (Config) | Signature Format (gRPC / Protobuf)     |
    +=======================================+===========================+========================================+
    | Ed25519                               | `concat`                  | `SIGNATURE_FORMAT_CONCAT`              |
    +---------------------------------------+---------------------------+----------------------------------------+
    | EC-DSA-SHA256                         | `der`                     | `SIGNATURE_FORMAT_DER`                 |
    +---------------------------------------+---------------------------+----------------------------------------+
    | EC-DSA-SHA384                         | `der`                     | `SIGNATURE_FORMAT_DER`                 |
    +---------------------------------------+---------------------------+----------------------------------------+

.. note::

    The currently supported signature formats in Canton align with the default signature formats
    used when generating signatures with OpenSSL, Java, or Python (`cryptography` package).
