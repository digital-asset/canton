# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import time

import grpc
from cryptography.hazmat.primitives.asymmetric.ec import EllipticCurvePrivateKey
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives import serialization
from grpc import Channel

from com.daml.ledger.api.v2.admin import (
    party_management_service_pb2_grpc,
    party_management_service_pb2
)
from com.digitalasset.canton.protocol.v30 import topology_pb2
from com.digitalasset.canton.crypto.v30 import crypto_pb2
from com.daml.ledger.api.v2 import crypto_pb2 as ledger_api_crypto_pb2
from interactive_topology_util import (
    compute_fingerprint,
    compute_sha256_canton_hash,
    serialize_topology_transaction,
    sign_hash,
)

def build_serialized_transaction_and_hash(
    mapping: topology_pb2.TopologyMapping,
) -> (bytes, bytes):
    """
    Generates a serialized topology transaction and its corresponding hash.

    Args:
        mapping (topology_pb2.TopologyMapping): The topology mapping to be serialized.

    Returns:
        tuple: A tuple containing:
            - bytes: The serialized transaction.
            - bytes: The SHA-256 hash of the serialized transaction.
    """
    transaction = serialize_topology_transaction(mapping)
    transaction_hash = compute_sha256_canton_hash(11, transaction)
    return transaction, transaction_hash


# Onboard a new external party
def onboard_external_party(
    party_name: str,
    confirming_participant_ids: [str],
    confirming_threshold: int,
    synchronizer_id: str,
    channel: Channel,
) -> (EllipticCurvePrivateKey, str):
    """
    Onboard a new external party.
    Generates an in-memory signing key pair to authenticate the external party.

    Args:
        party_name (str): Name of the party.
        confirming_participant_ids (str): Participant IDs on which the party will be hosted for transaction confirmation.
        confirming_threshold (int): Minimum number of confirmations that must be received from the confirming participants to authorize a transaction.
        synchronizer_id (str): ID of the synchronizer on which the party will be registered.
        channel (grpc.Channel): gRPC channel to one of the confirming participants Admin API.

    Returns:
        tuple: A tuple containing:
            - EllipticCurvePrivateKey: Private key created for the party.
            - str: Fingerprint of the public key created for the party.
    """
    print(f"Onboarding {party_name}")

    ledger_api_client = (
        party_management_service_pb2_grpc.PartyManagementServiceStub(channel)
    )

    # For the sake of simplicity in the demo, we use a single signing key pair for the party namespace (used to manage the party itself on the network),
    # and for the signing of transactions via the interactive submission service. We however recommend to use different keys in real world deployment for better security.
    private_key = ec.generate_private_key(curve=ec.SECP256R1())
    public_key = private_key.public_key()

    # Extract the public key in the DER format
    public_key_bytes: bytes = public_key.public_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    # Wrap the public key in a Canton protobuf message
    signing_public_key = crypto_pb2.SigningPublicKey(
        # Must match the format to which the key was exported to above
        format=crypto_pb2.CryptoKeyFormat.CRYPTO_KEY_FORMAT_DER_X509_SUBJECT_PUBLIC_KEY_INFO,
        public_key=public_key_bytes,
        # Must match the scheme of the key
        scheme=crypto_pb2.SigningKeyScheme.SIGNING_KEY_SCHEME_EC_DSA_P256,
        # Because we have only one key, we specify both NAMESPACE and PROTOCOL usage for it
        # When using different keys, ensure to use only the correct usage for each
        usage=[
            crypto_pb2.SigningKeyUsage.SIGNING_KEY_USAGE_NAMESPACE,
            crypto_pb2.SigningKeyUsage.SIGNING_KEY_USAGE_PROTOCOL,
        ],
        # This field is deprecated in favor of scheme but python requires us to set it
        key_spec=crypto_pb2.SIGNING_KEY_SPEC_EC_P256,
    )

    public_key_fingerprint = compute_fingerprint(public_key_bytes)

    # The party id is constructed with party_name :: fingerprint
    # This must be the fingerprint of the _namespace signing key_
    party_id = party_name + "::" + public_key_fingerprint

    # Party to participant: records the fact that the party wants to be hosted on the participants with confirmation rights
    # This means those participants are not allowed to submit transactions on behalf of this party but will validate transactions
    # on behalf of the party by confirming or rejecting them according to the ledger model. They also records transaction for that party on the ledger.
    # It also contains the protocol signing keys and signing threshold for the party
    confirming_participants_hosting = []
    for confirming_participant_id in confirming_participant_ids:
        confirming_participants_hosting.append(
            topology_pb2.PartyToParticipant.HostingParticipant(
                participant_uid=confirming_participant_id,
                permission=topology_pb2.Enums.ParticipantPermission.PARTICIPANT_PERMISSION_CONFIRMATION,
            )
        )
    party_to_participant_mapping = topology_pb2.TopologyMapping(
        party_to_participant=topology_pb2.PartyToParticipant(
            party=party_id,
            threshold=confirming_threshold,
            participants=confirming_participants_hosting,
            party_signing_keys= crypto_pb2.SigningKeysWithThreshold(
                # This is the same key the party id was generated from, and is consequently
                # used both as the namespace key and protocol signing key
                keys = [signing_public_key],
                threshold=1,
            )
        )
    )
    (party_to_participant_transaction, party_to_participant_transaction_hash) = (
        build_serialized_transaction_and_hash(party_to_participant_mapping)
    )

    signature = sign_hash(private_key, party_to_participant_transaction_hash)

    allocate_external_party_request = (
        party_management_service_pb2.AllocateExternalPartyRequest(
            onboarding_transactions=[
                party_management_service_pb2.AllocateExternalPartyRequest.SignedTransaction(
                    transaction=party_to_participant_transaction,
                    signatures=[
                        ledger_api_crypto_pb2.Signature(
                            signature=signature,
                            signed_by=public_key_fingerprint,
                            signing_algorithm_spec = ledger_api_crypto_pb2.SIGNING_KEY_SPEC_EC_P256,
                            format=ledger_api_crypto_pb2.CRYPTO_KEY_FORMAT_DER,
                        )
                    ],
                )
            ],
            wait_for_allocation=True,
            synchronizer=synchronizer_id,
        )
    )
    ledger_api_client.AllocateExternalParty(allocate_external_party_request)

    return private_key, public_key_fingerprint
