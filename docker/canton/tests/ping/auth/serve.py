#!/usr/bin/env python3
# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Minimal JWKS server plus a pre-minted audience-based token for the smoke stack.

Canton's ledger API is configured with an auth-service of type ``jwt-jwks`` (see
docker/canton/images/canton-participant/app.conf). That verifier fetches a JWKS
document over HTTP and validates RS256 tokens whose ``aud`` contains the
configured ``target-audience``. This service:

  1. generates an RSA keypair at startup (no secret is committed to the repo),
  2. serves the public half as a JWKS at ``/.well-known/jwks.json``,
  3. mints one long-lived RS256 token (``aud`` = target audience, ``sub`` = the
     ledger user id PQS authenticates as) and serves it at ``/token``,
  4. writes the same ``token`` and ``jwks.json`` into a shared volume so
     containers that cannot speak HTTP at startup (PQS) can read the token from
     a file.

The token shape matches what Canton expects for an audience-based token, proven
by community/ledger/ledger-api-core/src/test/scala/.../AuthServiceJWTCodecSpec.scala:
``{"aud": ["<audience>"], "sub": "<userId>", "exp": <epoch-seconds>}``.

Everything is driven by env vars so the smoke can retarget audience/subject
without rebuilding the image.
"""

import base64
import json
import os
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
import jwt

AUDIENCE = os.environ.get("AUTH_TARGET_AUDIENCE", "participant")
SUBJECT = os.environ.get("AUTH_TOKEN_SUBJECT", "pqs-user")
KEY_ID = os.environ.get("AUTH_KEY_ID", "smoke-key-1")
PORT = int(os.environ.get("AUTH_PORT", "8080"))
SHARED_DIR = os.environ.get("AUTH_SHARED_DIR", "/shared")
# Outlives any smoke run (the participant allows unlimited token lifetime).
TOKEN_TTL_SECONDS = int(os.environ.get("AUTH_TOKEN_TTL_SECONDS", str(10 * 365 * 24 * 3600)))


def _b64url_uint(value: int) -> str:
    raw = value.to_bytes((value.bit_length() + 7) // 8, "big")
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def build_material():
    """Return (jwks_json, token_str) for a freshly generated keypair."""
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    numbers = private_key.public_key().public_numbers()

    jwks = {
        "keys": [
            {
                "kty": "RSA",
                "use": "sig",
                "alg": "RS256",
                "kid": KEY_ID,
                "n": _b64url_uint(numbers.n),
                "e": _b64url_uint(numbers.e),
            }
        ]
    }

    now = int(time.time())
    claims = {
        "aud": [AUDIENCE],
        "sub": SUBJECT,
        "iat": now,
        "exp": now + TOKEN_TTL_SECONDS,
    }
    private_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    token = jwt.encode(claims, private_pem, algorithm="RS256", headers={"kid": KEY_ID})
    return json.dumps(jwks), token


def write_shared(jwks_json: str, token: str) -> None:
    try:
        os.makedirs(SHARED_DIR, exist_ok=True)
        with open(os.path.join(SHARED_DIR, "jwks.json"), "w") as fh:
            fh.write(jwks_json)
        with open(os.path.join(SHARED_DIR, "token"), "w") as fh:
            fh.write(token)
        print(f"auth: wrote token and jwks.json to {SHARED_DIR}", flush=True)
    except OSError as exc:
        # HTTP is the source of truth, a failed write must not stop the service.
        print(f"auth: could not write shared dir {SHARED_DIR}: {exc}", flush=True)


JWKS_JSON, TOKEN = build_material()
write_shared(JWKS_JSON, TOKEN)


class Handler(BaseHTTPRequestHandler):
    def _send(self, body: str, content_type: str) -> None:
        payload = body.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self):  # noqa: N802 (http.server API)
        if self.path == "/.well-known/jwks.json":
            self._send(JWKS_JSON, "application/json")
        elif self.path == "/token":
            self._send(TOKEN, "text/plain")
        elif self.path == "/health":
            self._send("ok", "text/plain")
        else:
            self.send_error(404, "not found")

    def log_message(self, fmt, *args):
        # Keep the smoke log readable: one compact line per request.
        print("auth: " + (fmt % args), flush=True)


def main():
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(
        f"auth: serving JWKS on :{PORT} (aud={AUDIENCE!r}, sub={SUBJECT!r}, kid={KEY_ID!r})",
        flush=True,
    )
    server.serve_forever()


if __name__ == "__main__":
    main()
