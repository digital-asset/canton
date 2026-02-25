# Snippet Macros

This file serves as short documentation for the existing macros

## allocateExternalParty

The `allocateExternalParty` macro, as its name indicates, allocates an external party using the Ledger API and makes its Party ID available on the console under the `externalParty` value.
The whole allocation is hidden, as in it will not show anything in the RST file / final doc page.
An ED25519 signing key pair is created on disk using openssl and can be used to sign data on behalf of the external party to demonstrate a workflow.
When doing so, make sure to clearly point out that this is an example for demonstration purposes but that keys in real world deployments must be safely stored and accessed.
The keys are stored in a temporary folder tied to the scenario and can be accessed in shell snippets under the names `public_key.der` and `private_key.der`.
Example for signing:

```
.. shell:: openssl pkeyutl -sign -inkey private_key.der -rawin -in tx_hash.hash -out tx_sig.sig -keyform DER
```
