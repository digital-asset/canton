# Canton

Canton is a next-generation distributed DAML runtime that implements DAML's built-in models of authorization and
privacy faithfully.

* By partitioning the global state it solves both the privacy problems and the scaling bottlenecks of platforms such as
  Ethereum.

* It allows developers to balance auditability requirements with the right to forget, making it well-suited for building
  GDPR-compliant systems.

* Canton handles authentication and data transport through our so-called synchronization domains.

* Domains can be deployed at will to address scalability, operational or trust concerns.

* They are permissioned but can be federated at no interoperability cost, yielding a virtual global ledger that enables
  truly global workflow composition.

Refer to the [Canton Whitepaper](https://www.canton.io/publications/canton-whitepaper.pdf) for further details.

## Availability

This repository is hosting an early version of Canton (pre-alpha) and only
provides binary releases. At this point, Canton is not open source. Refer to the
[LICENSE](LICENSE.txt) for details.

## Running

Please read [Getting
Started](https://www.canton.io/docs/stable/user-manual/tutorials/getting_started.html)
for instructions on how to get started with Canton.

Consult the [Canton User
Manual](https://www.canton.io/docs/stable/user-manual/index.html) for further
references of Canton's configuration, command-line arguments, or its console.
