<picture>
  <source
    media="(prefers-color-scheme: dark)"
    srcset="https://github.com/digital-asset/.github/raw/main/images/Canton - Horizontal-stack-Logo-White.png"
  >
  <img
    alt="Canton"
    src="https://github.com/digital-asset/.github/raw/main/images/Canton-Vertical-logo-Black-logo-Yellow-C-Black-diamond.png"
    width="50%"
  >
</picture>

<br/><br/>

Canton is a next-generation Daml ledger interoperability protocol that faithfully implements Daml’s built-in models of authorization and privacy.

- By partitioning global state, it solves both the privacy challenges and scaling bottlenecks of platforms such as a single Ethereum instance.
- It allows developers to balance auditability requirements with the right to forget, making it well-suited for building GDPR-compliant systems.
- Canton handles authentication and data transport through so-called synchronizers.
- Synchronizers can be deployed as needed to address scalability, operational, or trust concerns.
- Synchronizers are permissioned but can be federated at no interoperability cost, yielding a virtual global ledger that enables truly global workflow composition.

Refer to the [Canton Whitepaper](https://www.canton.io/publications/canton-whitepaper.pdf) for further details.

## Documentation

Please refer to the [documentation](https://docs.digitalasset.com/) for instructions on how to operate a participant node or a synchronizer.

## Development

Please read our [contributing guidelines](CONTRIBUTING.md).