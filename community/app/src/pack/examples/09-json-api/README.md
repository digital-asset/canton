# Json Api Example

Demonstrates running Canton and interacting with it via the **Ledger JSON API**.

Uses `curl` and `websocat` to communicate with Ledger JSON API.

Detailed description: https://docs.canton.network/appdev/modules/m4-json-api-tutorial.

## Prerequisites

  - Bash-compatible terminal
  - DPM (Daml Package Manager) (https://docs.canton.network/sdks-tools/cli-tools/dpm)
  - curl (https://github.com/curl/curl)
  - jq (https://github.com/jqlang/jq)
  - optional
    - Node.js (version 18.20 or later) and npm (https://nodejs.org/en/download/)
    - websocat (https://github.com/vi/websocat)

## Scenario
- create two parties
- create an Iou contract
- transfer the Iou contract to another party

## Running

### Preparation
- build canton: `sbt compile`
- build the release bundle: `sbt bundle`
- navigate to the example dir: `cd <code_repo_root>/community/app/target/release/canton/examples/09-json-api`

### Running the example
Open two terminal windows in the above dir.

- first window
  - run `./run.sh`
    - builds the DAR, starts Canton in interactive mode, bootstraps the participant and synchronizer and loads the DAR
- second window
  - run `./scenario.sh`
    - runs the scenario steps
    - creates contracts, exercises choices and queries active contracts via Ledger JSON API
    - can be run multiple times
## Further steps
- refer to https://docs.canton.network/appdev/modules/m4-json-api-tutorial to do the following
  - generate TypeScript code bindings for the Ledger JSON API and the contract templates
  - run the scenario with TypeScript and websocket via npm
