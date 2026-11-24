# Deployment smoke (L2)

This stack extends the original single-participant ping test into a black-box
deployment smoke that exercises the packaged Canton images across the real
network, auth, and query-store boundaries the in-process suites never cross.

## What the stack contains

| Service        | Role                                                                          |
| -------------- | ----------------------------------------------------------------------------- |
| `db`           | One Postgres, one database per node (`participant`, `participant2`, `mediator`, `sequencer`, `sequencer_driver`, `pqs`). `sequencer_driver` is the BFT orderer's database, its name is fixed by `canton-sequencer/app.conf`. |
| `auth`         | JWKS server + a pre-minted long-lived RS256 token (`aud=participant`, `sub=pqs-user`). Serves `/.well-known/jwks.json` and `/token`, and writes the token to a shared volume for PQS. |
| `participant`  | Participant 1. Provisions `pqs-user` as a ledger user, grants it read-as-any-party, and uploads the standard CantonExamples DAR plus one self-issued `Iou` so PQS has a user Daml model and a contract to ingest. |
| `participant2` | Participant 2 on the same synchronizer, own database. Not exercised by the smoke phases yet, kept for cross-participant scenarios. |
| `sequencer` / `mediator` / `synchronizer` | Sequencer (BFT orderer), mediator and synchronizer bootstrap, the same nodes as the base smoke. |
| `pqs`          | Participant Query Store. Reads participant 1's ledger stream with the minted token and projects it into the `pqs` database. Profile-gated (see below). |
| `test`         | The feature pack. Runs the smoke phases and is the container the run exits from. |

## Smoke phases (`test/run.sh`)

0. Admin API reachable and reports a healthy synchronizer connection (the original check).
1. The minted token is **accepted** on the ledger API (`jwt-jwks`), and an unauthenticated call is **rejected** (auth is really enforced).
2. Drive real ledger activity on participant 1 only: discover a party hosted on participant 1, then self-ping it via the admin `PingService` so contracts flow through the stream. Participant 2 is not involved.
3. **PQS visibility oracle**: PQS's `active('Iou:Iou')` returns the seed contract. The bootstrap seeds a self-issued `Iou` from the CantonExamples DAR so PQS has a user Daml model (it aborts on an empty one) and a contract row to project. The participant healthcheck waits for a bootstrap-complete marker so PQS only starts once the DAR and seed contract exist.

Phases 1-3 are env-driven and skip cleanly if their targets are not configured,
so the container stays backward compatible with the original ping-only test.

## Running

CI runs the base smoke automatically. Locally, `./build_canton_image.sh false`
from `docker/canton` builds the images and runs it.

Base smoke (no PQS image needed, runs phases 0-2):

```bash
./run_test.sh
```

Full smoke (adds PQS + phase 3). Set `PQS_IMAGE` and `run_test.sh` then enables
the `pqs` compose profile and the PQS assertion automatically:

```bash
PQS_IMAGE=<canonical-pqs-image-ref> ./run_test.sh
```

For released per-node canton images pass the registry path + tag as the original
test does, for example:

```bash
OCI_PATH="<registry>/da-images/public-unstable/docker/" \
PQS_IMAGE="<registry>/.../participant-query-store:<tag>" \
  ./run_test.sh <release_suffix>
```

The `Deployment Smoke Nightly` GHA workflow runs the full smoke against the
newest `public-unstable` snapshot tags of this branch's `VERSION` line, for both
Canton and PQS. It fails if either newest tag is more than 10 days old:

```bash
./run_latest_snapshot.sh
```

## Images

Snapshot images of Canton (`canton-base`, `canton-participant`,
`canton-sequencer`, `canton-mediator`) and PQS (`participant-query-store`) can
be pulled without auth from `europe-docker.pkg.dev/da-images/public-unstable/docker/`.

The `pqs` service targets the 3.8 PQS CLI (`/pqs.jar`, `PQS_*` env). Older
`scribe` images use `/scribe.jar` and `SCRIBE_*` instead. Override the ledger
start offset with `PQS_LEDGER_START` (default `Genesis`) and the auth mode with
`PQS_LEDGER_AUTH` (default `OAuth`). There is no token-file flag, so the service
reads `/shared/token` and passes it inline as `--pipeline-oauth-accesstoken`.
