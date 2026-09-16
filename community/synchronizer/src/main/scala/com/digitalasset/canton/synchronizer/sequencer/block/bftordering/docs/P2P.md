# CantonBFT — P2P networking subsystem

Context doc for the peer-to-peer layer of the BFT block orderer: how sequencer nodes find,
**authenticate**, connect to, deduplicate, maintain, and tear down the links over which they
exchange protocol messages.

Source root (paths below are relative to it unless they start with `community/`):
`community/synchronizer/src/main/scala/com/digitalasset/canton/synchronizer/sequencer/block/bftordering/`

**Authentication is the spine of this subsystem**, so it is introduced first (§2) and then woven
through the establishment and management flows that build on it (§3–§5). The modules, wire protocol,
and config that frame all of it are summarised at the end.

---

## 1. The mental model: four handles for one peer

The whole subsystem is easier to read once you see that a single remote peer is referred to by
**four different handles**, learned at different times and serving different purposes. Most of the
complexity is keeping the mapping between them consistent.

| Handle | "Answers" | Known from | Lives in |
|---|---|---|---|
| `P2PEndpoint` / `P2PEndpoint.Id` = `(address, port, tls)` | **where** | config / admin / the header a dialing peer sends | `bindings/p2p/grpc/P2PGrpcNetworking.scala` |
| `BftNodeId` (derived from a `SequencerId`) | **who** | only **after authentication** | `framework/data/BftOrderingIdentifiers.scala` |
| `PeerSender` | **the outbound half of one gRPC stream** | when a bidi stream is opened | `P2PGrpcConnectionManager` (companion) |
| `P2PNetworkRef` (+ a Pekko "connection-managing actor") | **the module-facing send handle** | when the out-module first needs to reach a peer | `PekkoP2PGrpcNetworking.scala` |

Two consequences drive the design:

- **You dial an endpoint but address a node.** Outbound connectivity is keyed by `P2PEndpoint.Id`
  (you only know *where*). But sends from the protocol are keyed by `BftNodeId` (you know *who*).
  **Authentication is the step that turns a *where* into a verified *who***
  (`completeConnectivitySetupAfterSuccessfulAuthentication`,
  §2). Until that bridge exists, a send to that node is **dropped, not queued**
  (`P2PNetworkOutModule.networkSendIfKnown`). A connection that hasn't authenticated is, for routing
  purposes, not a connection at all.

- **`P2PAddress` is the "which do I know?" union.** `P2PAddress.Endpoint(ep)` = I know where but
  not who (a freshly-configured peer, pre-auth); `P2PAddress.NodeId(id, maybeEp)` = I know who
  (post-auth). Its `id: Either[P2PEndpoint.Id, BftNodeId]` is the lookup key used throughout the
  connection state.

### Two state machines, two concerns

Connection state is split across two files, by concern:

- **`P2PGrpcConnectionManager.State`** — *the channel/worker lifecycle*, per endpoint:
  `P2POutgoingConnectionStatus` ∈ {`Connecting`, `ConnectingOnChannel`, `ConnectedOnChannel`,
  `DisconnectingFromChannel`}. This tracks the gRPC `ManagedChannel`, the optional
  `GrpcSequencerClientAuth` context, and the running *connect worker*.
- **`P2PGrpcConnectionState.State`** (`bindings/p2p/grpc/P2PGrpcConnectionState.scala`) — *identity &
  routing*: the maps `endpointId→nodeId`, `nodeId↔peerSender`, `nodeId→networkRef`,
  `endpointId→networkRef`. This is where the result of authentication is recorded, and where
  deduplication, consolidation, and equivocation checks live.

The **connect worker** that appears repeatedly below is the recursive `FutureUnlessShutdown` loop in
`P2PGrpcConnectionManager.createPeerSender`: it opens the stream, sends the handshake, **awaits
authentication**, and retries on failure. Authentication is thus not a phase bolted onto the end of
connection setup — it is the gate the worker blocks on before the connection counts.

---

## 2. Authentication mechanics

Authentication is **on by default** (`P2PNetworkAuthenticationConfig.enabled = true`); it is only
absent when no auth services are available or in `standalone` mode (`BftBlockOrderer`'s
`maybeAuthenticationServices`). The result is **mutual** sequencer-to-sequencer authentication: each
end finishes a handshake holding a *cryptographically verified* `SequencerId` for the other, which
becomes the peer's `BftNodeId`. Everything in §3–§5 is built on this.

### 2.1 The trick: reuse Canton's sequencer member-authentication, in both directions

BFT peers are themselves sequencers, so the subsystem reuses Canton's standard *sequencer member
authentication* (challenge/nonce → sign with the member key → bearer token) rather than inventing a
new scheme. To make it mutual, **each node serves its own `SequencerAuthenticationService` on the
P2P port** (`BftBlockOrderer.createServer`, "so that the BFT orderers don't have to also know the
sequencer API endpoints"). So to prove *my* identity to peer X, I run the member-auth handshake
against **X's** auth service, obtain a token X will accept, and present it to X; X verifies it with
its own `MemberAuthenticationService`. Both ends do this, but over different gRPC header channels.

A node's P2P server therefore exposes two services and chains two interceptors over the ordering
service (`createServer`, filters applied in reverse list order):

| Component | Side | File | Role |
|---|---|---|---|
| `GrpcSequencerClientAuth` | client (dialer) | Canton core | attaches **the dialer's** token to *request* headers (token obtained from the dialed endpoint's auth service) |
| `authenticationServerInterceptor` (standard) | server | Canton core | verifies the dialer's request-header token, stores the dialer's member id in the gRPC `Context` |
| `ServerAuthenticatingServerInterceptor` | server | `authentication/ServerAuthenticatingServerInterceptor.scala` | on `sendHeaders`, fetches **the server's own** token and writes it into *response* headers; also lifts the client's advertised endpoint into the `Context` |
| `AuthenticateServerClientInterceptor` | client (dialer) | `authentication/AuthenticateServerClientInterceptor.scala` | on `onHeaders`, verifies the server's response-header token via `MemberAuthenticator.authenticate`, extracts the server's `SequencerId`, completes the promise |
| `AddEndpointHeaderClientInterceptor` | client (dialer) | `authentication/AddEndpointHeaderClientInterceptor.scala` | puts the dialer's **own external endpoint** into a binary request header (`ENDPOINT_METADATA_KEY`) |
| `SequencerAuthenticationService` | server | Canton core | issues tokens to peers handshaking against this node |

### 2.2 The two directions

```
            Node A (dials)                                   Node B (accepts)
  ┌──────────────────────────────┐                ┌──────────────────────────────────┐
  │ BftOrderingService stub:      │                │ BftOrderingService (Receive):     │
  │  + AddEndpointHeader(A.ext) ──┼── A.ext in req ►│  ServerAuthenticatingServerIcptr  │
  │  + GrpcSequencerClientAuth  ──┼── token(A) in ─►│  authenticationServerInterceptor: │
  │     token from B's auth svc   │   req headers   │     verify token(A) → store A's   │
  │                               │                 │     id in gRPC Context            │
  │  + AuthenticateServerClient ◄─┼── token(B) in ──┤  ServerAuthenticating: fetch      │
  │     verify token(B) →         │   resp headers  │     token(B) from A's auth svc    │
  │     B's SequencerId (promise) │                 │     (A.ext) → resp headers        │
  │                               │                 │                                   │
  │ SequencerAuthenticationSvc ◄──┼── B handshakes ─┤ SequencerAuthenticationSvc        │
  │  (issues token to B)          │   to get token  │  (issued token to A earlier)      │
  └──────────────────────────────┘                 └──────────────────────────────────┘
     A learns B  (from resp headers)                   B learns A  (from req headers→Context)
```

- **Client → server (standard).** `GrpcSequencerClientAuth` (built in `openGrpcChannel`, with
  `member = auth.sequencerId`, `endpoint = the dialed peer`) obtains a token from B's auth service
  and attaches it to every request. B's standard `authenticationServerInterceptor` verifies it and
  stashes A's member id in the gRPC `Context`. → **B learns A** (the source the *incoming* path reads,
  §4).
- **Server → client (reverse — the custom part).** When B sends its response headers,
  `ServerAuthenticatingServerInterceptor.sendHeaders` synchronously fetches **B's own** token via a
  `ChannelTokenFetcher` over a short-lived channel to **A's advertised endpoint** and writes it into
  the response headers (`SequencerClientTokenAuthentication.authenticationMetadata`). On A,
  `AuthenticateServerClientInterceptor.onHeaders` extracts the credentials
  (`MemberAuthenticator.extractAuthenticationCredentials`), verifies them
  (`MemberAuthenticator.authenticate`), parses the `member` into a `SequencerId`, and completes the
  promise the connect worker is waiting on. → **A learns B** (the source the *outgoing* path reads,
  §3).

So both sides end up with the *peer's* verified `SequencerId`; **which interceptor delivers it
depends on this node's role for that connection** — the dialer waits on the reverse-flow promise, the
acceptor reads the standard-flow `Context`. This single fact explains the central asymmetry between
the outgoing (§3) and incoming (§4) flows.

### 2.3 Why endpoint advertisement is part of authentication (not just dedup)

`AddEndpointHeaderClientInterceptor` is load-bearing for the reverse flow: B can only fetch a token
that *A* will accept by handshaking against A's `SequencerAuthenticationService`, and it learns where
that is from the endpoint A advertised. If the header is missing,
`ServerAuthenticatingServerInterceptor` closes the call with `Status.INTERNAL` ("No authenticated
endpoint header found"). The same advertised endpoint is *also* reused for connection deduplication
(§5.1) — one header, two purposes.

### 2.4 Initial state, failures, and the auth-off path

- **`AuthenticationInitialState`** (`P2PGrpcNetworking.scala`) carries what the interceptors need:
  `psId`, this node's `sequencerId`, the bundle `authenticationServices`
  (`memberAuthenticationService`, `sequencerAuthenticationService`, `authenticationServerInterceptor`,
  `syncCryptoForAuthentication`), `authTokenConfig`, `serverToClientAuthenticationEndpoint`, and a
  `clock`. It is `None` exactly when authentication is disabled — that `Option`
  (`isAuthenticationEnabled`) is the single switch read throughout.
- **Failures terminate the connection, then it retries.** Client-side verification failures
  `cancel` the call with `Status.UNAUTHENTICATED` and fail the promise → the connect worker's
  `await` fails → §5.3 retry/backoff. The 5s `AuthenticationTimeout` in `P2PGrpcStreamingReceiver`
  guards against a peer that never supplies an id (and prevents auth from blocking shutdown).
- **Auth-off path.** With no tokens, identity is taken on trust from the `ConnectionOpened` / first
  frame's `sentBy` (`P2PGrpcStreamingReceiver` backfills the promise). The integrity checks in §5.2
  (equivocation, `sentBy` match) still apply, but there is no cryptographic proof of identity —
  hence the config comment to keep auth on in production (or rely deliberately on mTLS alone).

---

## 3. Establishing an outgoing connection

Trigger: the out-module wants to reach a peer (initial peers on `Start`, an admin `AddEndpoint`, or
just the first `Multicast` to a node it isn't connected to yet). It calls
`p2pConnectionState.addNetworkRefIfMissing(addressId){ … }{ createNetworkRef }`, which (for a new
peer) spawns a **connection-managing actor** (`PekkoP2PGrpcNetworkManager.createNetworkRef`). That
actor immediately sends itself `Initialize` to connect **eagerly**, before any message is queued.

The flow is, end to end, *dial → channel → stream → mutual authentication → bind identity → usable*.
Authentication (§2) is steps 5–7 here, inline:

```
out-module          conn-mgr actor         P2PGrpcConnectionManager            remote peer (B)
   │  createNetworkRef                                                              │
   ├───────────────► (spawn) ──Initialize──►                                        │
   │                          getPeerSenderOrStartConnection(Endpoint(ep))          │
 1 │                                   │  no sender yet → connectIfNeeded(ep)        │
   │                                   │  state: ∅ → Connecting                      │
 2 │                                   │  openGrpcChannel(ep):                       │
   │                                   │    • build ManagedChannel (TLS per cfg)     │
   │                                   │    • GrpcSequencerClientAuth → my token     │  §2.2 client→server
   │                                   │      (handshakes B's auth svc) on requests  │
   │                                   │    • AddEndpointHeader(my ext ep)           │  §2.3 (needed for reverse)
   │                                   │    • AuthenticateServerClient (verifies B) ┐│
   │                                   │    state: Connecting → ConnectingOnChannel  │
 3 │                                   │  onConnect(ep) ───────────────► (Network.Connected)
 4 │                                   │  start CONNECT WORKER:                  │   │
   │                                   │    (jittered initial delay ≤500ms)      │   │
   │                                   │    asyncStub.receive(peerReceiver) ─── bidi stream ──►
   │                                   │    peerSender.onNext(ConnectionOpened{sentBy=me}) ──►
 5 │                                   │    ── my token rides on requests ───────────►  B verifies (learns me)
 6 │                                   │    ◄─ B's token in response headers ─────┘   │  §2.2 server→client
   │                                   │    AuthenticateServerClient verifies → B's SequencerId
   │                                   │    state: ConnectingOnChannel → ConnectedOnChannel
 7 │                                   │  completeConnectivitySetupAfter‐            │
   │                                   │    SuccessfulAuthentication(seqId,sender,ep):│
   │                                   │    nodeId = toBftNodeId(seqId)               │
   │                                   │    associate endpointId→nodeId  (where→who)  │
   │                                   │    addSenderIfMissing(nodeId, sender)=true   │
   │  ◄──onNodeId(nodeId,ep)────────────────────────────────────────────────         │
   │  (Network.Authenticated → ensureSendingEnabledTo + startModulesIfNeeded)        │
```

Key points, with the code that enforces them:

- **The handshake (`ConnectionOpened`) is sent first, before any payload** (`createConnectionOpener`,
  `createPeerSender`). It pre-checks the stream and, when authentication is off, carries the
  sender's `BftNodeId` in `sentBy` so the receiver can learn *who* it is.
- **The dialer learns the peer from the reverse-auth flow.** For an outgoing connection, "who is on
  the other end" is answered by step 6 — B authenticating itself to A — surfaced through the
  `AuthenticateServerClientInterceptor`'s `PromiseUnlessShutdown[SequencerId]`. The connect worker
  literally `await`s that promise; a failed or absent token (5s timeout) fails the worker, not just a
  later send.
- **Only after that promise resolves** does
  `completeConnectivitySetupAfterSuccessfulAuthentication` perform the where→who bind
  (`endpointId→nodeId`, `nodeId↔sender`); *that* is the moment the peer becomes addressable and
  `onNodeId` fires, which the out-module turns into `Network.Authenticated` and feeds into
  module-start gating.

### The outgoing channel state machine

`P2POutgoingConnectionStatus` (per endpoint, in `P2PGrpcConnectionManager`). Absent-from-map ≡
*Disconnected*. The `ConnectingOnChannel → ConnectedOnChannel` edge is precisely "authentication
completed" (step 6→7 above).

```
                attemptTransitionToConnecting
   (absent) ─────────────────────────────────► Connecting
      ▲                                            │ openGrpcChannel ok
      │                                            ▼
      │                            ConnectingOnChannel(channel, auth, worker?)
      │                              │  │  authentication completes (peer SequencerId verified)
      │             worker fails /   │  └──────────────────────────► ConnectedOnChannel(channel,auth)
      │             shutdown         │                                   │  disconnect / send-fail /
      └──────────────────────────────┴───────────────────────────────────┘  remote completion
                                       (channel shut down, entry removed)
                  disconnect requested while a worker is mid-flight
   ConnectingOnChannel ───────────────────────────► DisconnectingFromChannel(channel,auth,worker)
                          (worker observes this, aborts cleanly, removes entry)
```

- A disconnect that arrives *while connecting/authenticating* doesn't yank the channel out from under
  the worker; it flips the entry to `DisconnectingFromChannel`, and the worker's next state check
  (`attemptTransitionToRetryConnecting` / `attemptConnectionOrDisconnectionCompletion`) sees it and
  shuts the channel down itself. Conversely, an `attemptTransitionToConnecting` arriving during
  `DisconnectingFromChannel` *cancels* the pending disconnect (back to `ConnectingOnChannel`).
- Every transition is a pure function on the immutable `State`, applied via
  `AtomicUtil.updateAndGetComputed` and logged through `ResultWithLogs` — so the log narrates the
  exact transition (`"$old -> $new"`) for every channel, which is the first thing to grep when
  debugging connectivity or a stuck handshake.

---

## 4. Accepting an incoming connection

The mirror image, and it reads authentication from the *other* source (§2.2). gRPC delivers a new
stream to `P2PGrpcBftOrderingService.receive` (`bindings/p2p/grpc/P2PGrpcBftOrderingService.scala`),
which calls `createServerSidePeerReceiver`:

```
remote peer (A)        P2PGrpcBftOrderingService        P2PGrpcConnectionManager
   │  receive(stream) ──────────────►                              │
   │  (server interceptors already ran: A's token verified,        │
   │   A's id in gRPC Context; A.ext endpoint in peerEndpointKey)   │   §2.2 client→server
   │                          createServerSidePeerReceiver(sendingObserver)
   │                                   │ peerSender = wrap(sendingObserver)
   │  ◄── ConnectionOpened{sentBy=me} ─┤ peerSender.onNext(opener)
   │                                   │ sequencerId promise:
   │                                   │   • auth on  → from gRPC Context
   │                                   │               (IdentityContextHelper.storedMemberContextKey,
   │                                   │                set by the standard server interceptor)
   │                                   │   • auth off → backfilled by receiver from first frame
   │                                   │ maybeEndpoint = peerEndpointContextKey  (A's AddEndpointHeader)
   │  ── messages ───────────────────► P2PGrpcStreamingReceiver.onNext
   │                                   │ on sequencerId: completeConnectivitySetupAfterSuccessfulAuthentication(seqId, sender, maybeEp)
```

For an incoming connection, "who is on the other end" was already answered by the standard
client→server flow **before** `receive` even runs: A's token was verified by
`authenticationServerInterceptor` and A's verified id placed in the gRPC `Context`, which
`extractSequencerIdFromGrpcContextInto` reads out. (Meanwhile this node, as server, also ran the
reverse flow in its response headers so that *A* could authenticate *it*.) Same destination
(`completeConnectivitySetupAfterSuccessfulAuthentication` → a verified peer `SequencerId`, the
where→who bind), different source than §3.

The crucial asymmetry: **an incoming connection may not tell you its endpoint.** With auth off (or a
misbehaving/edge-case peer) the client might omit, or send a different, `AddEndpointHeader`. Then
this node knows the peer's `nodeId` and has a usable `peerSender`, but has no `endpointId→nodeId`
binding — so if it *also* has a configured endpoint for that peer it may dial a **redundant reverse
connection**. That's expected and handled by deduplication (§5.1).

---

## 5. Managing connections

### 5.1 Deduplication and convergence — when both nodes dial each other

In a fresh N-node network every node dials every configured peer, so each pair tends to form **two**
connections (one per direction). They must collapse to **one** bidirectional stream, used both ways,
once identity is known (§2):

- `addSenderIfMissing(nodeId, sender)` is the referee. The **first** stream to authenticate for a
  given `nodeId` wins (`true` → bound, `onNodeId` fires). The **second** loses (`false`) and
  its `peerSender` is immediately completed/closed
  (`completeConnectivitySetupAfterSuccessfulAuthentication`), which closes that stream
  **end-to-end** — the counterparty's receiver sees `onCompleted` and tears its half down too.
- `getPeerSenderOrStartConnection` also short-circuits proactively: if a usable sender already
  exists (e.g. via an incoming connection), it `shutdownOutgoingConnectionIfNeeded(...,
  onlyIfNotFullyConnected = true)` to abandon any half-built outgoing duplicate.
- `consolidateNetworkRefs` (in `P2PGrpcConnectionState`) makes all of a node's endpoints point at a
  **single** `P2PNetworkRef`, preferring one already bound to the `nodeId` (typically the incoming
  connection), and returns the now-orphaned refs to `close()`. This is why a node may briefly hold
  two refs and then drop one — visible in the `BeforeAndAfter` state logs.

**Convergence is probabilistic, not deterministic.** The referee is purely local first-come; there
is **no global tiebreaker** (e.g. "the lower `SequencerId` keeps its outgoing connection"). For a
pair to collapse to one stream, both nodes must keep the *same* stream — e.g. both keep `X`, so B
reaches A over X's server→client direction. If a symmetric simultaneous dial has them pick different
winners (A keeps its outgoing `X`, B keeps its outgoing `Y`), each rejects the stream the *other* is
keeping; and because rejection *closes* the stream end-to-end (bullet 1), **both** `X` and `Y` are
torn down and both nodes re-dial. What breaks the symmetry is connection **jitter** (random initial
connect delay ≤500ms + jittered backoff, §5.3), which desynchronizes the next round so one stream
authenticates first and both sides agree. A split decision is therefore a brief oscillation, not a
deadlock — *provided the closes actually propagate* (when they may not, see §5.6).

### 5.2 Identity equivocation — the security boundary

Authentication established *who* a peer is; this map keeps that binding honest.
`State.associateP2PEndpointIdToBftNodeId` rejects the one impersonation attempt it currently
enforces, logging at WARN and marking a `security.noncompliant` metric (`emitIdentityEquivocation`):
`CannotAssociateP2PEndpointIdsToSelf` — a peer authenticates as *this* node. The error also fails the
connectivity-setup future, so the offending stream doesn't become usable.
Re-associating a known endpoint to a *different* authenticated node id is **not** currently rejected:
the `P2PEndpointIdAlreadyAssociated` error exists (and is handled by the connection manager), but the
branch that would produce it is commented out, so the association is just updated and logged at INFO.
Hardening this is tracked by #34191.

A separate check guards the data path: `P2PGrpcStreamingReceiver.validateNodeId` drops any frame
whose `sentBy` disagrees with the authenticated `SequencerId` (metric
`WrongGrpcMessageSentByBftNodeId`) and fails the stream. (Payload *signatures* are still verified
later, downstream, against the inner signed message's `from` field.)

### 5.3 Retries — two independent loops

Do not conflate these; they live in different layers and have different parameters. Note that
**authentication lives inside the connect-worker loop**, so an auth failure is retried with
connection backoff, not send backoff:

| | Connect-worker retry | Actor send retry |
|---|---|---|
| Where | `P2PGrpcConnectionManager.createPeerSender` | `PekkoP2PGrpcNetworking` connection-managing actor |
| Retries | establishing the stream + **authentication (§2)** | obtaining a sender for a queued `Send` / a failed `onNext` |
| Backoff | exponential: base `initialConnectionRetryDelay` (500ms) ×`connectionRetryDelayMultiplier` (2), capped at `maxConnectionRetryDelay` (2min), **jittered** to `[base/2, base)` | fixed `SendRetryDelay` = 2s |
| Limit | unbounded; **logs escalate INFO→WARN after `maxConnectionAttemptsBeforeWarning` (30)** | `MaxAttempts` = 5, then the message is dropped |
| Notable | initial attempt also jittered by `initialConnectionMaxDelay` (≤500ms) to avoid thundering-herd | **`Initialize` is retried forever** (modules block on quorum, so giving up could deadlock startup) |

A send that throws on `onNext` must, per gRPC, end the stream (`onError` is terminal): the actor
`failGrpcStreamObserver` + `shutdownConnection(clearNetworkRefAssociations=false)` to *invalidate but
not forget* the connection, then reschedules the same `SendMessage` — the next attempt transparently
rebuilds the connection, re-running the full authentication handshake (§2). Retry/backoff knobs are
all under `P2PConnectionManagementConfig`.

### 5.4 Disconnect and shutdown

- **Admin / topology-driven** (`P2PNetworkOut.Admin.RemoveEndpoint`, `Internal.EndpointRemoved`) →
  `shutdownConnection(endpointId, clearNetworkRefAssociations=true, closeNetworkRefs=true)`:
  removes associations, completes the sender, shuts the channel, fires `onDisconnect`. A subsequent
  send re-dials and re-authenticates from scratch.
- **Remote-initiated, outgoing** (`onError`/`onCompleted` on an outgoing connection's receiver) →
  `shutdownOutgoingConnectionDueToRemoteCompletion`: shuts the outgoing channel, completes the
  sender and notifies disconnection (`cleanupPeerSender`), but **keeps** the node↔endpoint
  association and the network ref, because this node manages that connection and its connect worker
  is expected to re-establish and re-authenticate it.
- **Remote-initiated, incoming** (`onError`/`onCompleted` on an incoming connection's receiver) →
  `shutdownIncomingConnectionDueToRemoteCompletion`: completes the sender and fully clears the
  active connection state (`shutdownAndCleanupActiveConnection` → `clearActiveConnectionState`),
  removing the network-ref associations and **closing** the network ref. Incoming connections aren't
  managed by this node, so there is nothing to re-establish at the transport level (the peer re-dials
  if needed), and a retained ref would wrongly re-assert the transient node↔endpoint association or
  keep a dead stream addressable. The endpoint→node-id mapping is also cleared.
- **Re-arming configured endpoints after any disconnection.** Because the cleanup above erases *all*
  runtime state for the endpoint, `P2PNetworkOut` re-asserts the invariant "every **configured**
  endpoint has a network ref" on every `Network.Disconnected`: it looks the endpoint up in its
  module-local cache of configured endpoints and, if it is still there, calls
  `ensureSendingEnabledTo` (§3, via `reconnectIfStillConfigured`) **synchronously**, i.e. on the
  module thread, while processing the disconnection. This
  is idempotent (`addNetworkRefIfMissing` is a no-op when a ref exists), so it only bites when the
  state was actually erased. Without it, a peer that *won* deduplication with an incoming connection
  and then dropped it (e.g. its operator removed this node's endpoint, so it will never re-dial)
  would leave this node permanently disconnected from a peer it is configured to connect to, with
  sends silently dropped by `networkSendIfKnown` and the endpoint absent from `Admin.GetStatus`,
  recoverable only by a restart or an admin remove/re-add.
  **The configured-endpoint lookup is load-bearing, not a mere policy check**, for two independent
  reasons: (i) a disconnection only carries a `P2PEndpoint.Id` = `(address, port, tls)`, whereas
  dialing a TLS peer also needs `endpointConfig.tlsConfig` (trust collection, client cert), which
  neither the ID nor the endpoint-ID-keyed connection state retains — rebuilding an endpoint from its
  ID would silently drop the operator's TLS client material; and (ii) disconnections are *also*
  notified for endpoints that are not configured here, so the lookup is what prevents both
  resurrecting an endpoint the operator just removed and dialing a node that merely connected *to* us
  (incoming connections advertise an endpoint for dedup, §2.3, and their teardown notifies a
  disconnection for it).
  **Why a cache and not a store read.** The cache is what makes the decision *synchronous*, and hence
  race-free against a concurrent `Admin.RemoveEndpoint`: reading `P2PEndpointsStore` completes
  asynchronously, so a removal processed in between could be undone by a reconnection based on a
  stale snapshot. It is safe to keep, because after `BftBlockOrderer`'s bootstrap `P2PNetworkOut` is
  the only writer of the endpoint *set* (`P2PGrpcConnectionManager` only calls `associate` on
  successful authentication, implemented as an `update … where` keyed on the endpoint id, so it can
  only set an existing row's `node_id` and never insert or remove). Coherence is achieved by mutating
  it exclusively on the module thread: it is seeded from the store when handling `Start`, and updated
  when handling `Internal.EndpointAdded` / `Internal.EndpointRemoved`, which the admin `pipeToSelf`
  continuations emit **unconditionally** (they also carry the connect/disconnect decision, which is
  likewise taken on the module thread against the runtime connection state).
  The stored `node_id` is deliberately *ignored* when (re)establishing outgoing connectivity, both on
  this path and in `connectInitialNodes`: connections are always ensured by **endpoint**, because the
  peer must re-authenticate (§2) before it is addressable by node id again. Asserting a stored
  association upfront would register the network ref under the node id, which
  `addNetworkRefIfMissing` treats as *incoming* (`isOutgoingConnection = false`), and would make the
  endpoint report `Authenticated` as soon as its gRPC channel is up but before any authentication —
  inflating `maxNodesContemporarilyAuthenticated` and potentially starting Availability/Consensus
  below their quorum of genuinely authenticated nodes. The persisted node id is for operator
  visibility (`Admin.ListConfiguredEndpoints`) only; durable identity pinning is tracked by #34191.
- **Manager close** drains every connection (`closeConnectionState`) and, per endpoint, waits for the
  connect worker to finish before shutting the channel (`closeAsync`), avoiding orphaned gRPC
  channels (channel shutdown blocks on a dedicated long-running executor). The server-side
  `ServerAuthenticatingServerInterceptor` is closed too (its token provider).

### 5.5 Reconnection identity note

`PekkoP2PGrpcNetworkManager.createNetworkRef` appends a `UUID` to each connection-managing actor's
name. Network-ref consolidation can stop and later recreate an actor for the *same* endpoint+node
(e.g. A→B configured but not B→A: B's ref to A is created, the link crashes, B cleans up, A
reconnects and re-authenticates) — the UUID keeps Pekko actor names unique across those cycles.

### 5.6 Liveness, failure detection, and the duplicate-rejection loop

The dedup and "shared fate per stream" guarantees in §5.1 assume a stream close
(`onCompleted`/`onError`) actually reaches the counterparty. It is gRPC trailers / `RST_STREAM` /
`GOAWAY` over TCP, with **no delivery guarantee and no application-level ACK** — under a network
partition, blackhole, NAT/firewall idle-eviction, or an abrupt peer-host crash (no FIN/RST at all),
the close may never arrive. Liveness therefore also leans on keepalive and send-failure. Keepalive is
now enabled on **both** ends, so the *mechanism* is symmetric; the *timing* is not, because each side
runs an independent timer measured from its own last read:

- **Dialer (gRPC client of the stream): keepalive ON.** P2P client channels use
  `ClientChannelParams.Default` → `KeepAliveClientConfig()`: PING after **40s** of read-inactivity,
  **15s** ACK timeout (`config/ServerConfig.scala`). `keepAliveWithoutCalls = false`, but the
  long-lived `Receive` RPC keeps a call active, so PINGs flow even on an idle-but-established stream
  → the dialer detects a dead/half-open link in **~55s without app traffic**; the 15s timeout also
  sets the socket `TCP_USER_TIMEOUT`, so unacked sends fail within ~15s.
- **Acceptor (gRPC server of the stream): keepalive ON too.**
  `P2PServerConfig.keepAliveServer = Some(BasicKeepAliveServerConfig())`: PING after **40s** of
  read-inactivity, **20s** ACK timeout, `permitKeepAliveTime = 20s`,
  `permitKeepAliveWithoutCalls = false` (`config/ServerConfig.scala`,
  `core/BftBlockOrdererConfig.scala`). The server's keepalive manager is active while the
  long-lived `Receive` call is in flight, so the acceptor detects a dead/half-open link in
  **~60s without app traffic**, instead of having to wait for its own outbound send to fail (a
  prompt TCP RST once a path heals, otherwise TCP-retransmit timescales — minutes) or for the
  peer's close to arrive. Note the compatibility constraint: the dialer's 40s PING interval is
  ≥ the acceptor's `permitKeepAliveTime` (20s), so client PINGs are never punished with an
  `ENHANCE_YOUR_CALM` / `too_many_pings` `GOAWAY`.

**The duplicate-rejection loop.** Combine a non-propagated close, the fact that each side runs its
own independent detection timer, and §5.1's referee, and a *transient* version of the asymmetric "one
side up, the other perpetually rejected" loop can still occur. Surviving link `X = A→B` (A dialer, B
acceptor); partition, then heal:

1. A's keepalive fails (~55s after A's last read on `X`) → A tears down `X` and re-dials; A's
   `GOAWAY` to B is lost (still partitioned).
2. B's own keepalive hasn't fired yet, so B keeps the **stale** `bftNodeIdToPeerSender[A]` from `X`.
3. Heal before B's deadline: A's re-dial `X'` authenticates at B → `addSenderIfMissing(A, …)` returns
   **false** (a sender for A already exists) → B **rejects `X'` as a duplicate** and closes it.
4. A's receiver sees the close → tears down → re-dials → rejected again. **Loop.**

It is **bounded by B's own keepalive deadline**, not by the nominal 5s difference between the two
timeouts. Each side measures from *its own* last read, and those instants can be far apart under
asymmetric application traffic: if B was reading from A shortly before the partition while A had been
idle, B's ~60s window starts much later than A's ~55s one, so B can hold the stale sender for close
to a **full ~60s after A has already timed out** (and the loop lasts for whatever part of that window
remains once the path heals). The bound is therefore "until B's ~60s deadline, measured from B's last
read on `X`", with two ways to end sooner: B's next send to A over the dead socket, which post-heal
draws a prompt TCP RST (A closed the socket), or the peer's close finally arriving. With server
keepalive on, a silent acceptor no longer drags toward TCP-retransmit timescales.

> **Caveats.** The keepalive values are from config; the loop *dynamics* are reasoned from
> gRPC/TCP semantics, not an observed test. And the deterministic
> simulator does **not** exercise this: `NetworkSimulator` replaces the entire gRPC transport, so
> real keepalive and close-propagation behavior live only in the gRPC binding and would surface only
> in integration/chaos testing (e.g. Toxiproxy partition-then-heal), not the sim.

---

## 6. Worked scenarios

**(a) 4-node genesis bootstrap.** Each node has the other three as `peerEndpoints`. All dial all →
up to 12 directed connections, each **mutually authenticated** (§2) and then deduped to 6 links
(§5.1). Each successful authentication raises the out-module's `maxNodesContemporarilyAuthenticated`;
`startModulesIfNeeded` starts Availability at weak quorum (`AvailabilityModule.quorum(4)` = 2 others)
and Consensus at strong quorum (`strongQuorumSize(4)` = 3 others). The peak counter means a peer
flapping right at the threshold doesn't stall startup. (`P2PNetworkOutModule.startModulesIfNeeded`.)

**(b) Operator adds a peer at runtime.** `Admin.AddEndpoint(ep)` → persist to `P2PEndpointsStore`
→ on success `Internal.EndpointAdded(ep)` → cache the configured endpoint and, unless it is already
known at runtime, `ensureSendingEnabledTo(Endpoint(ep))` → §3 (dial + authenticate). Because the store is
insert-only for the endpoint set, a re-add of an already-configured endpoint ID does **not** overwrite
its TLS client material, and the cache mirrors that with `getOrElseUpdate`; dialing therefore uses the
value the cache *returns* (the persisted endpoint), never the rejected argument, so a duplicate
`AddEndpoint` carrying different TLS material can't open a connection with unpersisted configuration.
If the peer was already connected inbound, §5.1 collapses the new outgoing attempt.

**(c) Asymmetric config (A→B only).** A dials B and they authenticate mutually (A still advertises
its endpoint, so B can reverse-authenticate and reach A's auth service). B has no configured endpoint
for A, so it relies on the incoming connection; when B needs to send to A it finds A's sender bound
by node id (established when A authenticated) and uses it over the same bidi stream — no reverse dial
needed. If that link drops, A's connect-worker backoff re-establishes and re-authenticates it.

**(d) Impersonation attempt.** A forged or expired token is rejected by the standard or reverse auth
interceptor, failing the call with `UNAUTHENTICATED` before any id is bound (§2.4). If a peer at a
known endpoint authenticates as *this* node id, the binding stage catches it:
`CannotAssociateP2PEndpointIdsToSelf`, WARN + security metric, the association is not made and the
setup future fails (§5.2). A peer that authenticates at a known endpoint as a *different* node id
than previously recorded is, in contrast, currently **accepted**: the association is simply updated
and logged at INFO (`P2PEndpointIdAlreadyAssociated` exists and is handled, but the code that would
emit it is commented out; durable identity pinning is tracked by #34191).

**(e) Partition that heals (the duplicate-rejection loop).** A link `X = A→B` is silently
partitioned. A's keepalive tears `X` down ~55s after A's last read and A re-dials; B's independent
~60s timer, measured from *B's* last read, may still be far from expiring, so B keeps a stale sender
for A. If the path heals before B's deadline, A's reconnects are rejected by B as duplicates until
that deadline (or until B's next send to A draws a TCP RST) clears the stale sender — a transient
loop bounded by B's own keepalive deadline, which under asymmetric traffic can be nearly a full ~60s
after A timed out. Full mechanics and caveats in §5.6.

**(f) The peer wins, then unilaterally leaves.** A and B are both configured for each other. B dials
first and wins deduplication, so the surviving link is *incoming* at A; A's own outgoing attempt is
closed as a duplicate and its channel entry removed (§5.1). B's operator then runs
`RemoveEndpoint(A)`: B tears its outgoing connection down and will never re-dial. A's server-side
receiver sees the close and runs the *incoming* cleanup (§5.4), which erases the network ref, the
sender, and the `endpointId→nodeId` binding for B. At that point nothing at the transport layer is
left to retry, and `connectInitialNodes` only ever runs once, on `Start`. The re-arming step in
§5.4 is what closes the gap: A consults its module-local cache, still finds B's endpoint, and dials
again, so the pair reconverges on a single link owned by A. (Covered by `P2PNetworkOutModuleTest`, "an
endpoint gets disconnected" → "re-establish an outgoing connection to it".)

---

## 7. The framework seam (why all this is swappable)

`P2PNetworkIn`/`P2PNetworkOut` and the abstract `P2PConnectionState`
(`core/modules/p2p/P2PConnectionState.scala`) contain **no gRPC and no authentication** — they speak
only `P2PNetworkManager` / `P2PNetworkRef` / `P2PConnectionEventListener` / `P2PAddress` (all in
`framework/Module.scala`). The gRPC + token-auth stack in `bindings/p2p/grpc/` is one implementation
of that seam; the deterministic simulator (`src/test/.../framework/simulation/`) is another,
supplying its own manager/ref over `SimulationEnv` (and treating peers as pre-authenticated) so the
*identical* out/in module logic — including the gating, dedup, and equivocation handling above — runs
under modelled partitions and crashes (`NetworkSimulator`). Transport- and auth-level tests live
under `src/test/.../bindings/p2p/grpc/`; module logic under `src/test/.../core/modules/p2p/`.

---

## 8. Reference: modules, wire protocol, config

### P2PNetworkOut — `core/modules/p2p/P2PNetworkOutModule.scala` (trait `framework/modules/P2PNetworkOut.scala`)
The send side and connection orchestrator. Also, a `P2PConnectionEventListener` (its `onConnect` /
`onDisconnect` / `onNodeId` callbacks become `Network.Connected` / `Disconnected` /
`Authenticated` self-messages). Message ADT: `Start`, `Internal.{EndpointAdded,EndpointRemoved}`,
`Network.{Connected,Disconnected,Authenticated,TopologyUpdate}`, `Admin.{AddEndpoint,RemoveEndpoint,
ListConfiguredEndpoints,GetStatus}`, `Multicast(message, destinationBftNodeIds)`,
`SendToRandomAuthenticated(…)`, `EndWorkflow(workflowId)`. Responsibilities: send (drop if no ref for
the node; iterate recipients **sorted** for determinism; loop self-sends back into `P2PNetworkIn`),
**module-start gating** (boots Mempool/Output/Pruning immediately, Availability at weak quorum,
Consensus at strong quorum), and pushing `Mempool.P2PConnectivityUpdate` so the mempool knows it has
write-readiness.

### P2PNetworkIn — `core/modules/p2p/P2PNetworkInModule.scala` (trait `framework/modules/P2PNetworkIn.scala`)
The receive side. Its message type *is* the raw wire `BftOrderingMessage`. Switches on the body
`oneof`, parses with the owning module's parser, forwards to `availability` / `consensus` (the
latter wrapped "Unverified" — signature checks happen downstream). Parse failures are dropped with a
WARN, never thrown.

### Wire protocol
`community/synchronizer/src/main/protobuf/.../bftordering/v30/bft_ordering_service.proto`:
`service BftOrderingService { rpc Receive(stream BftOrderingMessage) returns (stream BftOrderingMessage); }`
— one **bidirectional** stream carries both directions for a pair. Envelope `BftOrderingMessage {
trace_context; body; sent_by /*BftNodeId*/; sent_at /*metrics*/ }`; body `oneof` ∈ {availability,
consensus, state_transfer (each a `SignedMessage`), retransmission, `ConnectionOpened` (handshake)}.
P2P moves the `SignedMessage`s opaquely; modules verify them. (Token auth rides in gRPC *headers*,
not in these messages.)

### Endpoint persistence — `core/modules/p2p/data/`
`P2PEndpointsStore[E]` (`listEndpoints`/`addEndpoint`/`removeEndpoint`/`clearAllEndpoints`), with
`InMemory…` and `Db…` (Postgres/H2) impls chosen by `Storage` type. Read (blocking) on `Start`;
written by admin add/remove. `overwriteStoredEndpoints` decides whether config `peerEndpoints`
replace the stored set at startup.

### Configuration — `core/BftBlockOrdererConfig.scala`, `P2PNetworkConfig`
- `serverEndpoint: P2PServerConfig` — internal bind `address`/`internalPort`; **external**
  `externalAddress`/`externalPort` that peers dial and reverse-authenticate against (may be a
  TLS-terminating proxy); `externalTlsConfig` (client TLS used when calling a peer back to
  authenticate); `tls` (server TLS); `maxInboundMessageSize`; `keepAliveServer`, defaulting to
  `BasicKeepAliveServerConfig()` (40s PING / 20s timeout, `permitKeepAliveTime` 20s), which is what
  gives the acceptor side its own failure detection (§5.6).
- `endpointAuthentication: P2PNetworkAuthenticationConfig` — `enabled` (keep true outside tests) +
  `authToken` manager config (drives `AuthenticationInitialState.authTokenConfig`).
- `connectionManagementConfig: P2PConnectionManagementConfig` — the connect-worker retry knobs from
  §5.3 (`maxConnectionAttemptsBeforeWarning`=30, `initialConnectionRetryDelay`=500ms,
  `maxConnectionRetryDelay`=2min, `connectionRetryDelayMultiplier`=2, `initialConnectionMaxDelay`).
- `peerEndpoints: Seq[P2PEndpointConfig]`, `overwriteStoredEndpoints: Boolean`.

### Observability
- **Admin**: `Admin.GetStatus` → `SequencerBftAdminData.PeerNetworkStatus` (per-endpoint health:
  `Authenticated(sequencerId)` / `Unauthenticated` / `Disconnected` / `UnknownEndpoint`, plus
  incoming-only entries), surfaced via the sequencer BFT admin API.
- **Metrics** (`core/modules/p2p/P2PMetrics.scala` → `BftOrderingMetrics.p2p`): connected &
  authenticated gauges; send/receive bytes & messages (labelled by peer and target/source module);
  `sendsRetried`; network-write & gRPC latencies; and the `security.noncompliant` identity-
  equivocation / wrong-`sentBy` counters.

### Standalone mode
`bindings/p2p/grpc/standalone/P2PGrpcStandaloneBftOrderingService.scala` exposes a different service
(`Send` unary + `ReadOrdered` server-stream) to drive the orderer in isolation (benchmarks) without a
full Canton sequencer; the peer `Receive` service is unaffected and **authentication is not supported
in standalone mode**. Topology/keys come from `bindings/standalone/`.
