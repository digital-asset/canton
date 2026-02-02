Health Management
=================

Canton provides some tooling to monitor the health of internal components. The health information can be used internally
to take action when a component's health changes, and externally by users of the node when the aggregated health of the node changes.

## HealthComponent

`HealthComponent` is a trait providing base tooling to record the health of a component throughout its lifetime. To use it simply mix it into the class you want to
monitor the health of. You'll have to provide a name, an initial state, and make sure your class also extends `FlagCloseable`.
The state itself is of type `ComponentHealthState` which is sealed trait with 3 subtypes:

- `Ok` - The component is healthy
- `Degraded` - The component is not functioning in its normal state but can still perform its basic duty, albeit possibly sub-optimally (e.g: increased observed latency, lagging behind)
- `Failed` - The component cannot perform its basic duty and is to be considered non-functional

In a typical scenario one defines `initialHealthState` to set the initial state of the component. Then `degradationOccurred` (resp. `failureOccurred`) can be used to report
degradation (resp. failure) of the component. If/when the component recovers, `resolveUnhealthy` will reset the state of the component to Ok.
Use `degradationOccurred` if you want to merely make it visible that the component is not fully functional without affecting how its clients consume it.
Use `failureOccured` if you want to signal to its client that the component is now non-functioning and should not be used.

The current state can always be accessed via `getState`.

For cases where a custom state type is needed, use `BaseHealthComponent` which lets you override the  `State` type.

To receive updates when a component's health changes, use the `registerOnHealthChange` method and provide a `HealthListener` to it.

When a `HealthComponent` is closed, its health state will be changed to a shutdown `Failed` state automatically.

## HealthService

Already with using the `HealthComponent` trait above, we can manipulate the state of a component, access it within the code, and receive updates on health changes.
To go a step further and make use of the health states outside the node, we aggregate them into `HealthService`s.
A `HealthService` is just a collection of `HealthComponent`s, split into 2 categories:

- Critical dependencies: Components of the service that must all be in a **non-failed** state in order for the service to be considered functional
- Soft dependencies: Components who logically belong to that service but should not affect the overall health output of the service

`HealthService`s can be thought of as mappings to our APIs (Sequencer public API, Admin API, ledger API).
They also aggregate a set of components into a single binary value that can be used by external monitoring tools to react to health changes of a node,
as well as clients of the API to help with client-side load balancing decisions.

To be more precise, a `HealthService` serves a double purpose:

- It maps to the concept of "service" in the [gRPC Health Check Protocol](https://github.com/grpc/grpc/blob/master/doc/health-checking.md).
  Every node can be configured to expose a health endpoint following the gRPC service definition described in the link above. `HealthService`s gather components and aggregate their state to
  expose it through this health endpoint, as either `SERVING` or `NOT_SERVING`. This is where critical vs soft dependencies makes a difference. A service will report `SERVING` if and only if all of its
  critical components show a "non failed" state. Note that non failed is not the same as "ok" state. Namely, degraded components count as non failed.
  This is important because this health service can be used by clients of the nodes or external monitoring systems (e.g k8s probes) to determine whether the node is healthy.
  When adding a new `HealthComponent` to a `HealthService`, consider carefully how it will affect the overall node health.
- It is used as a container for all health components related to a node, which is made available to query via the `health.status` RPC of the admin API on every node.

You shouldn't generally need to add new `HealthService`s. They already exist for every node in their respective `CantonNodeBoostsrap` subtype.
All bootstrap instances have a `nodeHealthService` that is used for the configurable health endpoint as explained above. When adding new health components to a node, make sure to add them accordingly to the existing `nodeHealthService`.
Sequencer nodes additionally have a public API which also implements the gRPC Health Check Service, and therefore have an additional `HealthService` in their bootstrap class for that purpose.

It is important to note that `HealthService`s take instances of `HealthComponent`s as their dependencies, and that the dependency tree is only 1 level deep.
Meaning `HealthService`s have 1 layer of `HealthComponent` dependencies, and `HealthComponent`s do not depend on each other. This keeps the health relationships relatively simple to reason about at the cost of some flexibility.
It also means that the actual health component object instance must be passed through up to the bootstrap class of the node to be added to its `HealthService`.
It is common for `ComponentHealth` objects to be instantiated later than at bootstrap time, and/or be re-instantiated during the lifetime of a node (especially for active/passive nodes). To account for that, there's a `MutableHealthComponent` class
that can be added as a dependency to the `HealthService` at bootstrap time, and later on be filled with its delegate `ComponentHealth` when available. This mechanism allows enough flexibility to wire `ComponentHealth` objects all the way up to the bootstrap class.
See the `SequencerClient` or the `CantonSyncService` for examples.
