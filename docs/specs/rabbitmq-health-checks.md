# RabbitMQ child-resource health check design

This document captures the design intent and key decisions for how health checks work across
`Aspire.Hosting.RabbitMQ` child resources. It is aimed at contributors extending the integration.

For the user-facing contract see the [README](../../src/Aspire.Hosting.RabbitMQ/README.md#health-checks).

## Guiding principle

A child resource is `Healthy` iff it has been provisioned **exactly as declared** in the AppHost,
including every cross-cutting configuration that affects its runtime behaviour, and a live probe
confirms it still exists on the broker.

"Exactly as declared" is the key phrase. A queue with a TTL policy that failed to apply is not
what the user declared — it must be `Unhealthy` so that `WaitFor(queue)` blocks dependents.

## Design decisions

### Self-scheduling via `OnInitializeResource`

Every RabbitMQ child resource uses the platform-standard `OnInitializeResource` event to manage
its own lifecycle. There is no central provisioner. Each resource:

1. Starts in `NotStarted` (set via `WithInitialState`).
2. Transitions to `Waiting` while blocking on hard dependencies.
3. Transitions to `Starting` immediately before its broker call.
4. Transitions to `Running` on success, or `FailedToStart` on failure.

The `WithRabbitMQProvisioning` internal helper encapsulates steps 1–4 so that each builder
extension only provides the broker-specific delegate. No resource type needs to repeat the
state-transition boilerplate.

### Two signals per resource: lifecycle and health

Every RabbitMQ child resource participates in two independent signaling channels:

**Lifecycle signal** (Aspire resource state — `Waiting` / `Starting` / `Running` / `FailedToStart`):
Reflects whether the provisioning attempt has been made and what its outcome was.
The resource transitions to `Running` once the broker call completes successfully.
If the broker call fails, the resource transitions to `FailedToStart`.
Resources waiting for a dependency remain in `Waiting`.
Resources whose dependency never became ready remain in `NotStarted`.

**Health signal** (health check result):
The health check is the sole gate for `WaitFor` dependents. It returns `Unhealthy` until the
resource is fully provisioned (broker call succeeded, bindings applied, live probe passes).
There is no separate `ProvisionedTask` — the health check IS the provisioning signal.

These two channels are intentionally separate: lifecycle state is visible in the Aspire dashboard
resource list; health state is visible in the health check panel and gates `WaitFor` dependents.

### Lifecycle and health state matrix

| Situation | Lifecycle state | Health check result |
|---|---|---|
| `OnInitializeResource` not yet fired | `NotStarted` | `Unhealthy` |
| Waiting for hard dependency | `Waiting` | `Unhealthy` |
| Broker call in progress | `Starting` | `Unhealthy` |
| Provisioning succeeded | `Running` | `Healthy` after live probe |
| Provisioning failed | `FailedToStart` | `Unhealthy` |
| Exchange declared; bindings in progress | `Running` | `Unhealthy` |
| Exchange declared; bindings failed | `Running` | `Unhealthy` |
| Vhost failed; children never reached | `NotStarted` | `Unhealthy` |

### Dependency wait types

Resources wait for their dependencies inside `OnInitializeResource` using
`ResourceNotificationService.WaitForResourceAsync` and `WaitForResourceHealthyAsync`.
The wait type differs by dependency kind:

| Dependency | Wait type | Reason |
|---|---|---|
| Vhost (for queues, exchanges, policies, shovels) | `Healthy` | Proves `CanConnectAsync` — vhost is reachable, not just declared |
| Binding target (for exchange binding subscribers) | `Running` | Entity exists on broker; avoids circular health dependency chains |
| Shovel source / destination | `Running` | Entity exists on broker |

### Exchange is a special case: two-phase provisioning

Exchanges are declared in their `OnInitializeResource` handler (phase 1) and have their bindings
applied by separate concurrent `OnInitializeResource` subscribers registered at `AddBinding` call
time (phase 2).

The exchange transitions to `Running` after successful declaration (it is live on the broker at
that point). Each binding subscriber self-gates on the exchange reaching `Running` before applying
its binding. The health check returns `Unhealthy` until all expected bindings are applied.

If declaration itself fails, the exchange transitions to `FailedToStart`. Binding subscribers
waiting on `WaitForResourceAsync(exchange.Name, Running)` will never unblock — they are cancelled
when the application shuts down or the cancellation token is signalled.

If a binding fails, the exchange stays `Running` but the health check reports `Unhealthy` because
`AnyBindingFailed` is true.

### Exchange binding tracking (replaces `ProvisionedTask`)

`RabbitMQExchangeResource` tracks binding completion with simple counters rather than a
`TaskCompletionSource`:

- `_pendingBindings`: set at build time to `Bindings.Count`
- `_appliedBindings`: incremented by each successful binding subscriber
- `_failedBindings`: incremented by each failed binding subscriber

The health check reads:
- `AllBindingsApplied` (`_appliedBindings + _failedBindings >= _pendingBindings`) — gate before probe
- `AnyBindingFailed` — returns `Unhealthy` if true

### Cross-exchange binding — no deadlock

When Exchange A binds to B and B binds to A, both declare concurrently and reach `Running`
independently. Each binding subscriber then waits for its target to be `Running` (already
satisfied) and applies its binding. No circular wait exists because binding subscribers wait for
`Running` (declaration complete), not `Healthy` (bindings complete).

### Two-stage health check: provisioning state + live probe

The provisioning state (binding counters, `FailedToStart` check) proves "we sent the declare and
the broker accepted it, and all bindings were applied." The live probe proves "the entity still
exists" — catching out-of-band deletion by an operator. Both stages are required for correctness.

### Resource owns its own health semantics

Each resource type knows how to verify itself (existence check, connection check, state check).
This keeps health-check registration in the builder extensions trivial and uniform — every `Add*`
call site uses the same one-liner helper with no per-resource parameters.

### Probe result type is separate from `HealthCheckResult`

Resource classes return a lightweight domain type rather than `HealthCheckResult` directly. This
keeps `Microsoft.Extensions.Diagnostics.HealthChecks` out of the resource model layer, which is
important for testability and layering.

### Binding failures are attributed to the source exchange only

Bindings are declared on the exchange; routing is the exchange's responsibility. The destination
queue's own behaviour is unaffected by a missing binding. Propagating to the destination would
fan-in failures from many exchanges onto one queue and obscure the root cause.

### Shovel failures are isolated to the shovel resource

Shovels move messages between otherwise-independent endpoints. If a shovel fails, the source queue
still exists and is correctly configured. The shovel's live-state probe naturally catches downstream
breakage without needing to cascade to source or destination.

### Policy failures cascade to matching queues/exchanges

Unlike bindings, a policy changes the behaviour of the entity itself (TTL, max-length, DLX, HA).
A queue without its declared TTL policy will silently retain messages forever — a correctness bug
the user cannot observe from "queue exists = true". Therefore a policy failure marks every
queue/exchange whose name matches the policy pattern as `Unhealthy`.

Policy-to-entity matching is resolved once after the model is fully built (not at `AddPolicy` call
time, to avoid order-dependency) and cached on each entity via `AppliedPolicies`. The same
resolution pass adds a dashboard relationship edge so the cascade is visible without reading logs.
Queues and exchanges do **not** wait for their policies before starting — they declare themselves
independently and remain `Running-but-Unhealthy` if a policy fails.

## Extension guidance

When adding a new provisionable resource type:

- Inherit from `RabbitMQProvisionableResource`.
- In the builder extension, call `WithRabbitMQProvisioning(server, dependencies, provisionAsync)`:
  - `dependencies`: list of `(IResource, WaitType)` pairs — vhost with `WaitUntilHealthy`,
    other resources with `WaitUntilStarted` as appropriate.
  - `provisionAsync`: performs the broker call only. State transitions are handled by the helper.
- Implement `ProbeAsync` for the live existence/state check appropriate to the entity type.
- Override `HealthDependencies` if the resource's correctness depends on other provisionables
  (e.g. policies applied to it).
- Register the health check using the shared helper — no bespoke registration logic.
- No changes to any existing file are required. The new resource type is fully self-contained.
