# Dark Factory

Dark Factory is an open-source toolkit for running Linear-backed autonomous OpenClaw workflows through a portable, fail-closed controller.

The intended release combines:

- `factoryd` and `factoryctl` for scheduling, state, leases, retries, and recovery
- Linear and OpenClaw adapters
- workflow policies and agent-facing operating skills
- immutable review artifacts and deterministic completion gates

The controller—not prompts—will enforce safety and durability properties such as fencing, idempotency, bounded retries, deterministic issue advancement, and fail-closed completion.

## Status

Architecture and scope definition. No production-ready controller has been released yet.

## License

MIT
