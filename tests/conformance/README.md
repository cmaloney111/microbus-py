# Conformance suite

Verifies microbus-py interoperates byte-correctly with a real fabric@v1.27.1
deployment. Boots a docker compose stack with `nats:2.10-alpine` plus a custom
Go sidecar and drives request/response, workflow, auth, signed access-token
JWKS verification, fragmentation, single-service and aggregate OpenAPI portal
docs, trace context, and TracedError scenarios across the language boundary.
It also verifies multi-hop baggage propagation across a Go -> Python -> Go
request chain and raw NATS ACK-before-response timing against the Go sidecar,
plus `llm.core` Chat delegation to a Python provider implementing the provider
`Turn` endpoint. Python connector spans are exported to an in-process
OTLP gRPC collector.

## Run

```bash
bash scripts/conformance.sh
```

Requires Docker Desktop running. The script builds the Go sidecar, brings the
stack up with `--wait`, polls `GET /health`, runs `pytest tests/conformance -m
conformance`, and tears down.

To keep the stack up for debugging:

```bash
KEEP=1 bash scripts/conformance.sh
```

To run against an externally managed stack, export `GO_SIDECAR_URL` and
`NATS_URL` before invoking pytest directly.

## Skipping

Tests are marked `@pytest.mark.conformance` and the fixture skips cleanly
when neither `GO_SIDECAR_URL` is set nor `docker compose` is available, so
`pytest tests/conformance` is safe in any environment.

## Goldens

Byte-equivalence fixtures live in `tests/fixtures/golden/` as paired
`*.json` (input descriptor) + `*.bin` (Go-emitted bytes via
`http.Request.WriteProxy`). Capture is one-shot — only re-run when fabric
upgrades change the encoded form.

```bash
cd docker/gosidecar
go run ./cmd/golden ../../tests/fixtures/golden
```

Commit the resulting binaries.

## Out of scope (deferred)

- JetStream durable subjects, forced access-token key rotation
