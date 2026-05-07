# ledger-api

`ledger-api` is the HTTP transport adapter for Lens.

At this stage it exposes the generic `store` object registry only. It stays thin:
HTTP routing and Lens-facing DTOs live here, while object behavior belongs in
`store`.

## Routes

```text
GET    /health
GET    /store/objects?role=raw&kind=databento.dbn.zst&id_prefix=sha256-af
GET    /store/objects/:id
DELETE /store/objects/:id
```

`DELETE /store/objects/:id` removes the exact object from the local descriptor
registry, local object, R2 object, and R2 descriptor mirror when those locations
exist. It does not cascade into related objects.

## Development

```bash
cargo run -p ledger-api
```

The default bind address is `127.0.0.1:3001` and can be overridden with
`LEDGER_API_ADDR`.
