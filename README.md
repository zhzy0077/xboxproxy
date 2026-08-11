# xboxproxy

A Rust reverse proxy for Xbox downloads. The service listens on port 80 and
uses the request `Host` header to select a bundled Xbox CDN endpoint and its
fastest candidate IP. The management dashboard is served by the same listener
under `/manage`.

## Built-in Endpoints

Endpoint groups, CDN domains, candidate IP lists, ports, and speed-test URLs
are hardcoded in `src/endpoints.rs`. The bundled IP lists are compiled into
the binary, so no TOML configuration file is required.

The endpoint groups are:

- Xbox download: `assets1`, `assets2`, `d1`, and `d2` under `xboxlive.com`
- Xbox content: `dlassets` and `dlassets2` under `xboxlive.com`

CN1 and CN2 intentionally use different speed-test URLs, matching the test
files in the reference XboxDownload project.

Only these six hosts are accepted. Other hosts receive `404`; the service does
not perform passthrough DNS resolution.

## Build and Run

```sh
cargo build --release
sudo ./target/release/xboxproxy
```

The process requires permission to bind port 80. The dashboard is available
at `http://<host>/manage`.

With Docker:

```sh
docker compose up -d --build
```

## API

| Endpoint | Description |
| --- | --- |
| `/manage` | Dashboard HTML |
| `/manage/api/summary` | Recent summary, charts, hosts, and records |
| `/manage/api/requests?limit=100&since=<unix>` | Recent request metrics |
| `/manage/api/speedtests?limit=100` | Recent speed-test results |
| `/manage/api/hosts` | Current candidate ranking |

## Tests

```sh
cargo test
cargo clippy --all-targets
cargo fmt --check
```

The database is stored at `data/xboxproxy.db` with seven-day retention.
