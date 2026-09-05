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

## Pinning CDN IPs

To skip speed testing and always use a known-good address, set one IPv4 per
group:

```sh
XBOXPROXY_PIN_CN1=112.64.213.194 XBOXPROXY_PIN_CN2=218.98.44.41 ./target/release/xboxproxy
```

`XBOXPROXY_PIN_CN1` pins `xbox-assets` (assets/d1/d2). `XBOXPROXY_PIN_CN2` pins
`xbox-content` (dlassets). When either is set, speed testing is fully disabled:
no startup warmup, no hourly pass, and the dashboard "Run now" button is
rejected.

To keep pinning off but skip the hourly/warmup loop, set
`XBOXPROXY_AUTO_SPEEDTEST=false`. Manual tests from `/manage` still run.

The dashboard can also pin one IPv4 per group without restarting. Empty + Set
restores the bundled candidate list.

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

## Home Network Setup (AdGuard Home + Caddy)

xboxproxy only helps if Xbox download traffic actually reaches it. The typical
home setup hijacks the CDN hostnames on the LAN with a DNS server and terminates
them with Caddy in front of xboxproxy:

```text
Xbox/console  →  AdGuard Home (DNS rewrite → proxy host)  →  Caddy :80  →  xboxproxy :80
```

1. Point the LAN router/DHCP at AdGuard Home (or any DNS server that supports
   rewrites, e.g. dnsmasq or Pi-hole) so every client on the subnet resolves
   through it.

2. Add a DNS rewrite for **every** CDN hostname to the proxy host IP
   (example: `192.168.31.246`):

   ```text
   assets1.xboxlive.com    → 192.168.31.246
   assets2.xboxlive.com    → 192.168.31.246
   d1.xboxlive.com         → 192.168.31.246
   d2.xboxlive.com         → 192.168.31.246
   xvcf1.xboxlive.com      → 192.168.31.246
   xvcf2.xboxlive.com      → 192.168.31.246
   dlassets.xboxlive.com   → 192.168.31.246
   dlassets2.xboxlive.com  → 192.168.31.246
   assets1.xboxlive.cn     → 192.168.31.246
   assets2.xboxlive.cn     → 192.168.31.246
   d1.xboxlive.cn          → 192.168.31.246
   d2.xboxlive.cn          → 192.168.31.246
   dlassets.xboxlive.cn    → 192.168.31.246
   dlassets2.xboxlive.cn   → 192.168.31.246
   ```

   The `.com` entries cover the hostnames the console actually uses; the `.cn`
   entries cover the upstream hostnames so that direct `.cn` lookups don't
   bypass the proxy and hit the real Microsoft edge.

3. Run xboxproxy behind Caddy. If another service already owns host port 80
   (Caddy itself, for example), publish xboxproxy on a different port
   (`8056:80`) and reverse-proxy the hijacked hostnames in the Caddyfile:

   ```caddyfile
   http://assets1.xboxlive.com, http://assets2.xboxlive.com,
   http://d1.xboxlive.com, http://d2.xboxlive.com,
   http://xvcf1.xboxlive.com, http://xvcf2.xboxlive.com,
   http://dlassets.xboxlive.com, http://dlassets2.xboxlive.com,
   http://assets1.xboxlive.cn, http://assets2.xboxlive.cn,
   http://d1.xboxlive.cn, http://d2.xboxlive.cn,
   http://dlassets.xboxlive.cn, http://dlassets2.xboxlive.cn {
       reverse_proxy xboxproxy:80
   }
   ```

   Caddy forwards the original `Host` header by default, which is what the
   proxy uses to select the endpoint and its upstream `.cn` host. If port 80
   is free, `docker compose up -d` (`80:80`) works without Caddy.

4. Verify from a LAN client:

   ```sh
   dig +short assets1.xboxlive.com   # → 192.168.31.246
   dig +short assets1.xboxlive.cn    # → 192.168.31.246
   curl -sv http://assets1.xboxlive.com/ 2>&1 | grep '< HTTP'
   ```

   Any HTTP response (even a 400 XML error from the Microsoft edge for the
   bare `/` path) means the request was proxied; a connection error or timeout
   means the DNS rewrite or Caddy routing is wrong.

## Speed Testing Policy

Speed tests can saturate the uplink (64 concurrent 10 MiB downloads), so they
are deliberately conservative. They do not run at all when `XBOXPROXY_PIN_CN1`
or `XBOXPROXY_PIN_CN2` is set. Set `XBOXPROXY_AUTO_SPEEDTEST=false` to disable
only the scheduled warmup/hourly passes.

The dashboard URL field runs a pass against that file and updates **only** the
matching group: an `assets1` / `assets2` / `d1` / `d2` URL updates xbox-assets;
a `dlassets` / `dlassets2` URL updates xbox-content.

- A pass runs at most once per hour (plus one warm-up pass at startup).
- While the proxy is serving client traffic (or served a request within the
  last 60 seconds), a pass is postponed; if a download starts mid-pass, the
  remaining tests are aborted. Active Xbox downloads are never starved, and
  measurements are not skewed by concurrent transfers.
- The dashboard (`/manage`, "Run now" button) can trigger a pass manually. It
  is a forced task: it runs immediately even while a download is in progress,
  and is only skipped if a pass is already running.

## API

| Endpoint | Description |
| --- | --- |
| `/manage` | Dashboard HTML |
| `/manage/api/summary` | Recent summary, charts, hosts, and records |
| `/manage/api/requests?limit=100&since=<unix>` | Recent request metrics |
| `/manage/api/speedtests?limit=100` | Recent speed-test results |
| `/manage/api/speedtests/run` (POST `{ "url": "..." }`) | Trigger a speed-test pass; `url` limits it to one group |
| `/manage/api/hosts` | Current candidate ranking |
| `/manage/api/groups/pin` (POST `{ "name", "ip" }`) | Pin a group to one IPv4; omit `ip` to restore bundled candidates |

## Tests

```sh
cargo test
cargo clippy --all-targets
cargo fmt --check
```

The database is stored at `data/xboxproxy.db` with seven-day retention.
