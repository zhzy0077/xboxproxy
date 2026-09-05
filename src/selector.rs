use std::cell::Cell;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::Serialize;
use tokio::sync::RwLock;

#[derive(Debug, Clone)]
pub struct HostDef {
    pub name: String,
    /// Accepted client domains paired with the upstream Host to send to the CDN.
    /// E.g. ("assets1.xboxlive.com", "assets1.xboxlive.cn") means a request
    /// arriving with Host: assets1.xboxlive.com is forwarded to the CDN with
    /// Host: assets1.xboxlive.cn.
    pub domain_map: Vec<(String, String)>,
    pub ips: Vec<IpAddr>,
    pub upstream_port: u16,
    pub test_url: String,
}

tokio::task_local! {
    pub(crate) static CURRENT_IP: Cell<Option<IpAddr>>;
}

pub fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[derive(Debug, Clone, Serialize)]
pub struct CandidateState {
    pub ip: IpAddr,
    pub latency_ms: Option<u64>,
    pub speed_kbps: Option<f64>,
    pub last_ok_ts: u64,
    pub last_fail_ts: u64,
    pub fails: u32,
    pub down: bool,
}

#[derive(Debug, Serialize)]
pub struct HostState {
    pub name: String,
    pub domains: Vec<String>,
    pub upstream_port: u16,
    pub candidates: Vec<CandidateState>,
    /// Dashboard/runtime pin; `None` means the bundled candidate list is used.
    pub pinned_ip: Option<IpAddr>,
}

#[derive(Clone)]
pub struct Selector {
    inner: Arc<SelectorInner>,
}

struct SelectorInner {
    hosts: Vec<HostStateInner>,
    failure_penalty: Duration,
}

struct HostStateInner {
    def: HostDef,
    bundled_ips: Vec<IpAddr>,
    pinned_ip: RwLock<Option<IpAddr>>,
    candidates: RwLock<Vec<CandidateState>>,
}

impl Selector {
    pub fn new(defs: Vec<HostDef>, failure_penalty: Duration) -> Self {
        let hosts: Vec<HostStateInner> = defs
            .into_iter()
            .map(|def| HostStateInner {
                bundled_ips: def.ips.clone(),
                candidates: RwLock::new(Self::fresh_candidates(&def.ips)),
                def,
                pinned_ip: RwLock::new(None),
            })
            .collect();
        Selector {
            inner: Arc::new(SelectorInner {
                hosts,
                failure_penalty,
            }),
        }
    }

    pub fn host_for(&self, host: &str) -> Option<&HostDef> {
        let host = normalize_host(host);
        for h in &self.inner.hosts {
            for (client_domain, _) in &h.def.domain_map {
                let domain = normalize_host(client_domain);
                if host == domain || host.ends_with(&format!(".{domain}")) {
                    return Some(&h.def);
                }
            }
        }
        None
    }

    /// Returns the upstream Host to use for a given client domain, or None if
    /// the domain is not managed.
    pub fn upstream_host_for(&self, host: &str) -> Option<&str> {
        let host = normalize_host(host);
        for h in &self.inner.hosts {
            for (client_domain, upstream_host) in &h.def.domain_map {
                let domain = normalize_host(client_domain);
                if host == domain || host.ends_with(&format!(".{domain}")) {
                    return Some(upstream_host);
                }
            }
        }
        None
    }

    fn index_of(&self, host: &str) -> Option<usize> {
        let host = normalize_host(host);
        for (i, h) in self.inner.hosts.iter().enumerate() {
            if h.def.name.eq_ignore_ascii_case(&host) {
                return Some(i);
            }
            for (client_domain, _) in &h.def.domain_map {
                let domain = normalize_host(client_domain);
                if host == domain || host.ends_with(&format!(".{domain}")) {
                    return Some(i);
                }
            }
        }
        None
    }

    pub async fn best_ip(&self, host: &str) -> Option<IpAddr> {
        let i = self.index_of(host)?;
        let inner = &self.inner.hosts[i];
        let cands = inner.candidates.read().await;
        if cands.is_empty() {
            return None;
        }
        let now = now_unix();
        let penalty = self.inner.failure_penalty.as_secs();
        let healthy: Vec<&CandidateState> = cands
            .iter()
            .filter(|c| now.saturating_sub(c.last_fail_ts) > penalty)
            .collect();
        let pool: Vec<&CandidateState> = if healthy.is_empty() {
            cands.iter().collect()
        } else {
            healthy
        };
        pool.iter()
            .max_by(|a, b| {
                score_at(a, now, penalty)
                    .partial_cmp(&score_at(b, now, penalty))
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|c| c.ip)
    }

    /// Upstream port for a rewritten host, if the host is managed.
    pub fn upstream_port(&self, host: &str) -> Option<u16> {
        self.index_of(host)
            .map(|i| self.inner.hosts[i].def.upstream_port)
    }

    pub async fn update_result(
        &self,
        host: &str,
        ip: IpAddr,
        latency_ms: Option<u64>,
        speed_kbps: Option<f64>,
        success: bool,
    ) {
        let Some(i) = self.index_of(host) else { return };
        let inner = &self.inner.hosts[i];
        let mut cands = inner.candidates.write().await;
        if let Some(c) = cands.iter_mut().find(|c| c.ip == ip) {
            let now = now_unix();
            if success {
                if latency_ms.is_some() {
                    c.latency_ms = latency_ms;
                }
                if speed_kbps.is_some() {
                    c.speed_kbps = speed_kbps;
                }
                c.last_ok_ts = now;
                c.fails = 0;
            } else {
                c.last_fail_ts = now;
                c.fails = c.fails.saturating_add(1);
            }
            c.down = now.saturating_sub(c.last_fail_ts) <= self.inner.failure_penalty.as_secs()
                && c.fails > 0;
        }
    }

    pub async fn mark_failure(&self, host: &str, ip: IpAddr) {
        self.update_result(host, ip, None, None, false).await;
    }

    fn fresh_candidates(ips: &[IpAddr]) -> Vec<CandidateState> {
        ips.iter()
            .map(|ip| CandidateState {
                ip: *ip,
                latency_ms: None,
                speed_kbps: None,
                last_ok_ts: 0,
                last_fail_ts: 0,
                fails: 0,
                down: false,
            })
            .collect()
    }

    /// Pin a group to one IPv4, or `None` to restore the bundled candidate list.
    pub async fn set_pinned_ip(&self, name: &str, ip: Option<IpAddr>) -> anyhow::Result<()> {
        let i = self
            .index_of(name)
            .ok_or_else(|| anyhow::anyhow!("unknown group {name:?}"))?;
        let inner = &self.inner.hosts[i];
        let ips = match ip {
            Some(ip) => vec![ip],
            None => inner.bundled_ips.clone(),
        };
        if ips.is_empty() {
            anyhow::bail!("no IPs to restore for {name}");
        }
        *inner.pinned_ip.write().await = ip;
        *inner.candidates.write().await = Self::fresh_candidates(&ips);
        Ok(())
    }

    /// Host definition with the live candidate IP list (after dashboard pins).
    pub async fn def_for_host(&self, host: &str) -> Option<HostDef> {
        let i = self.index_of(host)?;
        let h = &self.inner.hosts[i];
        let mut def = h.def.clone();
        def.ips = h.candidates.read().await.iter().map(|c| c.ip).collect();
        Some(def)
    }

    pub async fn resolve(&self, host: &str, port: u16) -> io::Result<SocketAddr> {
        if let Some(ip) = CURRENT_IP.try_with(|c| c.get()).unwrap_or(None) {
            return Ok(SocketAddr::new(ip, port));
        }
        if self.host_for(host).is_some() {
            if let Some(ip) = self.best_ip(host).await {
                let p = self
                    .inner
                    .hosts
                    .get(self.index_of(host).unwrap_or(usize::MAX))
                    .map(|h| h.def.upstream_port)
                    .unwrap_or(port);
                return Ok(SocketAddr::new(ip, p));
            }
        }
        Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("no managed endpoint candidate for {host}:{port}"),
        ))
    }

    pub async fn snapshot(&self) -> Vec<HostState> {
        let mut out = Vec::new();
        for h in &self.inner.hosts {
            let cands = h.candidates.read().await;
            let mut sorted = cands.clone();
            let now = now_unix();
            let penalty = self.inner.failure_penalty.as_secs();
            for candidate in &mut sorted {
                candidate.down =
                    candidate.down && now.saturating_sub(candidate.last_fail_ts) <= penalty;
            }
            sorted.sort_by(|a, b| {
                score_at(b, now, penalty)
                    .partial_cmp(&score_at(a, now, penalty))
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            out.push(HostState {
                name: h.def.name.clone(),
                domains: h
                    .def
                    .domain_map
                    .iter()
                    .map(|(client, _)| client.clone())
                    .collect(),
                upstream_port: h.def.upstream_port,
                candidates: sorted,
                pinned_ip: *h.pinned_ip.read().await,
            });
        }
        out
    }

    pub async fn defs(&self) -> Vec<HostDef> {
        let mut out = Vec::new();
        for h in &self.inner.hosts {
            let mut def = h.def.clone();
            def.ips = h.candidates.read().await.iter().map(|c| c.ip).collect();
            out.push(def);
        }
        out
    }
}

/// Composite score: download speed dominates, latency breaks ties.
fn score_at(c: &CandidateState, now: u64, penalty: u64) -> f64 {
    let speed = c.speed_kbps.unwrap_or(0.0);
    let latency = c.latency_ms.unwrap_or(0) as f64;
    if c.down && now.saturating_sub(c.last_fail_ts) <= penalty {
        return f64::MIN;
    }
    speed * 1000.0 - latency
}

fn normalize_host(host: &str) -> String {
    host.trim_end_matches('.').to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn def(ips: Vec<IpAddr>) -> HostDef {
        HostDef {
            name: "t".into(),
            domain_map: vec![("cdn.example.com".into(), "cdn.example.com".into())],
            ips,
            upstream_port: 80,
            test_url: "http://cdn.example.com/test".into(),
        }
    }

    #[tokio::test]
    async fn picks_fastest_by_speed() {
        let s = Selector::new(
            vec![def(vec![
                "1.1.1.1".parse().unwrap(),
                "2.2.2.2".parse().unwrap(),
            ])],
            Duration::from_secs(60),
        );
        s.update_result(
            "cdn.example.com",
            "1.1.1.1".parse().unwrap(),
            Some(50),
            Some(1000.0),
            true,
        )
        .await;
        s.update_result(
            "cdn.example.com",
            "2.2.2.2".parse().unwrap(),
            Some(10),
            Some(9000.0),
            true,
        )
        .await;
        assert_eq!(
            s.best_ip("cdn.example.com").await,
            Some("2.2.2.2".parse().unwrap())
        );
    }

    #[tokio::test]
    async fn down_candidate_is_skipped() {
        let s = Selector::new(
            vec![def(vec![
                "1.1.1.1".parse().unwrap(),
                "2.2.2.2".parse().unwrap(),
            ])],
            Duration::from_secs(3600),
        );
        s.update_result(
            "cdn.example.com",
            "1.1.1.1".parse().unwrap(),
            None,
            Some(10000.0),
            true,
        )
        .await;
        s.mark_failure("cdn.example.com", "1.1.1.1".parse().unwrap())
            .await;
        assert_eq!(
            s.best_ip("cdn.example.com").await,
            Some("2.2.2.2".parse().unwrap())
        );
    }

    #[test]
    fn host_suffix_match() {
        let s = Selector::new(
            vec![def(vec!["1.1.1.1".parse().unwrap()])],
            Duration::from_secs(60),
        );
        assert!(s.host_for("cdn.example.com").is_some());
        assert!(s.host_for("dl.cdn.example.com").is_some());
        assert!(s.host_for("cdn.example.com.evil.net").is_none());
        assert!(s.host_for("other.com").is_none());
    }

    #[tokio::test]
    async fn matching_is_case_insensitive_and_handles_trailing_dot() {
        let s = Selector::new(
            vec![HostDef {
                name: "t".into(),
                domain_map: vec![("CDN.Example.COM".into(), "cdn.example.com".into())],
                ips: vec!["1.1.1.1".parse().unwrap()],
                upstream_port: 80,
                test_url: "http://cdn.example.com/test".into(),
            }],
            Duration::from_secs(60),
        );
        assert!(s.host_for("DL.CDN.EXAMPLE.COM.").is_some());
        assert_eq!(
            s.best_ip("CDN.EXAMPLE.COM.").await,
            Some("1.1.1.1".parse().unwrap())
        );
    }

    #[tokio::test]
    async fn dashboard_pin_replaces_candidates_and_can_restore() {
        let s = Selector::new(
            vec![def(vec![
                "1.1.1.1".parse().unwrap(),
                "2.2.2.2".parse().unwrap(),
            ])],
            Duration::from_secs(60),
        );
        let pin: IpAddr = "9.9.9.9".parse().unwrap();
        s.set_pinned_ip("t", Some(pin)).await.unwrap();
        assert_eq!(s.best_ip("cdn.example.com").await, Some(pin));
        assert_eq!(s.defs().await[0].ips, vec![pin]);
        assert_eq!(s.snapshot().await[0].pinned_ip, Some(pin));

        s.set_pinned_ip("t", None).await.unwrap();
        let restored = s.defs().await;
        assert_eq!(restored[0].ips.len(), 2);
        assert!(s.snapshot().await[0].pinned_ip.is_none());
    }
}
