use std::collections::HashSet;
use std::net::IpAddr;

use crate::selector::HostDef;

struct EndpointSpec {
    name: &'static str,
    /// (accepted client domain, upstream Host to send to the CDN)
    domain_map: &'static [(&'static str, &'static str)],
    ip_list: &'static str,
    upstream_port: u16,
    test_url: &'static str,
}

const ENDPOINTS: &[EndpointSpec] = &[
    EndpointSpec {
        name: "xbox-assets",
        domain_map: &[
            ("assets1.xboxlive.com", "assets1.xboxlive.cn"),
            ("assets2.xboxlive.com", "assets2.xboxlive.cn"),
            ("d1.xboxlive.com", "assets1.xboxlive.cn"),
            ("d2.xboxlive.com", "assets2.xboxlive.cn"),
            ("xvcf1.xboxlive.com", "assets1.xboxlive.cn"),
            ("xvcf2.xboxlive.com", "assets2.xboxlive.cn"),
            ("assets1.xboxlive.cn", "assets1.xboxlive.cn"),
            ("assets2.xboxlive.cn", "assets2.xboxlive.cn"),
            ("d1.xboxlive.cn", "assets1.xboxlive.cn"),
            ("d2.xboxlive.cn", "assets2.xboxlive.cn"),
        ],
        ip_list: include_str!("../data/ip/IP.XboxCn1.txt"),
        upstream_port: 80,
        test_url: "http://assets1.xboxlive.cn/Z/routing/extraextralarge.txt",
    },
    EndpointSpec {
        name: "xbox-content",
        domain_map: &[
            ("dlassets.xboxlive.com", "dlassets.xboxlive.cn"),
            ("dlassets2.xboxlive.com", "dlassets2.xboxlive.cn"),
            ("dlassets.xboxlive.cn", "dlassets.xboxlive.cn"),
            ("dlassets2.xboxlive.cn", "dlassets2.xboxlive.cn"),
        ],
        ip_list: include_str!("../data/ip/IP.XboxCn2.txt"),
        upstream_port: 80,
        test_url: "http://dlassets.xboxlive.cn/public/content/1b5a4a08-06f0-49d6-b25f-d7322c11f3c8/372e2966-b158-4488-8bc8-15ef23db1379/1.5.0.1018.88cd7a5d-f56a-40c7-afd8-85cd4940b891/ACUEU771E1BF7_1.5.0.1018_x64__b6krnev7r9sf8",
    },
];

pub fn load() -> anyhow::Result<Vec<HostDef>> {
    ENDPOINTS
        .iter()
        .map(|spec| {
            let mut ips = Vec::new();
            for line in spec.ip_list.lines() {
                let token = line.split(['\t', ' ', ';']).next().unwrap_or("");
                if let Ok(ip) = token.parse::<IpAddr>() {
                    ips.push(ip);
                }
            }
            let mut seen = HashSet::new();
            ips.retain(|ip| seen.insert(*ip));
            if ips.is_empty() {
                anyhow::bail!("endpoint {:?} has no IP candidates", spec.name);
            }
            Ok(HostDef {
                name: spec.name.to_string(),
                domain_map: spec
                    .domain_map
                    .iter()
                    .map(|(client, upstream)| (client.to_string(), upstream.to_string()))
                    .collect(),
                ips,
                upstream_port: spec.upstream_port,
                test_url: spec.test_url.to_string(),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loads_hardcoded_xbox_endpoint_groups() {
        let endpoints = load().unwrap();
        assert_eq!(endpoints.len(), 2);
        assert_eq!(
            endpoints[0].test_url,
            "http://assets1.xboxlive.cn/Z/routing/extraextralarge.txt"
        );
        assert!(endpoints[1]
            .test_url
            .starts_with("http://dlassets.xboxlive.cn/"));
        assert!(endpoints.iter().all(|endpoint| !endpoint.ips.is_empty()));
        assert!(endpoints[0]
            .domain_map
            .iter()
            .any(|(c, u)| c == "assets1.xboxlive.com" && u == "assets1.xboxlive.cn"));
    }
}
