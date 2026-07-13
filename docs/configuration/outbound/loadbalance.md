# Load Balance

`loadbalance` distributes new connections across healthy outbounds. The
`load-balance` alias is also accepted for Clash/Mihomo-compatible imports.

```json
{
  "type": "loadbalance",
  "tag": "proxy-pool",
  "strategy": "consistent-hashing",
  "outbounds": ["proxy-a", "proxy-b", "proxy-c"],
  "url": "https://www.gstatic.com/generate_204",
  "interval": "5m"
}
```

`strategy` can be `round-robin` or `consistent-hashing`. Consistent hashing
uses the effective top-level domain, so connections to the same site normally
stay on the same outbound while different sites are distributed. URL testing
uses the same health history as `urltest`; if no health result exists yet, all
listed outbounds are eligible.
