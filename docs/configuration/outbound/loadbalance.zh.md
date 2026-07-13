# 负载均衡

`loadbalance` 会把新连接分配到可用的出站。导入 Clash/Mihomo 配置时也接受
`load-balance` 别名。

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

`strategy` 支持 `round-robin` 和 `consistent-hashing`。一致性哈希使用有效顶级域名，
因此同一网站的新连接通常会使用同一个出站，不同网站则会分散到不同出站。
