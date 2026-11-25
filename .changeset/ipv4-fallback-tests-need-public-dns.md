---
'@core/sync-service': patch
---

Test only: the IPv4 fallback tests, which reach `local-ipv4-only.electric-sql.dev` through public DNS, are skipped when `PUBLIC_DNS_UNAVAILABLE` is set to `1` or `true`, such as in environments without public DNS.
