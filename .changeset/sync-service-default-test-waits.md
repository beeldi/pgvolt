---
"@core/sync-service": patch
---

Tests that wait for a message, a task or a polled condition now use the suite's default receive timeout instead of 100ms to 2s budgets or fixed sleeps that CI load overran.
