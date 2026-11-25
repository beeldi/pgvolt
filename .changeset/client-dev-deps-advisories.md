---
'@electric-sql/client': patch
---

The client builds with tsup 9 and its tests use uuid 11. The built files keep the same code; the newer bundler only adds parentheses around two arrow functions and lists type exports as `export type`.
