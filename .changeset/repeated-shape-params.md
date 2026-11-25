---
'@core/sync-service': patch
---

`GET`, `POST` and `DELETE /v1/shape` now refuse with `400` a query string that gives `table`, `where`, a `params[n]` key, `columns`, `replica`, `log`, `queryable_columns`, `locale`, `localized_columns` or a subset parameter more than once, in two forms (`params` with `params[1]`, `subset__where` with `subset[order_by]`), or under another spelling such as `TABLE`, `queryable.columns` or `w%68ere`. Electric used to keep one of the values and ignore the other spellings.
