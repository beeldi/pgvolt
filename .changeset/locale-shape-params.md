---
"@core/sync-service": patch
---

Read `locale` and `localized_columns` from the shape request. The shape already
projected a locale-keyed column when built with them, but the HTTP parameters
never declared either field, so the request dropped both and every reader got the
stored object. `localized_columns` follows the identifier rules of `columns`: a
camelCase name has to be quoted, or it folds to lowercase and stays unprojected.
