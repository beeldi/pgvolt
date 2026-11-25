---
"@core/sync-service": minor
---

Serve a locale-keyed column at the language the reader asked for. A shape may
declare `locale` and `localized_columns`; the snapshot query then reads the
stored generated column Postgres fills for that language, and the live path
renames it onto the column the reader knows before dropping the others. The
locale joins the shape's comparable and its fan-out class only when the shape
declares one, so a shape asking for no language keeps the identity it had.
