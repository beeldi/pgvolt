---
'@core/sync-service': patch
---

A subquery now treats a float `-0` and `0`, intervals of one span written another way (`1 day` and `24:00:00`), `char(n)` values of two lengths (`char(3)` against `char(5)`), and `jsonb` values whose numbers have another scale (`{"rate": 1.0}` and `{"rate": 1}`) as one key, as Postgres does, so a shape sends and removes those rows as Postgres selects them. A where clause that casts with a type modifier, such as `varchar(2)` or `timestamp(0)`, is refused with a 400, because the service would compare the value before Postgres rounds or cuts it. A precision of 6 on a time or a timestamp changes no value and stays accepted.
