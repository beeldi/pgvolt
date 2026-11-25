---
'@electric-sql/client': patch
---

Order the `inf` operation boundary (such as `0_inf`, the end of the initial
snapshot) after finite operations when comparing offsets, so an SSE up-to-date
offset is no longer ignored while the stream sits at `0_inf`. The `Offset` type
now accepts that form.
