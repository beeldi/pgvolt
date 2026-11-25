---
'@electric-sql/client': patch
---

Client warnings now pass the `[Electric]` prefix as their first console argument and the message after it, so a `%` inside an error message, handle, or state name can no longer be read as a format directive and swallow the stack trace argument. The printed text is unchanged.
