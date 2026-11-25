---
"@core/sync-service": patch
---

A store on a volume of its own, mounted at the shapes directory or at a stack's directory under the storage directory, now has its shapes deleted. The deleter renamed them into a trash on the storage directory's filesystem, which a rename does not reach from another filesystem, so every shape removal left the shape's files and a purge left the whole store, its transaction journal included, in place. Such a path now goes to a trash inside the store, which the deleter empties after each delete into it.
