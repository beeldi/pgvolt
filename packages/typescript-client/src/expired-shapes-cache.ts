import { hashShapeKey } from './shape-key-hash'

interface ExpiredShapeCacheEntry {
  expiredHandle: string
  lastUsed: number
}

/**
 * LRU cache for tracking expired shapes with automatic cleanup.
 *
 * Membership changes (mark/delete/clear) persist immediately. A read hit only
 * bumps `lastUsed` in memory: it is eviction ordering rather than durable
 * content, and every request/response cycle looks a handle up, so persisting
 * it would re-serialize the whole cache (up to 250 entries) on each request.
 */
export class ExpiredShapesCache {
  private data: Record<string, ExpiredShapeCacheEntry> = {}
  private max: number = 250
  private readonly storageKey = `electric_expired_shapes_v2`
  private readonly legacyStorageKey = `electric_expired_shapes`

  getExpiredHandle(shapeUrl: string): string | null {
    const entry = this.data[hashShapeKey(shapeUrl)]
    if (entry) {
      // Update last used time when accessed
      entry.lastUsed = Date.now()
      return entry.expiredHandle
    }
    return null
  }

  markExpired(shapeUrl: string, handle: string): void {
    this.data[hashShapeKey(shapeUrl)] = {
      expiredHandle: handle,
      lastUsed: Date.now(),
    }

    const keys = Object.keys(this.data)
    if (keys.length > this.max) {
      const oldest = keys.reduce((min, k) =>
        this.data[k].lastUsed < this.data[min].lastUsed ? k : min
      )
      delete this.data[oldest]
    }

    this.save()
  }

  private save(): void {
    try {
      if (typeof localStorage === `undefined`) return
      localStorage.setItem(this.storageKey, JSON.stringify(this.data))
    } catch {
      // Ignore localStorage errors
    }
  }

  private load(): void {
    try {
      if (typeof localStorage === `undefined`) return
      localStorage.removeItem(this.legacyStorageKey)
      const stored = localStorage.getItem(this.storageKey)
      if (stored) {
        this.data = JSON.parse(stored)
      }
    } catch {
      // Ignore localStorage errors, start fresh
      this.data = {}
    }
  }

  constructor() {
    this.load()
  }

  clear(): void {
    this.data = {}
    this.save()
  }

  delete(shapeUrl: string): void {
    delete this.data[hashShapeKey(shapeUrl)]
    this.save()
  }
}

// Module-level singleton instance
export const expiredShapesCache = new ExpiredShapesCache()
