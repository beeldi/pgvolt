/**
 * Digest of a shape key for browser storage: persisted entries can identify a
 * shape without holding the parameters of its request.
 *
 * The callers are synchronous, which rules out `crypto.subtle`. Two shapes
 * sharing a digest would only exchange a cache hint that the server then
 * ignores, so a 53-bit non-cryptographic hash (cyrb53) is enough.
 */
export function hashShapeKey(shapeKey: string): string {
  let h1 = 0xdeadbeef
  let h2 = 0x41c6ce57

  for (let i = 0; i < shapeKey.length; i++) {
    const code = shapeKey.charCodeAt(i)
    h1 = Math.imul(h1 ^ code, 2654435761)
    h2 = Math.imul(h2 ^ code, 1597334677)
  }

  h1 = Math.imul(h1 ^ (h1 >>> 16), 2246822507)
  h1 ^= Math.imul(h2 ^ (h2 >>> 13), 3266489909)
  h2 = Math.imul(h2 ^ (h2 >>> 16), 2246822507)
  h2 ^= Math.imul(h1 ^ (h1 >>> 13), 3266489909)

  return (4294967296 * (2097151 & h2) + (h1 >>> 0)).toString(36)
}
