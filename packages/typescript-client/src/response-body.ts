export const DEFAULT_MAX_RESPONSE_BYTES = 256 * 1024 * 1024

export interface BodyRead {
  text: string
  /** False when the body is longer than the limit; `text` is then a prefix. */
  complete: boolean
}

/**
 * Reads the body of `response` as text, holding at most `maxBytes` of it. A
 * longer body is cancelled as soon as it passes the limit.
 *
 * Runtimes without streamed bodies, such as React Native, load the body in
 * full before handing it over, so all this can do there is refuse a declared
 * length over the limit.
 *
 * `onChunk` is called for each chunk received, so a caller can tell a body
 * that is still arriving from one that has stalled.
 */
export async function readBodyUpTo(
  response: Response,
  maxBytes: number,
  onChunk?: () => void
): Promise<BodyRead> {
  if (!response.body || response.bodyUsed) {
    if (Number(response.headers.get(`content-length`)) > maxBytes) {
      return { text: ``, complete: false }
    }
    const text = await response.text()
    const fitting = utf8PrefixLength(text, maxBytes)
    return fitting < text.length
      ? { text: text.slice(0, fitting), complete: false }
      : { text, complete: true }
  }

  const reader = response.body.getReader()
  const decoder = new TextDecoder()
  let text = ``
  let received = 0

  while (true) {
    const { done, value } = await reader.read()
    if (done) return { text: text + decoder.decode(), complete: true }

    onChunk?.()
    received += value.byteLength
    if (received > maxBytes) {
      reader.cancel().catch(() => {})
      return { text, complete: false }
    }
    text += decoder.decode(value, { stream: true })
  }
}

/** How many UTF-16 code units of `text` fit in `maxBytes` of UTF-8. */
function utf8PrefixLength(text: string, maxBytes: number): number {
  let bytes = 0
  let index = 0
  while (index < text.length) {
    const codePoint = text.codePointAt(index)!
    bytes +=
      codePoint < 0x80 ? 1 : codePoint < 0x800 ? 2 : codePoint < 0x10000 ? 3 : 4
    if (bytes > maxBytes) return index
    index += codePoint > 0xffff ? 2 : 1
  }
  return index
}
