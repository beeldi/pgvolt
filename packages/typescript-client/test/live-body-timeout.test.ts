// @vitest-environment node
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { ShapeStream } from '../src'
import {
  MockFetchHarness,
  upToDateResponse,
} from './support/mock-fetch-harness'

const liveHeaders = () =>
  new Headers({
    'electric-handle': `test-handle`,
    'electric-offset': `0_1`,
    'electric-schema': `{}`,
    'electric-cursor': `cursor-2`,
  })

function stallingBodyResponse(): Response {
  const body = new ReadableStream<Uint8Array>({ start() {} })
  return new Response(body, { status: 200, headers: liveHeaders() })
}

/**
 * A live body that arrives one chunk every `gapMs`. With `stallAfter` set the
 * body stops after that many chunks and never ends.
 */
function slowBodyResponse(opts: {
  text: string
  chunks: number
  gapMs: number
  stallAfter?: number
}): Response {
  const size = Math.ceil(opts.text.length / opts.chunks)
  const encoder = new TextEncoder()
  let sent = 0
  let timer: ReturnType<typeof setTimeout> | undefined
  const body = new ReadableStream<Uint8Array>({
    pull(controller) {
      if (sent === opts.stallAfter) return new Promise(() => {})
      return new Promise<void>((resolve) => {
        timer = setTimeout(() => {
          const start = sent * size
          if (start >= opts.text.length) {
            controller.close()
          } else {
            controller.enqueue(
              encoder.encode(opts.text.slice(start, start + size))
            )
            sent++
          }
          resolve()
        }, opts.gapMs)
      })
    },
    cancel() {
      clearTimeout(timer)
    },
  })
  return new Response(body, { status: 200, headers: liveHeaders() })
}

const insertThenUpToDate = JSON.stringify([
  { key: `k1`, value: { id: `1` }, headers: { operation: `insert` } },
  { headers: { control: `up-to-date` } },
])

describe(`Live request body watchdog`, () => {
  let aborter: AbortController

  beforeEach(() => {
    aborter = new AbortController()
  })

  afterEach(() => {
    aborter.abort()
  })

  it(`aborts and reconnects when a live response body stalls past the timeout`, async () => {
    const harness = new MockFetchHarness()
    harness.onFallback((input) =>
      input.toString().includes(`live=true`)
        ? stallingBodyResponse()
        : upToDateResponse()
    )
    const stream = new ShapeStream({
      url: `http://localhost:3000/v1/shape`,
      params: { table: `test_table` },
      fetchClient: harness.fetchClient,
      signal: aborter.signal,
      liveRequestTimeoutMs: 40,
    })

    const unsub = stream.subscribe(() => {})

    let liveIndex = -1
    await vi.waitFor(
      () => {
        liveIndex = harness.requests.findIndex((request) =>
          request.input.toString().includes(`live=true`)
        )
        expect(liveIndex).toBeGreaterThanOrEqual(0)
      },
      { timeout: 2000 }
    )

    await vi.waitFor(
      () => {
        const liveSignal = harness.requests[liveIndex]?.init?.signal
        expect(liveSignal?.aborted).toBe(true)
        expect(liveSignal?.reason).toBe(`live-request-timeout`)
        expect(harness.requests.length).toBeGreaterThan(liveIndex + 1)
      },
      { timeout: 2000 }
    )

    unsub()
  })

  it(`lets a large live body finish while it keeps arriving past the timeout`, async () => {
    const harness = new MockFetchHarness()
    let liveRequests = 0
    harness.onFallback((input) => {
      if (!input.toString().includes(`live=true`)) return upToDateResponse()
      // The body takes five times the timeout, a chunk at a time.
      return ++liveRequests === 1
        ? slowBodyResponse({ text: insertThenUpToDate, chunks: 10, gapMs: 50 })
        : stallingBodyResponse()
    })
    const stream = new ShapeStream({
      url: `http://localhost:3000/v1/shape`,
      params: { table: `test_table` },
      fetchClient: harness.fetchClient,
      signal: aborter.signal,
      liveRequestTimeoutMs: 100,
    })
    const keys: Array<string | undefined> = []
    stream.subscribe((messages) => {
      for (const message of messages) {
        if (`key` in message) keys.push(message.key)
      }
    })

    await vi.waitFor(() => expect(keys).toEqual([`k1`]), { timeout: 3000 })

    const firstLive = harness.requests.find((request) =>
      request.input.toString().includes(`live=true`)
    )
    expect(firstLive?.init?.signal?.aborted).toBe(false)
  })

  it(`still aborts a live body that stops arriving part way`, async () => {
    const harness = new MockFetchHarness()
    harness.onFallback((input) =>
      input.toString().includes(`live=true`)
        ? slowBodyResponse({
            text: insertThenUpToDate,
            chunks: 10,
            gapMs: 20,
            stallAfter: 3,
          })
        : upToDateResponse()
    )
    const stream = new ShapeStream({
      url: `http://localhost:3000/v1/shape`,
      params: { table: `test_table` },
      fetchClient: harness.fetchClient,
      signal: aborter.signal,
      liveRequestTimeoutMs: 100,
    })

    stream.subscribe(() => {})

    await vi.waitFor(
      () => {
        const live = harness.requests.filter((request) =>
          request.input.toString().includes(`live=true`)
        )
        expect(live.length).toBeGreaterThan(1)
        expect(live[0]?.init?.signal?.reason).toBe(`live-request-timeout`)
      },
      { timeout: 3000 }
    )
  })
})
