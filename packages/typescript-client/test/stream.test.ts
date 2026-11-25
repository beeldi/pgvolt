import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import {
  ShapeStream,
  ShapeStreamOptions,
  Shape,
  FetchError,
  FetchBackoffAbortError,
  isChangeMessage,
  isControlMessage,
  Message,
  ChangeMessage,
  Offset,
  Row,
  _resetHttpWarningForTesting,
} from '../src'
import { snakeCamelMapper } from '../src/column-mapper'
import { expiredShapesCache } from '../src/expired-shapes-cache'
import { upToDateTracker } from '../src/up-to-date-tracker'
import { resolveInMacrotask } from './support/test-helpers'

describe(`ShapeStream`, () => {
  const shapeUrl = `https://example.com/v1/shape`
  let aborter: AbortController

  beforeEach(() => {
    localStorage.clear()
    expiredShapesCache.clear()
    upToDateTracker.clear()
    aborter = new AbortController()
  })

  afterEach(() => aborter.abort())

  it(`does not create an unhandled rejection when a subscriber handles an error`, async () => {
    const unhandledRejections: unknown[] = []
    const onUnhandledRejection = (reason: unknown) => {
      unhandledRejections.push(reason)
    }
    process.on(`unhandledRejection`, onUnhandledRejection)

    try {
      const stream = new ShapeStream({
        url: shapeUrl,
        params: { table: `test` },
        signal: aborter.signal,
        fetchClient: async () =>
          new Response(undefined, {
            status: 401,
          }),
      })

      const subscriberError = new Promise<Error>((resolve) => {
        stream.subscribe(() => {}, resolve)
      })

      await expect(subscriberError).resolves.toBeInstanceOf(FetchError)
      await resolveInMacrotask(undefined)
      expect(unhandledRejections).toEqual([])
    } finally {
      process.off(`unhandledRejection`, onUnhandledRejection)
    }
  })

  it(`Shape rejects rows/value when the stream fails terminally with a non-FetchError`, async () => {
    // A 200 response missing the required Electric headers makes the stream throw
    // a MissingHeadersError — a terminal, non-FetchError error. Before the fix the
    // Shape dropped it, leaving rows/value pending forever.
    const stream = new ShapeStream({
      url: shapeUrl,
      params: { table: `test` },
      signal: aborter.signal,
      fetchClient: async () =>
        new Response(JSON.stringify([]), {
          status: 200,
          headers: { 'content-type': `application/json` },
        }),
    })
    const shape = new Shape(stream)

    // An early reader, subscribed before the failure, must be rejected when it lands.
    await expect(shape.rows).rejects.toBeInstanceOf(Error)

    // A late reader, attached after the stream already failed, must reject up front
    // rather than wait for a notification that can never arrive.
    await expect(shape.value).rejects.toBeInstanceOf(Error)
  })

  it(`applies overlapping subset snapshots by database_lsn so a stale one cannot overwrite a newer row`, async () => {
    // Two overlapping snapshots for the same key. The newer one (database_lsn 100)
    // is applied first; the older one (database_lsn 50) completes last and must NOT
    // overwrite the newer row.
    const snapshots = [
      { database_lsn: `100`, v: `new` },
      { database_lsn: `50`, v: `old` },
    ]
    let snapshotCall = 0

    const fetchClient = async (input: RequestInfo | URL): Promise<Response> => {
      const url = String(input instanceof Request ? input.url : input)
      if (url.includes(`subset__`)) {
        const spec = snapshots[snapshotCall++]
        return new Response(
          JSON.stringify({
            metadata: {
              snapshot_mark: snapshotCall,
              xmin: `1`,
              xmax: `2`,
              xip_list: [],
              database_lsn: spec.database_lsn,
            },
            data: [
              {
                key: `shared`,
                value: { id: `shared`, v: spec.v },
                headers: { operation: `insert`, relation: [`public`, `test`] },
                offset: `0_0`,
              },
            ],
          }),
          {
            status: 200,
            headers: {
              'content-type': `application/json`,
              'electric-handle': `handle-1`,
              'electric-offset': `0_0`,
              'electric-schema': `{"id":{"type":"text"},"v":{"type":"text"}}`,
            },
          }
        )
      }

      // Regular fetch: report up-to-date with no rows so the stream idles.
      return new Response(
        JSON.stringify([{ headers: { control: `up-to-date` } }]),
        {
          status: 200,
          headers: {
            'content-type': `application/json`,
            'electric-handle': `handle-1`,
            'electric-offset': `0_0`,
            'electric-schema': `{"id":{"type":"text"},"v":{"type":"text"}}`,
            'electric-cursor': `1`,
          },
        }
      )
    }

    const stream = new ShapeStream({
      url: shapeUrl,
      params: { table: `test` },
      log: `changes_only`,
      signal: aborter.signal,
      fetchClient,
      subscribe: false,
    })
    const shape = new Shape(stream)

    await stream.requestSnapshot({ limit: 1 }) // newer snapshot, lsn 100
    await stream.requestSnapshot({ limit: 1 }) // older snapshot, lsn 50, completes last

    expect(shape.currentValue.get(`shared`)).toEqual({ id: `shared`, v: `new` })
  })

  it.each([
    { method: `fetchSnapshot`, subsetMethod: `GET` },
    { method: `fetchSnapshot`, subsetMethod: `POST` },
    { method: `requestSnapshot`, subsetMethod: `GET` },
    { method: `requestSnapshot`, subsetMethod: `POST` },
  ] as const)(
    `$method aborts an in-flight $subsetMethod snapshot with the stream signal`,
    async ({ method, subsetMethod }) => {
      let signal: AbortSignal | null | undefined
      let rejectFetch!: (error: unknown) => void
      const fetchMock = vi.fn(
        (_input: RequestInfo | URL, init?: RequestInit) => {
          signal = init?.signal
          return new Promise<Response>((_resolve, reject) => {
            rejectFetch = reject
            signal?.addEventListener(`abort`, () => reject(signal!.reason), {
              once: true,
            })
          })
        }
      )
      const stream = new ShapeStream({
        url: shapeUrl,
        params: { table: `test` },
        signal: aborter.signal,
        subsetMethod,
        log: `changes_only`,
        fetchClient: fetchMock,
        backoffOptions: {
          initialDelay: 1,
          maxDelay: 1,
          multiplier: 1,
          maxRetries: 0,
        },
      })
      const result = stream[method]({ limit: 1 }).catch((error) => error)

      try {
        await vi.waitFor(() => expect(fetchMock).toHaveBeenCalledOnce())
        expect(signal).toBe(aborter.signal)
        expect(signal?.aborted).toBe(false)
        aborter.abort()
        expect(await result).toBeInstanceOf(FetchBackoffAbortError)
        expect(fetchMock).toHaveBeenCalledOnce()
      } finally {
        aborter.abort()
        // Settle the request even if the signal assertion fails.
        rejectFetch(aborter.signal.reason)
        await result
      }
    }
  )

  it(`requestSnapshot waits for snapshot messages to be published to subscribers before resolving`, async () => {
    const snapshotRow = {
      key: `test-1`,
      value: { id: `1` },
      headers: {
        operation: `insert`,
        relation: [`public`, `test`],
      },
      offset: `0_0`,
    }

    const fetchMock = vi.fn(() =>
      Promise.resolve(
        new Response(
          JSON.stringify({
            metadata: {
              snapshot_mark: 1,
              xmin: `1`,
              xmax: `2`,
              xip_list: [],
              database_lsn: `0`,
            },
            data: [snapshotRow],
          }),
          {
            status: 200,
            headers: {
              'content-type': `application/json`,
              'electric-handle': `handle-1`,
              'electric-offset': `0_0`,
              'electric-schema': `{"id":{"type":"text"}}`,
            },
          }
        )
      )
    )

    const stream = new ShapeStream({
      url: shapeUrl,
      params: { table: `test` },
      log: `changes_only`,
      signal: aborter.signal,
      fetchClient: fetchMock,
      subscribe: false,
    })

    let releaseSubscriber!: () => void
    const subscriberFinished = new Promise<void>((resolve) => {
      releaseSubscriber = resolve
    })
    let snapshotRequestResolved = false
    let publishedMessages: Message<Row>[] = []

    stream.subscribe(async (messages) => {
      if (messages.some(isChangeMessage)) {
        publishedMessages = messages
        await subscriberFinished
      }
    })

    const snapshotRequest = stream.requestSnapshot({ limit: 1 }).then(() => {
      snapshotRequestResolved = true
    })

    await resolveInMacrotask(undefined)
    expect(snapshotRequestResolved).toBe(false)

    releaseSubscriber()
    await snapshotRequest

    expect(publishedMessages.some(isChangeMessage)).toBe(true)
    expect(
      publishedMessages.some(
        (message) =>
          isControlMessage(message) &&
          message.headers.control === `snapshot-end`
      )
    ).toBe(true)
    expect(
      publishedMessages.some(
        (message) =>
          isControlMessage(message) && message.headers.control === `subset-end`
      )
    ).toBe(true)
  })

  it(`requestSnapshot can be awaited reentrantly from a subscriber`, async () => {
    const streamRow = {
      key: `stream-1`,
      value: { id: `1` },
      headers: {
        operation: `insert`,
        relation: [`public`, `test`],
      },
      offset: `0_0`,
    }
    const snapshotRow = {
      key: `snapshot-1`,
      value: { id: `2` },
      headers: {
        operation: `insert`,
        relation: [`public`, `test`],
      },
      offset: `0_1`,
    }

    let requestCount = 0
    const fetchMock = vi.fn(() => {
      requestCount++

      if (requestCount === 1) {
        return Promise.resolve(
          new Response(
            JSON.stringify([
              streamRow,
              { headers: { control: `up-to-date` }, offset: `0_0` },
            ]),
            {
              status: 200,
              headers: {
                'content-type': `application/json`,
                'electric-handle': `handle-1`,
                'electric-offset': `0_0`,
                'electric-schema': `{"id":{"type":"text"}}`,
              },
            }
          )
        )
      }

      return Promise.resolve(
        new Response(
          JSON.stringify({
            metadata: {
              snapshot_mark: 1,
              xmin: `1`,
              xmax: `2`,
              xip_list: [],
              database_lsn: `0`,
            },
            data: [snapshotRow],
          }),
          {
            status: 200,
            headers: {
              'content-type': `application/json`,
              'electric-handle': `handle-1`,
              'electric-offset': `0_1`,
              'electric-schema': `{"id":{"type":"text"}}`,
            },
          }
        )
      )
    })

    const stream = new ShapeStream({
      url: shapeUrl,
      params: { table: `test` },
      log: `changes_only`,
      signal: aborter.signal,
      fetchClient: fetchMock,
      subscribe: false,
    })

    let requestedSnapshot = false
    let reentrantSnapshotResolved = false
    stream.subscribe(async (messages) => {
      if (requestedSnapshot || !messages.some(isChangeMessage)) return

      requestedSnapshot = true
      await stream.requestSnapshot({ limit: 1 })
      reentrantSnapshotResolved = true
    })

    await vi.waitFor(() => {
      expect(reentrantSnapshotResolved).toBe(true)
    })
  })

  it(`does not deliver a later SSE batch before subscriber callbacks for the earlier batch finish`, async () => {
    const warnSpy = vi.spyOn(console, `warn`).mockImplementation(() => {})

    const batch1Row = {
      key: `stream-1`,
      value: { id: `1` },
      headers: {
        operation: `insert`,
        relation: [`public`, `test`],
      },
      offset: `0_1`,
    }
    const batch2Row = {
      key: `stream-2`,
      value: { id: `2` },
      headers: {
        operation: `insert`,
        relation: [`public`, `test`],
      },
      offset: `0_2`,
    }

    const encode = (message: unknown) =>
      new TextEncoder().encode(`data: ${JSON.stringify(message)}\n\n`)

    let releaseSecondBatch!: () => void
    const secondBatchGate = new Promise<void>((resolve) => {
      releaseSecondBatch = resolve
    })

    let sseConnectionCount = 0
    const fetchMock = vi.fn((input: string | URL | Request) => {
      const url = input.toString()

      if (url.includes(`live_sse=true`)) {
        sseConnectionCount++
        const isFirst = sseConnectionCount === 1
        const stream = new ReadableStream<Uint8Array>({
          async start(controller) {
            if (!isFirst) {
              return
            }
            controller.enqueue(encode(batch1Row))
            controller.enqueue(
              encode({ headers: { control: `up-to-date` }, offset: `0_1` })
            )
            await secondBatchGate
            controller.enqueue(encode(batch2Row))
            controller.enqueue(
              encode({ headers: { control: `up-to-date` }, offset: `0_2` })
            )
            controller.close()
          },
        })

        return Promise.resolve(
          new Response(stream, {
            status: 200,
            headers: {
              'content-type': `text/event-stream`,
              'electric-handle': `handle-1`,
              'electric-offset': `0_0`,
              'electric-cursor': `cursor-1`,
              'electric-schema': `{"id":{"type":"text"}}`,
            },
          })
        )
      }

      return Promise.resolve(
        new Response(
          JSON.stringify([
            { headers: { control: `up-to-date` }, offset: `0_0` },
          ]),
          {
            status: 200,
            headers: {
              'content-type': `application/json`,
              'electric-handle': `handle-1`,
              'electric-offset': `0_0`,
              'electric-cursor': `cursor-0`,
              'electric-schema': `{"id":{"type":"text"}}`,
              'electric-up-to-date': ``,
            },
          }
        )
      )
    })

    const stream = new ShapeStream({
      url: shapeUrl,
      params: { table: `test` },
      signal: aborter.signal,
      fetchClient: fetchMock,
      liveSse: true,
    })

    let releaseFirstBatch!: () => void
    const firstBatchBlocked = new Promise<void>((resolve) => {
      releaseFirstBatch = resolve
    })

    const events: string[] = []
    let firstBatchSeen = false

    stream.subscribe(async (messages) => {
      const change = messages.find(isChangeMessage)
      if (!change) return

      if (change.key === `stream-1`) {
        events.push(`batch1:start`)
        firstBatchSeen = true
        releaseSecondBatch()
        await firstBatchBlocked
        events.push(`batch1:end`)
        return
      }

      if (change.key === `stream-2`) {
        events.push(`batch2:start`)
        aborter.abort()
      }
    })

    await vi.waitFor(() => {
      expect(firstBatchSeen).toBe(true)
    })

    await resolveInMacrotask(undefined)
    expect(events).toEqual([`batch1:start`])

    releaseFirstBatch()

    try {
      await vi.waitFor(() => {
        expect(events).toEqual([`batch1:start`, `batch1:end`, `batch2:start`])
      })
    } finally {
      warnSpy.mockRestore()
    }
  })

  const encodeEvent = (message: unknown) =>
    new TextEncoder().encode(`data: ${JSON.stringify(message)}\n\n`)

  const insertRow = (n: number, offset: string) => ({
    key: `row-${n}`,
    value: { id: `${n}` },
    headers: { operation: `insert`, relation: [`public`, `test`] },
    offset,
  })

  const electricHeaders = (offset: string, cursor: string) => ({
    'electric-handle': `handle-1`,
    'electric-offset': offset,
    'electric-cursor': cursor,
    'electric-schema': `{"id":{"type":"text"}}`,
  })

  const sseResponse = (body: ReadableStream<Uint8Array>, offset: string) =>
    new Response(body, {
      status: 200,
      headers: {
        ...electricHeaders(offset, `cursor-1`),
        'content-type': `text/event-stream`,
      },
    })

  const upToDateResponse = (
    messages: Array<unknown>,
    offset: string,
    cursor = `cursor-1`
  ) =>
    new Response(JSON.stringify(messages), {
      status: 200,
      headers: {
        ...electricHeaders(offset, cursor),
        'content-type': `application/json`,
        'electric-up-to-date': ``,
      },
    })

  const initialResponse = () =>
    upToDateResponse(
      [{ headers: { control: `up-to-date` }, offset: `0_0` }],
      `0_0`,
      `cursor-0`
    )

  /** Starts a live SSE stream and returns the keys of the rows it receives. */
  function startLiveSseStream(fetchClient: typeof fetch): Array<string> {
    const stream = new ShapeStream({
      url: shapeUrl,
      params: { table: `test` },
      signal: aborter.signal,
      fetchClient,
      liveSse: true,
    })
    const received: Array<string> = []
    stream.subscribe((messages) => {
      for (const message of messages) {
        if (isChangeMessage(message)) received.push(message.key)
      }
    })
    return received
  }

  describe.each([
    [
      `ends cleanly`,
      (controller: ReadableStreamDefaultController) => controller.close(),
    ],
    [
      `breaks`,
      (controller: ReadableStreamDefaultController) =>
        controller.error(new Error(`connection reset`)),
    ],
  ])(`SSE body that %s before its up-to-date`, (_, endBody) => {
    it(`resumes from the last applied offset with a long poll, then returns to SSE`, async () => {
      const row = insertRow(1, `3_0`)
      const requests: URL[] = []
      let liveAfterCatchUp = false
      const fetchMock = vi.fn((input: string | URL | Request) => {
        const url = new URL(input.toString())
        requests.push(url)

        if (url.searchParams.get(`offset`) === `-1`) {
          return Promise.resolve(initialResponse())
        }

        if (url.searchParams.get(`live_sse`) !== `true`) {
          return Promise.resolve(
            upToDateResponse(
              [
                row,
                {
                  headers: { control: `up-to-date`, global_last_seen_lsn: `5` },
                },
              ],
              `5_0`
            )
          )
        }

        if (url.searchParams.get(`offset`) === `0_0`) {
          // A catch-up chunk: its header is the chunk's end, and the body
          // stops before any up-to-date.
          const body = new ReadableStream<Uint8Array>({
            async start(controller) {
              controller.enqueue(encodeEvent(row))
              // Let the client read the event before the body ends.
              await resolveInMacrotask(undefined)
              endBody(controller)
            },
          })
          return Promise.resolve(sseResponse(body, `5_0`))
        }

        liveAfterCatchUp = true
        return new Promise<Response>(() => {})
      })

      const received = startLiveSseStream(fetchMock)

      await vi.waitFor(() => {
        expect(liveAfterCatchUp).toBe(true)
      })

      const params = requests.map((url) => ({
        offset: url.searchParams.get(`offset`),
        sse: url.searchParams.get(`live_sse`),
      }))
      expect(params.slice(1)).toEqual([
        { offset: `0_0`, sse: `true` },
        { offset: `0_0`, sse: null },
        { offset: `5_0`, sse: `true` },
      ])
      expect(received).toEqual([`row-1`])
    })
  })

  describe(`SSE memory limits`, () => {
    const filler = (n: number, offset: string) => ({
      ...insertRow(n, offset),
      value: { id: `${n}`, data: `x`.repeat(200) },
    })

    it(`closes a connection whose messages pass the limit before an up-to-date, then long polls`, async () => {
      const requests: URL[] = []
      const cancelled = vi.fn()
      let liveAfterCatchUp = false
      const fetchMock = vi.fn((input: string | URL | Request) => {
        const url = new URL(input.toString())
        requests.push(url)

        if (url.searchParams.get(`offset`) === `-1`) {
          return Promise.resolve(initialResponse())
        }

        if (url.searchParams.get(`live_sse`) !== `true`) {
          return Promise.resolve(
            upToDateResponse(
              [
                insertRow(9, `5_0`),
                {
                  headers: { control: `up-to-date`, global_last_seen_lsn: `5` },
                },
              ],
              `5_0`
            )
          )
        }

        if (url.searchParams.get(`offset`) === `0_0`) {
          let n = 0
          const body = new ReadableStream<Uint8Array>(
            {
              async pull(controller) {
                await resolveInMacrotask(undefined)
                controller.enqueue(encodeEvent(filler(++n, `0_${n}`)))
              },
              cancel: cancelled,
            },
            { highWaterMark: 0 }
          )
          return Promise.resolve(sseResponse(body, `5_0`))
        }

        liveAfterCatchUp = true
        return new Promise<Response>(() => {})
      })

      const stream = new ShapeStream({
        url: shapeUrl,
        params: { table: `test` },
        signal: aborter.signal,
        fetchClient: fetchMock,
        liveSse: true,
        maxResponseBytes: 2_000,
      })
      const received: Array<string> = []
      stream.subscribe((messages) => {
        for (const message of messages) {
          if (isChangeMessage(message)) received.push(message.key)
        }
      })

      await vi.waitFor(() => {
        expect(liveAfterCatchUp).toBe(true)
      })

      expect(cancelled).toHaveBeenCalledOnce()
      expect(
        requests.slice(1).map((url) => ({
          offset: url.searchParams.get(`offset`),
          sse: url.searchParams.get(`live_sse`),
        }))
      ).toEqual([
        { offset: `0_0`, sse: `true` },
        { offset: `0_0`, sse: null },
        { offset: `5_0`, sse: `true` },
      ])
      expect(received).toEqual([`row-9`])
    })

    it(`closes the connection and waits for subscribers when delivered batches pile up`, async () => {
      const upToDate = (n: number) => ({
        headers: { control: `up-to-date`, global_last_seen_lsn: `${n}` },
      })
      const closed = vi.fn()
      const liveRequestTimes: Array<number> = []
      let releaseSubscriber = () => {}
      let released = 0
      const fetchMock = vi.fn((input: string | URL | Request) => {
        const url = new URL(input.toString())

        if (url.searchParams.get(`offset`) === `-1`) {
          return Promise.resolve(initialResponse())
        }

        liveRequestTimes.push(released)
        if (liveRequestTimes.length > 1) {
          return new Promise<Response>(() => {})
        }

        let n = 0
        const body = new ReadableStream<Uint8Array>(
          {
            async pull(controller) {
              await resolveInMacrotask(undefined)
              n++
              controller.enqueue(encodeEvent(filler(n, `${n}_0`)))
              controller.enqueue(encodeEvent(upToDate(n)))
            },
            cancel: closed,
          },
          { highWaterMark: 0 }
        )
        return Promise.resolve(sseResponse(body, `1_0`))
      })

      const stream = new ShapeStream({
        url: shapeUrl,
        params: { table: `test` },
        signal: aborter.signal,
        fetchClient: fetchMock,
        liveSse: true,
        maxResponseBytes: 2_000,
      })
      let firstBatch = true
      stream.subscribe(async () => {
        if (firstBatch) {
          firstBatch = false
          return
        }
        await new Promise<void>((resolve) => {
          releaseSubscriber = () => {
            released++
            resolve()
          }
        })
      })

      await vi.waitFor(() => {
        expect(closed).toHaveBeenCalledOnce()
      })

      await resolveInMacrotask(undefined)
      expect(liveRequestTimes).toHaveLength(1)

      await vi.waitFor(() => {
        releaseSubscriber()
        expect(liveRequestTimes).toHaveLength(2)
      })
      expect(liveRequestTimes[1]).toBeGreaterThan(0)
    })

    it(`does not read a close for slow subscribers as a short connection`, async () => {
      const warnSpy = vi.spyOn(console, `warn`).mockImplementation(() => {})
      const upToDate = (n: number) => ({
        headers: { control: `up-to-date`, global_last_seen_lsn: `${n}` },
      })
      const batchBytes =
        encodeEvent(filler(1, `1_0`)).byteLength +
        encodeEvent(upToDate(1)).byteLength
      const emptyBatchBytes = encodeEvent(upToDate(1)).byteLength
      let releaseSubscriber = () => {}
      const liveRequests: Array<string | null> = []
      const fetchMock = vi.fn((input: string | URL | Request) => {
        const url = new URL(input.toString())
        if (url.searchParams.get(`offset`) === `-1`) {
          return Promise.resolve(initialResponse())
        }
        liveRequests.push(url.searchParams.get(`live_sse`))
        const connection = liveRequests.length
        if (connection > 6 || url.searchParams.get(`live_sse`) !== `true`) {
          return new Promise<Response>(() => {})
        }
        const round = Math.ceil(connection / 2)
        const body = new ReadableStream<Uint8Array>({
          start(controller) {
            if (connection % 2 === 1) {
              controller.enqueue(encodeEvent(filler(round, `${round}_0`)))
              controller.enqueue(encodeEvent(upToDate(round)))
              controller.close()
              return
            }
            controller.enqueue(encodeEvent(upToDate(round)))
          },
          cancel: () => releaseSubscriber(),
        })
        return Promise.resolve(sseResponse(body, `${round}_0`))
      })

      const stream = new ShapeStream({
        url: shapeUrl,
        params: { table: `test` },
        signal: aborter.signal,
        fetchClient: fetchMock,
        liveSse: true,
        maxResponseBytes: batchBytes + emptyBatchBytes - 1,
      })
      stream.subscribe(async (messages) => {
        if (messages.some(isChangeMessage)) {
          await new Promise<void>((resolve) => {
            releaseSubscriber = resolve
          })
        }
      })

      await vi.waitFor(
        () => {
          expect(liveRequests).toHaveLength(7)
        },
        { timeout: 15_000 }
      )

      expect(liveRequests[6]).toBe(`true`)
      expect(warnSpy).not.toHaveBeenCalled()
      warnSpy.mockRestore()
    })
  })

  describe(`column mapping of hostile column names`, () => {
    it(`keeps a __proto__ column as an ordinary field of the row`, async () => {
      const responseBody = `[{"key":"row-1","value":{"__proto__":{"isAdmin":true},"id":"1"},"headers":{"operation":"insert"},"offset":"0_0"},{"headers":{"control":"up-to-date"},"offset":"0_0"}]`
      const fetchMock = vi.fn(() =>
        Promise.resolve(
          new Response(responseBody, {
            status: 200,
            headers: {
              ...electricHeaders(`0_0`, `cursor-0`),
              'electric-schema': `{"id":{"type":"text"}}`,
              'content-type': `application/json`,
              'electric-up-to-date': ``,
            },
          })
        )
      )

      const stream = new ShapeStream({
        url: shapeUrl,
        params: { table: `test` },
        signal: aborter.signal,
        fetchClient: fetchMock,
        subscribe: false,
        columnMapper: { decode: (name) => name, encode: (name) => name },
      })
      const rows: Array<Row> = []
      await new Promise<void>((resolve) => {
        stream.subscribe((messages) => {
          for (const message of messages) {
            if (isChangeMessage(message)) rows.push(message.value)
          }
          resolve()
        })
      })

      expect(Object.getPrototypeOf(rows[0])).toBe(Object.prototype)
      expect((rows[0] as Record<string, unknown>).isAdmin).toBeUndefined()
      expect(Object.keys(rows[0]).sort()).toEqual([`__proto__`, `id`])
    })
  })

  describe(`SSE short-connection fallback`, () => {
    let warnSpy: ReturnType<typeof vi.spyOn>

    beforeEach(() => {
      warnSpy = vi.spyOn(console, `warn`).mockImplementation(() => {})
    })

    afterEach(() => warnSpy.mockRestore())

    const warnedOfSseFallback = () =>
      warnSpy.mock.calls.some((args: Array<unknown>) =>
        String(args[0]).includes(`SSE connections are closing immediately`)
      )

    it(`keeps using SSE when the server closes catch-up responses that carried messages`, async () => {
      const catchUps = 4
      let sseRequests = 0
      const liveRequests: Array<string> = []

      const fetchMock = vi.fn((input: string | URL | Request) => {
        const url = new URL(input.toString())
        if (url.searchParams.get(`live`) !== `true`) {
          return Promise.resolve(initialResponse())
        }

        liveRequests.push(url.toString())
        if (url.searchParams.get(`live_sse`) !== `true`) {
          return new Promise<Response>(() => {})
        }

        const n = ++sseRequests
        if (n > catchUps) {
          return Promise.resolve(
            sseResponse(new ReadableStream<Uint8Array>(), `${catchUps}_0`)
          )
        }

        // Behind the log end: the server sends the catch-up, then closes.
        const body = new ReadableStream<Uint8Array>({
          start(controller) {
            controller.enqueue(encodeEvent(insertRow(n, `${n}_0`)))
            controller.enqueue(
              encodeEvent({
                headers: {
                  control: `up-to-date`,
                  global_last_seen_lsn: `${n}`,
                },
              })
            )
            controller.close()
          },
        })
        return Promise.resolve(sseResponse(body, `${n}_0`))
      })

      startLiveSseStream(fetchMock)

      await vi.waitFor(() => {
        expect(liveRequests).toHaveLength(catchUps + 1)
      })

      expect(sseRequests).toBe(catchUps + 1)
      expect(warnedOfSseFallback()).toBe(false)
    })

    it(`falls back to long polling when short SSE responses carry messages but leave the offset where it was`, async () => {
      const liveRequests: Array<string> = []

      const fetchMock = vi.fn((input: string | URL | Request) => {
        const url = new URL(input.toString())
        if (url.searchParams.get(`live`) !== `true`) {
          return Promise.resolve(initialResponse())
        }

        liveRequests.push(url.toString())
        if (url.searchParams.get(`live_sse`) !== `true`) {
          return new Promise<Response>(() => {})
        }

        // Every response replays the same transaction at LSN 0 and ends with
        // an up-to-date naming that LSN, which leaves the offset at `0_0`.
        const body = new ReadableStream<Uint8Array>({
          start(controller) {
            for (const op of [1, 2]) {
              controller.enqueue(encodeEvent(insertRow(op, `0_${op}`)))
            }
            controller.enqueue(
              encodeEvent({
                headers: { control: `up-to-date`, global_last_seen_lsn: `0` },
              })
            )
            controller.close()
          },
        })
        return Promise.resolve(sseResponse(body, `0_0`))
      })

      startLiveSseStream(fetchMock)

      await vi.waitFor(() => {
        expect(liveRequests).toHaveLength(4)
      })

      const sentOffsets = liveRequests.map((url) =>
        new URL(url).searchParams.get(`offset`)
      )
      const usedSse = liveRequests.map(
        (url) => new URL(url).searchParams.get(`live_sse`) === `true`
      )
      expect(sentOffsets).toEqual([`0_0`, `0_0`, `0_0`, `0_0`])
      expect(usedSse).toEqual([true, true, true, false])
      expect(warnedOfSseFallback()).toBe(true)
    })

    it(`keeps using SSE when catch-up bodies end inside a chunk, before their up-to-date`, async () => {
      const catchUps = 4
      let sseRequests = 0
      const requests: Array<{ offset: string | null; sse: boolean }> = []

      const fetchMock = vi.fn((input: string | URL | Request) => {
        const url = new URL(input.toString())
        if (url.searchParams.get(`live`) !== `true`) {
          return Promise.resolve(initialResponse())
        }

        const sse = url.searchParams.get(`live_sse`) === `true`
        requests.push({ offset: url.searchParams.get(`offset`), sse })
        // Never reached when the stream behaves; stops a regression from
        // looping on instant long-poll responses.
        if (requests.length > 12) return new Promise<Response>(() => {})

        const n = sseRequests + (sse ? 1 : 0)
        const row = insertRow(n, `${n}_0`)

        if (!sse) {
          // The long poll that follows reads the whole chunk, then it is
          // up to date and SSE resumes.
          return Promise.resolve(
            upToDateResponse(
              [row, { headers: { control: `up-to-date` } }],
              `${n}_0`,
              `cursor-${n}`
            )
          )
        }

        sseRequests++
        if (sseRequests > catchUps) {
          return Promise.resolve(
            sseResponse(new ReadableStream<Uint8Array>(), `${catchUps}_0`)
          )
        }

        // A catch-up that crosses a chunk boundary: the server sends the
        // chunk and closes without an up-to-date because more is waiting.
        const body = new ReadableStream<Uint8Array>({
          start(controller) {
            controller.enqueue(encodeEvent(row))
            controller.close()
          },
        })
        return Promise.resolve(sseResponse(body, `${n}_0`))
      })

      const received = startLiveSseStream(fetchMock)

      await vi.waitFor(() => {
        expect(sseRequests).toBe(catchUps + 1)
      })

      // Each short body is followed by a long poll from the same offset, and
      // SSE is never given up on.
      expect(requests.map((r) => r.sse)).toEqual([
        true,
        false,
        true,
        false,
        true,
        false,
        true,
        false,
        true,
      ])
      expect(received).toEqual([`row-1`, `row-2`, `row-3`, `row-4`])
      expect(warnedOfSseFallback()).toBe(false)
    })

    it(`falls back to long polling after three short SSE responses without messages`, async () => {
      const liveRequests: Array<string> = []

      const fetchMock = vi.fn((input: string | URL | Request) => {
        const url = new URL(input.toString())
        if (url.searchParams.get(`live`) !== `true`) {
          return Promise.resolve(initialResponse())
        }

        liveRequests.push(url.toString())
        if (url.searchParams.get(`live_sse`) !== `true`) {
          return new Promise<Response>(() => {})
        }

        // A buffering proxy: the body ends before any event reaches the
        // client, though the header already names the end of a catch-up.
        const body = new ReadableStream<Uint8Array>({
          start(controller) {
            controller.enqueue(new TextEncoder().encode(`: keep-alive\n\n`))
            controller.close()
          },
        })
        return Promise.resolve(sseResponse(body, `1_0`))
      })

      startLiveSseStream(fetchMock)

      await vi.waitFor(() => {
        expect(liveRequests).toHaveLength(4)
      })

      const usedSse = liveRequests.map(
        (url) => new URL(url).searchParams.get(`live_sse`) === `true`
      )
      expect(usedSse).toEqual([true, true, true, false])
      expect(warnedOfSseFallback()).toBe(true)
    })
  })

  it(`reentrant requestSnapshot may re-enter bystander subscribers before their earlier callback completes`, async () => {
    const streamRow = {
      key: `stream-1`,
      value: { id: `1` },
      headers: {
        operation: `insert`,
        relation: [`public`, `test`],
      },
      offset: `0_0`,
    }
    const snapshotRow = {
      key: `snapshot-1`,
      value: { id: `2` },
      headers: {
        operation: `insert`,
        relation: [`public`, `test`],
      },
      offset: `0_1`,
    }

    let requestCount = 0
    const fetchMock = vi.fn(() => {
      requestCount++

      if (requestCount === 1) {
        return Promise.resolve(
          new Response(
            JSON.stringify([
              streamRow,
              { headers: { control: `up-to-date` }, offset: `0_0` },
            ]),
            {
              status: 200,
              headers: {
                'content-type': `application/json`,
                'electric-handle': `handle-1`,
                'electric-offset': `0_0`,
                'electric-schema': `{"id":{"type":"text"}}`,
              },
            }
          )
        )
      }

      return Promise.resolve(
        new Response(
          JSON.stringify({
            metadata: {
              snapshot_mark: 1,
              xmin: `1`,
              xmax: `2`,
              xip_list: [],
              database_lsn: `0`,
            },
            data: [snapshotRow],
          }),
          {
            status: 200,
            headers: {
              'content-type': `application/json`,
              'electric-handle': `handle-1`,
              'electric-offset': `0_1`,
              'electric-schema': `{"id":{"type":"text"}}`,
            },
          }
        )
      )
    })

    const stream = new ShapeStream({
      url: shapeUrl,
      params: { table: `test` },
      log: `changes_only`,
      signal: aborter.signal,
      fetchClient: fetchMock,
      subscribe: false,
    })

    let releaseBystander!: () => void
    const blockBystander = new Promise<void>((resolve) => {
      releaseBystander = resolve
    })

    const bystanderEvents: string[] = []
    let initialBystanderEntered = false
    let requestedSnapshot = false

    stream.subscribe(async (messages) => {
      if (requestedSnapshot) return
      if (
        !messages.some(
          (message) => isChangeMessage(message) && message.key === `stream-1`
        )
      ) {
        return
      }

      requestedSnapshot = true
      await stream.requestSnapshot({ limit: 1 })
    })

    stream.subscribe(async (messages) => {
      const change = messages.find(isChangeMessage)
      if (!change) return

      if (change.key === `stream-1`) {
        bystanderEvents.push(`stream:start`)
        initialBystanderEntered = true
        await blockBystander
        bystanderEvents.push(`stream:end`)
        return
      }

      if (change.key === `snapshot-1`) {
        bystanderEvents.push(`snapshot:start`)
        releaseBystander()
        aborter.abort()
      }
    })

    await vi.waitFor(() => {
      expect(initialBystanderEntered).toBe(true)
    })

    await vi.waitFor(() => {
      expect(bystanderEvents).toEqual([
        `stream:start`,
        `snapshot:start`,
        `stream:end`,
      ])
    })
  })

  it(`should attach specified headers to requests`, async () => {
    const eventTarget = new EventTarget()
    const requestArgs: Array<RequestInit | undefined> = []
    const fetchWrapper = (
      ...args: Parameters<typeof fetch>
    ): Promise<Response> => {
      requestArgs.push(args[1])
      eventTarget.dispatchEvent(new Event(`fetch`))
      return Promise.resolve(Response.error())
    }

    const aborter = new AbortController()
    const stream = new ShapeStream({
      url: shapeUrl,
      params: {
        table: `foo`,
      },
      signal: aborter.signal,
      fetchClient: fetchWrapper,
      headers: {
        Authorization: `my-token`,
        'X-Custom-Header': `my-value`,
      },
    })
    const unsub = stream.subscribe(() => unsub())

    await new Promise((resolve) =>
      eventTarget.addEventListener(`fetch`, resolve, { once: true })
    )

    expect(requestArgs[0]).toMatchObject({
      headers: {
        Authorization: `my-token`,
        'X-Custom-Header': `my-value`,
      },
    })
  })

  it(`should sort query parameters for stable URLs`, async () => {
    const eventTarget = new EventTarget()
    const requestedUrls: Array<string> = []
    const fetchWrapper = (
      ...args: Parameters<typeof fetch>
    ): Promise<Response> => {
      requestedUrls.push(args[0].toString())
      eventTarget.dispatchEvent(new Event(`fetch`))
      return Promise.resolve(Response.error())
    }

    const aborter = new AbortController()
    const stream = new ShapeStream({
      url: shapeUrl,
      params: {
        table: `foo`,
        where: `a=1`,
        columns: [`id`],
      },
      handle: `potato`,
      signal: aborter.signal,
      fetchClient: fetchWrapper,
    })

    const unsub = stream.subscribe(() => unsub())

    await new Promise((resolve) =>
      eventTarget.addEventListener(`fetch`, resolve, { once: true })
    )

    expect(requestedUrls[0].split(`?`)[1]).toEqual(
      `columns=%22id%22&handle=potato&log=full&offset=-1&table=foo&where=a%3D1`
    )
  })

  it(`stops a subscribed request loop after the external signal aborts`, async () => {
    let requestCount = 0
    const fetchWrapper = async () => {
      await resolveInMacrotask(undefined)
      requestCount++
      return new Response(
        JSON.stringify([{ headers: { control: `up-to-date` }, offset: `0_0` }]),
        {
          status: 200,
          headers: {
            'content-type': `application/json`,
            'electric-handle': `handle-1`,
            'electric-offset': `0_0`,
            'electric-cursor': `${requestCount}`,
            'electric-schema': `{"id":{"type":"text"}}`,
          },
        }
      )
    }

    const stream = new ShapeStream({
      url: shapeUrl,
      params: { table: `foo` },
      signal: aborter.signal,
      fetchClient: fetchWrapper,
      subscribe: true,
    })

    stream.subscribe(() => {})

    await vi.waitFor(() => {
      expect(requestCount).toBeGreaterThan(0)
    })

    aborter.abort()
    await resolveInMacrotask(undefined)
    await resolveInMacrotask(undefined)

    const requestsAfterAbortSettled = requestCount
    await resolveInMacrotask(undefined)
    await resolveInMacrotask(undefined)

    expect(requestCount).toBe(requestsAfterAbortSettled)
  })

  it(`should start requesting only after first subscription`, async () => {
    const eventTarget = new EventTarget()
    const fetchWrapper = (): Promise<Response> => {
      eventTarget.dispatchEvent(new Event(`fetch`))
      return Promise.resolve(Response.error())
    }

    const aborter = new AbortController()
    const stream = new ShapeStream({
      url: shapeUrl,
      params: {
        table: `foo`,
        where: `a=1`,
        columns: [`id`],
      },
      handle: `potato`,
      signal: aborter.signal,
      fetchClient: fetchWrapper,
    })

    // should not fire any fetch requests
    await new Promise<void>((resolve, reject) => {
      eventTarget.addEventListener(`fetch`, reject, { once: true })
      setTimeout(() => resolve(), 100)
    })

    // should fire fetch immediately after subbing
    const startedStreaming = new Promise<void>((resolve, reject) => {
      eventTarget.addEventListener(`fetch`, () => resolve(), {
        once: true,
      })
      setTimeout(() => reject(`timed out`), 100)
    })
    const unsub = stream.subscribe(() => unsub())
    await startedStreaming
  })

  it(`should correctly serialize objects into query params`, async () => {
    const eventTarget = new EventTarget()
    const requestedUrls: Array<string> = []
    const fetchWrapper = (
      ...args: Parameters<typeof fetch>
    ): Promise<Response> => {
      requestedUrls.push(args[0].toString())
      eventTarget.dispatchEvent(new Event(`fetch`))
      return Promise.resolve(Response.error())
    }

    const aborter = new AbortController()
    const stream = new ShapeStream({
      url: shapeUrl,
      params: {
        table: `foo`,
        where: `a=$1 and b=$2`,
        columns: [`id`],
        params: {
          '1': `test1`,
          '2': `test2`,
        },
      },
      handle: `potato`,
      signal: aborter.signal,
      fetchClient: fetchWrapper,
    })

    const unsub = stream.subscribe(() => unsub())

    await new Promise((resolve) =>
      eventTarget.addEventListener(`fetch`, resolve, { once: true })
    )

    expect(requestedUrls[0].split(`?`)[1]).toEqual(
      `columns=%22id%22&handle=potato&log=full&offset=-1&params%5B1%5D=test1&params%5B2%5D=test2&table=foo&where=a%3D%241+and+b%3D%242`
    )
  })

  it(`should correctly serialize where clause param array to query params`, async () => {
    const eventTarget = new EventTarget()
    const requestedUrls: Array<string> = []
    const fetchWrapper = (
      ...args: Parameters<typeof fetch>
    ): Promise<Response> => {
      requestedUrls.push(args[0].toString())
      eventTarget.dispatchEvent(new Event(`fetch`))
      return Promise.resolve(Response.error())
    }

    const aborter = new AbortController()
    const stream = new ShapeStream({
      url: shapeUrl,
      params: {
        table: `foo`,
        where: `a=$1 and b=$2`,
        columns: [`id`],
        params: [`test1`, `test2`],
      },
      handle: `potato`,
      signal: aborter.signal,
      fetchClient: fetchWrapper,
    })

    const unsub = stream.subscribe(() => unsub())

    await new Promise((resolve) =>
      eventTarget.addEventListener(`fetch`, resolve, { once: true })
    )

    expect(requestedUrls[0].split(`?`)[1]).toEqual(
      `columns=%22id%22&handle=potato&log=full&offset=-1&params%5B1%5D=test1&params%5B2%5D=test2&table=foo&where=a%3D%241+and+b%3D%242`
    )
  })

  it(`should encode columns with columnMapper`, async () => {
    const eventTarget = new EventTarget()
    const requestedUrls: Array<string> = []
    const fetchWrapper = (
      ...args: Parameters<typeof fetch>
    ): Promise<Response> => {
      requestedUrls.push(args[0].toString())
      eventTarget.dispatchEvent(new Event(`fetch`))
      return Promise.resolve(Response.error())
    }

    const aborter = new AbortController()
    const stream = new ShapeStream({
      url: shapeUrl,
      params: {
        table: `foo`,
        columns: [`userId`, `createdAt`],
      },
      signal: aborter.signal,
      fetchClient: fetchWrapper,
      columnMapper: snakeCamelMapper(),
    })

    const unsub = stream.subscribe(() => unsub())

    await new Promise((resolve) =>
      eventTarget.addEventListener(`fetch`, resolve, { once: true })
    )

    const url = new URL(requestedUrls[0])
    // columns should be encoded from app format (camelCase) to db format (snake_case)
    // and quoted for safe serialization
    expect(url.searchParams.get(`columns`)).toEqual(`"user_id","created_at"`)
  })

  it(`should encode where clause with columnMapper`, async () => {
    const eventTarget = new EventTarget()
    const requestedUrls: Array<string> = []
    const fetchWrapper = (
      ...args: Parameters<typeof fetch>
    ): Promise<Response> => {
      requestedUrls.push(args[0].toString())
      eventTarget.dispatchEvent(new Event(`fetch`))
      return Promise.resolve(Response.error())
    }

    const aborter = new AbortController()
    const stream = new ShapeStream({
      url: shapeUrl,
      params: {
        table: `foo`,
        where: `userId = $1`,
      },
      signal: aborter.signal,
      fetchClient: fetchWrapper,
      columnMapper: snakeCamelMapper(),
    })

    const unsub = stream.subscribe(() => unsub())

    await new Promise((resolve) =>
      eventTarget.addEventListener(`fetch`, resolve, { once: true })
    )

    const url = new URL(requestedUrls[0])
    // where clause should be encoded from app format (camelCase) to db format (snake_case)
    expect(url.searchParams.get(`where`)).toEqual(`user_id = $1`)
  })

  it(`should quote columns even when columnMapper is not provided`, async () => {
    const eventTarget = new EventTarget()
    const requestedUrls: Array<string> = []
    const fetchWrapper = (
      ...args: Parameters<typeof fetch>
    ): Promise<Response> => {
      requestedUrls.push(args[0].toString())
      eventTarget.dispatchEvent(new Event(`fetch`))
      return Promise.resolve(Response.error())
    }

    const aborter = new AbortController()
    const stream = new ShapeStream({
      url: shapeUrl,
      params: {
        table: `foo`,
        columns: [`user_id`, `created_at`],
      },
      signal: aborter.signal,
      fetchClient: fetchWrapper,
    })

    const unsub = stream.subscribe(() => unsub())

    await new Promise((resolve) =>
      eventTarget.addEventListener(`fetch`, resolve, { once: true })
    )

    const url = new URL(requestedUrls[0])
    // columns should be quoted for safe serialization
    expect(url.searchParams.get(`columns`)).toEqual(`"user_id","created_at"`)
  })

  it(`should handle columns with special characters`, async () => {
    const eventTarget = new EventTarget()
    const requestedUrls: Array<string> = []
    const fetchWrapper = (
      ...args: Parameters<typeof fetch>
    ): Promise<Response> => {
      requestedUrls.push(args[0].toString())
      eventTarget.dispatchEvent(new Event(`fetch`))
      return Promise.resolve(Response.error())
    }

    const aborter = new AbortController()
    const stream = new ShapeStream({
      url: shapeUrl,
      params: {
        table: `foo`,
        columns: [`normal`, `has,comma`, `has"quote`],
      },
      signal: aborter.signal,
      fetchClient: fetchWrapper,
    })

    const unsub = stream.subscribe(() => unsub())

    await new Promise((resolve) =>
      eventTarget.addEventListener(`fetch`, resolve, { once: true })
    )

    const url = new URL(requestedUrls[0])
    // columns with special characters should be properly quoted and escaped
    expect(url.searchParams.get(`columns`)).toEqual(
      `"normal","has,comma","has""quote"`
    )
  })

  it(`should decode data columns with columnMapper`, async () => {
    const receivedMessages: Message<Row>[] = []

    // Mock response with db column names (snake_case)
    const mockResponseData = [
      {
        key: `"public"."test"/"1"`,
        value: { user_id: `123`, created_at: `2025-01-01` },
        headers: { operation: `insert` },
      },
      {
        headers: { control: `up-to-date` },
      },
    ]

    const fetchWrapper = (): Promise<Response> => {
      // Use resolveInMacrotask to prevent infinite microtask loops
      return resolveInMacrotask(
        new Response(JSON.stringify(mockResponseData), {
          status: 200,
          headers: {
            'content-type': `application/json`,
            'electric-handle': `test-handle`,
            'electric-offset': `0_0`,
            'electric-cursor': `1`,
            'electric-up-to-date': `true`,
            'electric-schema': JSON.stringify({
              user_id: { type: `text` },
              created_at: { type: `text` },
            }),
          },
        })
      )
    }

    const stream = new ShapeStream({
      url: shapeUrl,
      params: {
        table: `foo`,
        columns: [`userId`, `createdAt`],
      },
      signal: aborter.signal,
      fetchClient: fetchWrapper,
      columnMapper: snakeCamelMapper(),
    })

    const unsub = stream.subscribe((messages) => {
      receivedMessages.push(...messages)
    })

    // Wait for messages to be processed
    await new Promise((resolve) => setTimeout(resolve, 100))

    unsub()
    aborter.abort()

    // Find the change message
    const changeMessage = receivedMessages.find(isChangeMessage)
    expect(changeMessage).toBeDefined()

    // Verify column names were decoded from snake_case to camelCase
    expect(changeMessage!.value).toHaveProperty(`userId`)
    expect(changeMessage!.value).toHaveProperty(`createdAt`)
    expect((changeMessage!.value as Record<string, unknown>).userId).toBe(`123`)
    expect((changeMessage!.value as Record<string, unknown>).createdAt).toBe(
      `2025-01-01`
    )

    // Verify original db column names are not present
    expect(changeMessage!.value).not.toHaveProperty(`user_id`)
    expect(changeMessage!.value).not.toHaveProperty(`created_at`)
  })

  it(`should detect fast retry loops, clear caches, and eventually throw`, async () => {
    // Simulate a misconfigured proxy that always returns 409, causing a tight
    // retry loop that should trigger cache clearing and eventually an error.
    let requestCount = 0
    let caughtError: Error | null = null
    const warnSpy = vi.spyOn(console, `warn`).mockImplementation(() => {})

    const fetchMock = (
      ..._args: Parameters<typeof fetch>
    ): Promise<Response> => {
      requestCount++
      return Promise.resolve(
        new Response(`[]`, {
          status: 409,
          headers: {
            'content-type': `application/json`,
            'electric-handle': `handle-${requestCount}`,
          },
        })
      )
    }

    const stream = new ShapeStream({
      url: shapeUrl,
      params: { table: `test` },
      signal: aborter.signal,
      fetchClient: fetchMock,
      subscribe: false,
      onError: (error) => {
        caughtError = error
      },
    })

    stream.subscribe(() => {})

    await vi.waitFor(
      () => {
        expect(caughtError).not.toBe(null)
      },
      { timeout: 15_000 }
    )

    expect(caughtError!.message).toContain(`fast retry loop`)
    expect(caughtError!.message).toContain(`caches were cleared`)
    expect(caughtError!.message).toContain(`proxy`)
    expect(caughtError!.message).toContain(`troubleshooting`)

    expect(warnSpy).toHaveBeenCalledWith(
      `[Electric]`,
      expect.stringContaining(`Clearing client-side caches`),
      expect.any(Error)
    )

    warnSpy.mockRestore()
  })

  it(`should reset fast-loop state when onError triggers a retry`, async () => {
    // Verifies that fast-loop detection doesn't permanently block a stream
    // after onError returns retry options. The consecutive count must reset
    // so the retried stream gets a fresh chance to sync.
    let requestCount = 0
    let errorCount = 0
    let lastError: Error | null = null
    const warnSpy = vi.spyOn(console, `warn`).mockImplementation(() => {})

    const fetchMock = (
      ..._args: Parameters<typeof fetch>
    ): Promise<Response> => {
      requestCount++

      // After the first onError retry, return a successful response
      // to prove the stream actually gets a fresh chance
      if (errorCount >= 1) {
        return Promise.resolve(
          new Response(
            JSON.stringify([
              {
                key: `test-1`,
                value: { id: `1` },
                headers: {
                  operation: `insert`,
                  relation: [`public`, `test`],
                },
                offset: `0_0`,
              },
              {
                headers: { control: `up-to-date` },
                offset: `0_0`,
              },
            ]),
            {
              status: 200,
              headers: {
                'content-type': `application/json`,
                'electric-handle': `good-handle`,
                'electric-offset': `0_0`,
                'electric-schema': `{"id":{"type":"text"}}`,
              },
            }
          )
        )
      }

      // Return 409 to trigger fast-loop detection
      return Promise.resolve(
        new Response(`[]`, {
          status: 409,
          headers: {
            'content-type': `application/json`,
            'electric-handle': `handle-${requestCount}`,
          },
        })
      )
    }

    const stream = new ShapeStream({
      url: shapeUrl,
      params: { table: `test` },
      signal: aborter.signal,
      fetchClient: fetchMock,
      subscribe: false,
      onError: (error) => {
        errorCount++
        lastError = error
        // Return retry options — this should reset fast-loop state
        return { params: { table: `test` } }
      },
    })

    let gotData = false
    stream.subscribe((messages) => {
      if (messages.some((m) => `key` in m)) {
        gotData = true
      }
    })

    // The stream should: detect fast loop → throw → onError retries →
    // fast-loop state resets → successful sync with good data
    await vi.waitFor(
      () => {
        expect(gotData).toBe(true)
      },
      { timeout: 15_000 }
    )

    expect(lastError).not.toBe(null)
    expect(lastError!.message).toContain(`fast retry loop`)

    warnSpy.mockRestore()
  })

  it(`should not trigger fast-loop detection when offset advances rapidly`, async () => {
    // Normal rapid syncing with advancing offsets should never be flagged
    // as a fast loop, even if many requests happen within the detection window.
    let requestCount = 0
    const warnSpy = vi.spyOn(console, `warn`).mockImplementation(() => {})

    const fetchMock = (
      ..._args: Parameters<typeof fetch>
    ): Promise<Response> => {
      requestCount++
      const offset = `${requestCount}_0`

      // Return data pages with advancing offsets, then up-to-date
      if (requestCount <= 10) {
        return Promise.resolve(
          new Response(
            JSON.stringify([
              {
                key: `row-${requestCount}`,
                value: { id: `${requestCount}` },
                headers: {
                  operation: `insert`,
                  relation: [`public`, `test`],
                },
                offset,
              },
            ]),
            {
              status: 200,
              headers: {
                'content-type': `application/json`,
                'electric-handle': `my-handle`,
                'electric-offset': offset,
                'electric-schema': `{"id":{"type":"text"}}`,
              },
            }
          )
        )
      }

      // After 10 pages, return up-to-date
      return Promise.resolve(
        new Response(
          JSON.stringify([
            { headers: { control: `up-to-date` }, offset: `10_0` },
          ]),
          {
            status: 200,
            headers: {
              'content-type': `application/json`,
              'electric-handle': `my-handle`,
              'electric-offset': `10_0`,
              'electric-schema': `{"id":{"type":"text"}}`,
            },
          }
        )
      )
    }

    const stream = new ShapeStream({
      url: shapeUrl,
      params: { table: `test` },
      signal: aborter.signal,
      fetchClient: fetchMock,
      subscribe: false,
    })

    stream.subscribe(() => {})

    // Wait for the stream to reach up-to-date
    await vi.waitFor(
      () => {
        expect(stream.isUpToDate).toBe(true)
      },
      { timeout: 5_000 }
    )

    // Should have made many rapid requests without triggering fast-loop detection
    expect(requestCount).toBeGreaterThan(5)
    expect(warnSpy).not.toHaveBeenCalledWith(
      `[Electric]`,
      expect.stringContaining(`fast retry loop`),
      expect.any(Error)
    )

    warnSpy.mockRestore()
  })

  it(`should not trigger fast-loop detection during live polling`, async () => {
    // Once up-to-date, the stream enters live polling mode. Rapid live
    // requests must not be flagged as a fast loop.
    const liveAborter = new AbortController()
    let requestCount = 0
    const warnSpy = vi.spyOn(console, `warn`).mockImplementation(() => {})

    const fetchMock = async (
      ..._args: Parameters<typeof fetch>
    ): Promise<Response> => {
      requestCount++

      // Stop after enough cycles to prove no fast-loop detection
      if (requestCount >= 12) {
        liveAborter.abort()
      }

      // Yield to prevent the tight loop from starving the event loop
      await new Promise((r) => setTimeout(r, 1))

      // Always return up-to-date to keep the stream in live mode.
      // Include electric-cursor for live requests.
      return new Response(
        JSON.stringify([{ headers: { control: `up-to-date` }, offset: `0_0` }]),
        {
          status: 200,
          headers: {
            'content-type': `application/json`,
            'electric-handle': `my-handle`,
            'electric-offset': `0_0`,
            'electric-schema': `{"id":{"type":"text"}}`,
            'electric-cursor': `${requestCount}`,
          },
        }
      )
    }

    const stream = new ShapeStream({
      url: shapeUrl,
      params: { table: `test` },
      signal: liveAborter.signal,
      fetchClient: fetchMock,
      subscribe: true,
      onError: () => {},
    })

    stream.subscribe(() => {})

    // Wait for the stream to complete several live polling cycles then abort
    await vi.waitFor(
      () => {
        expect(requestCount).toBeGreaterThanOrEqual(10)
      },
      { timeout: 5_000 }
    )

    expect(warnSpy).not.toHaveBeenCalledWith(
      `[Electric]`,
      expect.stringContaining(`fast retry loop`),
      expect.any(Error)
    )

    warnSpy.mockRestore()
  })

  describe(`fast-loop detection with subset snapshots`, () => {
    const snapshotResponse = () =>
      new Response(
        JSON.stringify({
          metadata: {
            snapshot_mark: 1,
            xmin: `1`,
            xmax: `2`,
            xip_list: [],
            database_lsn: `0`,
          },
          data: [],
        }),
        {
          status: 200,
          headers: {
            'content-type': `application/json`,
            'electric-handle': `my-handle`,
            'electric-offset': `0_0`,
            'electric-schema': `{"id":{"type":"text"}}`,
          },
        }
      )

    const isSnapshotRequest = (input: Parameters<typeof fetch>[0]) =>
      new URL(input.toString()).searchParams.has(`subset__limit`)

    const warnedFastLoop = (warnSpy: { mock: { calls: unknown[][] } }) =>
      warnSpy.mock.calls.some((args) =>
        args.some((arg) => String(arg).includes(`fast retry loop`))
      )

    it(`does not count requests that a snapshot pause aborted`, async () => {
      // Each requestSnapshot pauses the stream, which aborts the in-flight
      // request, and the stream restarts at the same offset once the snapshot
      // is done. An aborted request got no response, so many snapshots in a
      // row are not a retry loop.
      const warnSpy = vi.spyOn(console, `warn`).mockImplementation(() => {})
      let caughtError: Error | null = null
      let mainRequestCount = 0

      const fetchMock = (
        input: Parameters<typeof fetch>[0],
        init?: Parameters<typeof fetch>[1]
      ): Promise<Response> => {
        if (isSnapshotRequest(input)) {
          return Promise.resolve(snapshotResponse())
        }
        mainRequestCount++
        return new Promise<Response>((_, reject) => {
          init?.signal?.addEventListener(`abort`, () => {
            reject(new DOMException(`Aborted`, `AbortError`))
          })
        })
      }

      const stream = new ShapeStream({
        url: shapeUrl,
        params: { table: `test` },
        log: `changes_only`,
        signal: aborter.signal,
        fetchClient: fetchMock,
        onError: (error) => {
          caughtError = error
        },
      })

      stream.subscribe(() => {})

      for (let i = 0; i < 30; i++) {
        await stream.requestSnapshot({ limit: 1 })
      }

      expect(warnedFastLoop(warnSpy)).toBe(false)
      expect(caughtError).toBe(null)
      // The stream is still running: it resumes once the last snapshot is done.
      const requestsBeforeResume = mainRequestCount
      await vi.waitFor(() => {
        expect(mainRequestCount).toBe(requestsBeforeResume + 1)
      })

      warnSpy.mockRestore()
    })

    it(`still detects a loop of responses when snapshots run in between`, async () => {
      let requestCount = 0
      let caughtError: Error | null = null
      const warnSpy = vi.spyOn(console, `warn`).mockImplementation(() => {})

      const fetchMock = (
        input: Parameters<typeof fetch>[0]
      ): Promise<Response> => {
        if (isSnapshotRequest(input)) {
          return Promise.resolve(snapshotResponse())
        }
        requestCount++
        return Promise.resolve(
          new Response(`[]`, {
            status: 409,
            headers: {
              'content-type': `application/json`,
              'electric-handle': `handle-${requestCount}`,
            },
          })
        )
      }

      const stream = new ShapeStream({
        url: shapeUrl,
        params: { table: `test` },
        log: `changes_only`,
        signal: aborter.signal,
        fetchClient: fetchMock,
        subscribe: false,
        onError: (error) => {
          caughtError = error
        },
      })

      stream.subscribe(() => {})

      for (let i = 0; i < 5; i++) {
        await stream.requestSnapshot({ limit: 1 })
      }

      await vi.waitFor(
        () => {
          expect(caughtError).not.toBe(null)
        },
        { timeout: 15_000 }
      )

      expect(caughtError!.message).toContain(`fast retry loop`)
      expect(warnedFastLoop(warnSpy)).toBe(true)

      warnSpy.mockRestore()
    })
  })

  it(`backs off before retrying when onError returns retry options`, async () => {
    const randomSpy = vi.spyOn(Math, `random`).mockReturnValue(0.5)
    const requestTimes: number[] = []

    const fetchMock = vi.fn(async () => {
      requestTimes.push(Date.now())
      return new Response(`Bad Request`, {
        status: 400,
        statusText: `Bad Request`,
      })
    })

    const stream = new ShapeStream({
      url: shapeUrl,
      params: { table: `test` },
      signal: aborter.signal,
      fetchClient: fetchMock,
      subscribe: true,
      backoffOptions: {
        initialDelay: 100,
        maxDelay: 100,
        multiplier: 1,
        maxRetries: Infinity,
      },
      onError: () => ({}),
    })

    stream.subscribe(() => {})

    await vi.waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2), {
      timeout: 500,
    })
    expect(requestTimes[1] - requestTimes[0]).toBeGreaterThanOrEqual(40)
    expect(requestTimes[1] - requestTimes[0]).toBeLessThan(200)

    aborter.abort()
    randomSpy.mockRestore()
  })

  it(`tears down promptly when aborted during onError backoff`, async () => {
    const randomSpy = vi.spyOn(Math, `random`).mockReturnValue(1)
    let requestCount = 0

    const fetchMock = vi.fn(async () => {
      requestCount++
      return new Response(`Bad Request`, {
        status: 400,
        statusText: `Bad Request`,
      })
    })

    const stream = new ShapeStream({
      url: shapeUrl,
      params: { table: `test` },
      signal: aborter.signal,
      fetchClient: fetchMock,
      subscribe: true,
      backoffOptions: {
        initialDelay: 10_000,
        maxDelay: 10_000,
        multiplier: 1,
        maxRetries: Infinity,
      },
      onError: () => ({}),
    })

    stream.subscribe(() => {})

    await vi.waitFor(() => expect(requestCount).toBe(1), { timeout: 500 })

    const abortTime = Date.now()
    aborter.abort()

    await vi.waitFor(() => expect(Date.now() - abortTime).toBeLessThan(500), {
      timeout: 1000,
    })
    expect(requestCount).toBe(1)

    randomSpy.mockRestore()
  })

  it(`onError retry loop should be bounded for persistent errors`, async () => {
    // Regression: onError always returning retry for a persistent error
    // caused an unbounded retry loop. The consecutive error retry limit
    // ensures the loop terminates.
    let requestCount = 0

    // First request succeeds → LiveState. All subsequent → persistent 400.
    const fetchMock = vi.fn(async () => {
      requestCount++

      if (requestCount === 1) {
        return new Response(
          JSON.stringify([
            { value: { id: 1 } },
            { headers: { control: `up-to-date` } },
          ]),
          {
            status: 200,
            headers: {
              'electric-handle': `test-handle`,
              'electric-offset': `0_0`,
              'electric-schema': `{"id":"int4"}`,
              'electric-up-to-date': ``,
            },
          }
        )
      }

      // 400 bypasses backoff, creating a tight retry loop
      return new Response(`Bad Request`, {
        status: 400,
        statusText: `Bad Request`,
      })
    })

    let lastError: Error | null = null
    const stream = new ShapeStream({
      url: shapeUrl,
      params: { table: `test` },
      signal: aborter.signal,
      fetchClient: fetchMock,
      subscribe: true,
      backoffOptions: {
        initialDelay: 2,
        maxDelay: 2,
        multiplier: 1,
        maxRetries: Infinity,
      },
      onError: (error) => {
        lastError = error
        return {} // always retry — simulates TanStack DB's shouldRetryOnFailure
      },
    })

    let subscriberError: Error | null = null
    stream.subscribe(
      () => {},
      (err) => {
        subscriberError = err
      }
    )

    // The retry loop is asynchronous recursion via microtasks, so it
    // completes well within this window.
    await new Promise((resolve) => setTimeout(resolve, 500))

    expect(lastError).not.toBeNull()
    expect(subscriberError).not.toBeNull()
    // 1 initial success + ~51 retries (limit fires at >50)
    expect(requestCount).toBeLessThan(100)
  })

  it(`onError retry counter resets after successful data`, async () => {
    // The consecutive error retry counter must reset when the stream
    // processes real data. Without the reset, intermittent error bursts
    // would accumulate across the stream's lifetime and eventually kill
    // a stream that is making progress between failures.

    let requestCount = 0
    const phase = { current: `errors1` } // errors1 → success1 → errors2 → done

    const fetchMock = vi.fn(
      async (_url: RequestInfo | URL, init?: RequestInit) => {
        // Yield to the event loop to prevent OOM from deeply nested
        // recursive await this.#start() with instant mock resolution
        await new Promise((r) => setTimeout(r, 0))
        if (init?.signal?.aborted) return Response.error()

        requestCount++

        if (phase.current === `errors1` && requestCount <= 30) {
          return new Response(`Bad Request`, {
            status: 400,
            statusText: `Bad Request`,
          })
        }
        if (phase.current === `errors1`) {
          phase.current = `success1`
        }
        if (phase.current === `success1`) {
          phase.current = `errors2`
          requestCount = 0
          return new Response(
            JSON.stringify([
              { value: { id: 1 } },
              { headers: { control: `up-to-date` } },
            ]),
            {
              status: 200,
              headers: {
                'electric-handle': `test-handle`,
                'electric-offset': `0_0`,
                'electric-schema': `{"id":"int4"}`,
                'electric-cursor': `cursor-1`,
                'electric-up-to-date': ``,
              },
            }
          )
        }
        if (phase.current === `errors2` && requestCount <= 30) {
          return new Response(`Bad Request`, {
            status: 400,
            statusText: `Bad Request`,
          })
        }
        // Both bursts survived — abort
        phase.current = `done`
        aborter.abort()
        return new Response(
          JSON.stringify([
            { value: { id: 2 } },
            { headers: { control: `up-to-date` } },
          ]),
          {
            status: 200,
            headers: {
              'electric-handle': `test-handle`,
              'electric-offset': `0_1`,
              'electric-schema': `{"id":"int4"}`,
              'electric-cursor': `cursor-2`,
              'electric-up-to-date': ``,
            },
          }
        )
      }
    )

    let subscriberError: Error | null = null
    const stream = new ShapeStream({
      url: shapeUrl,
      params: { table: `test` },
      signal: aborter.signal,
      fetchClient: fetchMock,
      subscribe: true,
      backoffOptions: {
        initialDelay: 2,
        maxDelay: 2,
        multiplier: 1,
        maxRetries: Infinity,
      },
      onError: () => ({}), // always retry
    })

    stream.subscribe(
      () => {},
      (err) => {
        subscriberError = err
      }
    )

    await vi.waitFor(
      () => {
        expect(phase.current).toBe(`done`)
      },
      { timeout: 10_000 }
    )

    // Stream survived 60 total errors (2 bursts of 30) because the
    // counter reset between bursts. Without the reset, the cumulative
    // count would hit 50 and kill the stream during the first burst.
    expect(subscriberError).toBeNull()
  })

  it(`204 No Content responses reset the consecutive error retry counter`, async () => {
    // 204 is a deprecated but supported "you're caught up" response that
    // returns an empty body. The retry counter must reset on 204 success,
    // not just on non-empty message batches.
    let requestCount = 0

    // Pattern: initial 200 → LiveState, then alternating bursts of
    // 10 errors and 204 successes, repeated 6 times.
    // Total errors: 60 (exceeds the 50 cap if counter doesn't reset).
    const fetchMock = vi.fn(async () => {
      await new Promise((r) => setTimeout(r, 0))
      requestCount++

      // First request: initial 200 to establish shape
      if (requestCount === 1) {
        return new Response(
          JSON.stringify([
            { value: { id: 1 } },
            { headers: { control: `up-to-date` } },
          ]),
          {
            status: 200,
            headers: {
              'electric-handle': `handle-204-test`,
              'electric-offset': `0_0`,
              'electric-schema': `{"id":"int4"}`,
              'electric-up-to-date': ``,
            },
          }
        )
      }

      // After initial success: cycle through 10 errors then 1 x 204, repeat
      const cyclePos = (requestCount - 2) % 11 // 0-9 = errors, 10 = 204
      if (cyclePos < 10) {
        return new Response(`Bad Request`, {
          status: 400,
          statusText: `Bad Request`,
        })
      }

      // 204 No Content — should reset counter
      return new Response(null, {
        status: 204,
        headers: {
          'electric-handle': `handle-204-test`,
          'electric-offset': `0_0`,
          'electric-schema': `{"id":"int4"}`,
          'electric-cursor': `cursor-${requestCount}`,
        },
      })
    })

    let subscriberError: Error | null = null
    const stream = new ShapeStream({
      url: shapeUrl,
      params: { table: `test` },
      signal: aborter.signal,
      fetchClient: fetchMock,
      subscribe: true,
      backoffOptions: {
        initialDelay: 2,
        maxDelay: 2,
        multiplier: 1,
        maxRetries: Infinity,
      },
      onError: () => ({}),
    })

    stream.subscribe(
      () => {},
      (err) => {
        subscriberError = err
      }
    )

    // Wait long enough for 60+ errors across 6 cycles
    await vi.waitFor(
      () => {
        expect(requestCount).toBeGreaterThan(60)
      },
      { timeout: 10_000 }
    )

    aborter.abort()

    // If counter resets on 204, the stream survives 60+ total errors.
    // If it doesn't, the counter hits 50 and tears down the stream.
    expect(subscriberError).toBeNull()
  })

  describe(`a batch that cannot be processed`, () => {
    const withBadLsn = (row: ReturnType<typeof insertRow>) => ({
      ...row,
      headers: { ...row.headers, lsn: `not-a-number` },
    })
    const upToDate = { headers: { control: `up-to-date` } }
    const fastBackoff = {
      initialDelay: 2,
      maxDelay: 2,
      multiplier: 1,
      maxRetries: Infinity,
    }

    it(`is asked for again from the same offset after an SSE batch fails`, async () => {
      const row = insertRow(1, `3_0`)
      const requests: URL[] = []
      const fetchMock = vi.fn((input: string | URL | Request) => {
        const url = new URL(input.toString())
        if (url.searchParams.get(`offset`) === `-1`) {
          return Promise.resolve(initialResponse())
        }
        requests.push(url)
        if (requests.length === 1) {
          const body = new ReadableStream<Uint8Array>({
            start(controller) {
              controller.enqueue(encodeEvent(withBadLsn(row)))
              controller.enqueue(encodeEvent(upToDate))
            },
          })
          return Promise.resolve(sseResponse(body, `3_0`))
        }
        if (requests.length === 2) {
          return Promise.resolve(upToDateResponse([row, upToDate], `3_0`))
        }
        return new Promise<Response>(() => {})
      })

      const received = startLiveSseStream(fetchMock)

      await vi.waitFor(() => {
        expect(received).toEqual([row.key])
      })
      expect(requests[1]?.searchParams.get(`offset`)).toBe(`0_0`)
    })

    it(`is asked for again from the same offset after an onError retry`, async () => {
      const row = insertRow(1, `3_0`)
      const requests: URL[] = []
      const fetchMock = vi.fn((input: string | URL | Request) => {
        const url = new URL(input.toString())
        if (url.searchParams.get(`offset`) === `-1`) {
          return Promise.resolve(initialResponse())
        }
        requests.push(url)
        if (requests.length === 1) {
          return Promise.resolve(
            upToDateResponse([withBadLsn(row), upToDate], `3_0`)
          )
        }
        if (requests.length === 2) {
          return Promise.resolve(upToDateResponse([row, upToDate], `3_0`))
        }
        return new Promise<Response>(() => {})
      })

      const stream = new ShapeStream({
        url: shapeUrl,
        params: { table: `test` },
        signal: aborter.signal,
        fetchClient: fetchMock,
        backoffOptions: fastBackoff,
        onError: () => ({}),
      })
      const received: Array<string> = []
      stream.subscribe((messages) => {
        for (const message of messages) {
          if (isChangeMessage(message)) received.push(message.key)
        }
      })

      await vi.waitFor(() => {
        expect(received).toEqual([row.key])
      })
      expect(requests[1]?.searchParams.get(`offset`)).toBe(`0_0`)
    })

    it(`stops the stream with an error when it stays bad`, async () => {
      const row = insertRow(1, `3_0`)
      const fetchMock = vi.fn(async (input: string | URL | Request) => {
        await resolveInMacrotask(undefined)
        const url = new URL(input.toString())
        return url.searchParams.get(`offset`) === `-1`
          ? initialResponse()
          : upToDateResponse([withBadLsn(row), upToDate], `3_0`)
      })

      const stream = new ShapeStream({
        url: shapeUrl,
        params: { table: `test` },
        signal: aborter.signal,
        fetchClient: fetchMock,
        backoffOptions: fastBackoff,
        onError: () => ({}),
      })
      const subscriberError = vi.fn()
      const received: Array<string> = []
      stream.subscribe((messages) => {
        for (const message of messages) {
          if (isChangeMessage(message)) received.push(message.key)
        }
      }, subscriberError)

      await vi.waitFor(
        () => {
          expect(subscriberError).toHaveBeenCalledOnce()
        },
        { timeout: 5_000 }
      )
      expect(received).toEqual([])
    })
  })

  it(`malformed 200 responses are bounded by the retry counter`, async () => {
    // A proxy/CDN returning 200 OK with invalid JSON (non-array body)
    // must still be bounded. The counter must NOT reset before the body
    // is successfully parsed, otherwise accepted headers + parse failure
    // creates an unbounded loop (counter resets to 0 every iteration).
    let requestCount = 0

    const fetchMock = vi.fn(
      async (_url: RequestInfo | URL, init?: RequestInit) => {
        await new Promise((r) => setTimeout(r, 0))
        if (init?.signal?.aborted) return Response.error()
        requestCount++

        // First request: valid 200 to establish shape
        if (requestCount === 1) {
          return new Response(
            JSON.stringify([
              { value: { id: 1 } },
              { headers: { control: `up-to-date` } },
            ]),
            {
              status: 200,
              headers: {
                'electric-handle': `handle-malformed-test`,
                'electric-offset': `0_0`,
                'electric-schema': `{"id":"int4"}`,
                'electric-up-to-date': ``,
              },
            }
          )
        }

        // All subsequent: 200 OK with valid headers but non-array body
        return new Response(`{"error": "not an array"}`, {
          status: 200,
          headers: {
            'electric-handle': `handle-malformed-test`,
            'electric-offset': `0_0`,
            'electric-schema': `{"id":"int4"}`,
            'electric-cursor': `cursor-${requestCount}`,
          },
        })
      }
    )

    let subscriberError: Error | null = null
    const stream = new ShapeStream({
      url: shapeUrl,
      params: { table: `test` },
      signal: aborter.signal,
      fetchClient: fetchMock,
      subscribe: true,
      backoffOptions: {
        initialDelay: 2,
        maxDelay: 2,
        multiplier: 1,
        maxRetries: Infinity,
      },
      onError: () => ({}),
    })

    stream.subscribe(
      () => {},
      (err) => {
        subscriberError = err
      }
    )

    await new Promise((resolve) => setTimeout(resolve, 2000))

    // The retry counter must eventually exhaust and tear down the stream.
    // If the counter resets on accepted headers (before parse), this
    // assertion fails because the stream loops forever.
    expect(subscriberError).not.toBeNull()
    expect(requestCount).toBeLessThan(200)
  })

  describe(`requestSnapshot offset coordination`, () => {
    const change = (
      id: string,
      status: string,
      txid?: number
    ): ChangeMessage => ({
      key: `run-${id}`,
      value: { id, status },
      headers: {
        operation: txid === undefined ? `insert` : `update`,
        relation: [`public`, `runs`],
        ...(txid === undefined ? {} : { txids: [txid] }),
      },
    })

    const response = (body: unknown, offset: Offset, snapshot = false) =>
      new Response(JSON.stringify(body), {
        headers: {
          'content-type': `application/json`,
          'electric-handle': `handle-1`,
          'electric-offset': offset,
          'electric-schema': `{"id":{"type":"text"},"status":{"type":"text"}}`,
          ...(snapshot
            ? { 'electric-snapshot': `true` }
            : { 'electric-up-to-date': `` }),
        },
      })

    const logResponse = (offset: Offset, ...data: ChangeMessage[]) =>
      response(
        [...data, { headers: { control: `up-to-date` }, offset }],
        offset
      )

    const snapshotResponse = (
      mark: number,
      offset: Offset,
      ...data: ChangeMessage[]
    ) =>
      response(
        {
          metadata: {
            snapshot_mark: mark,
            xmin: `1`,
            xmax: `10`,
            xip_list: [],
            database_lsn: offset.split(`_`)[0],
          },
          data,
        },
        offset,
        true
      )

    function deferred<T>() {
      let resolve!: (value: T) => void
      let reject!: (reason: unknown) => void
      const promise = new Promise<T>((res, rej) => {
        resolve = res
        reject = rej
      })
      return { promise, resolve, reject }
    }

    // Explicit request/response barriers keep the race independent of timing.
    function holdRequest() {
      const received = deferred<URL>()
      const reply = deferred<Response>()
      return {
        received: received.promise,
        respond: reply.resolve,
        fetch: (url: URL, init?: RequestInit) => {
          received.resolve(url)
          const abort = () =>
            reply.reject(new DOMException(`Aborted`, `AbortError`))
          if (init?.signal?.aborted) abort()
          else init?.signal?.addEventListener(`abort`, abort, { once: true })
          return reply.promise.finally(() =>
            init?.signal?.removeEventListener(`abort`, abort)
          )
        },
      }
    }

    function createStream(
      offset: Offset,
      onError?: ShapeStreamOptions[`onError`]
    ) {
      const pendingFetch = (url: URL, init?: RequestInit) =>
        holdRequest().fetch(url, init)
      const logFetch = vi.fn(pendingFetch)
      const snapshotFetch = vi.fn(pendingFetch)
      const seenChanges: string[] = []
      const stream = new ShapeStream({
        url: shapeUrl,
        params: { table: `runs` },
        log: `changes_only`,
        offset,
        ...(offset === `now` || offset === `-1` ? {} : { handle: `handle-1` }),
        liveSse: false,
        signal: aborter.signal,
        onError,
        fetchClient: (input, init) => {
          const url = new URL(input.toString())
          return url.searchParams.has(`subset__limit`)
            ? snapshotFetch(url, init)
            : logFetch(url, init)
        },
      })
      stream.subscribe((messages) => {
        for (const message of messages) {
          if (isChangeMessage(message)) {
            seenChanges.push(`${message.key}:${message.value.status}`)
          }
        }
      })
      return { stream, logFetch, snapshotFetch, seenChanges }
    }

    it(`replays changes outside the subset and deduplicates changes inside it`, async () => {
      const { stream, logFetch, snapshotFetch, seenChanges } =
        createStream(`1_0`)
      const live = holdRequest()
      logFetch.mockResolvedValueOnce(logResponse(`1_0`, change(`a`, `running`)))
      logFetch.mockImplementationOnce(live.fetch)
      await live.received

      const snapshot = holdRequest()
      snapshotFetch.mockImplementationOnce(snapshot.fetch)
      const loading = stream.requestSnapshot({ where: `id = 'b'`, limit: 1 })
      await snapshot.received

      // A completes while B's snapshot is in flight. B's older update will
      // also be replayed, but must not overwrite its newer snapshot value.
      const replay = holdRequest()
      logFetch.mockImplementationOnce(replay.fetch)
      snapshot.respond(snapshotResponse(1, `5_0`, change(`b`, `completed`)))
      await loading
      expect((await replay.received).searchParams.get(`offset`)).toBe(`1_0`)
      replay.respond(
        logResponse(
          `5_0`,
          change(`a`, `completed`, 2),
          change(`b`, `running`, 3)
        )
      )

      await vi.waitFor(() =>
        expect(seenChanges).toEqual([
          `run-a:running`,
          `run-b:completed`,
          `run-a:completed`,
        ])
      )
    })

    it(`keeps deduplicating a batch that failed after retiring a snapshot`, async () => {
      const withLsn = (message: ChangeMessage, lsn: string): ChangeMessage => ({
        ...message,
        headers: { ...message.headers, lsn },
      })
      const { stream, logFetch, snapshotFetch, seenChanges } = createStream(
        `1_0`,
        () => ({})
      )
      const live = holdRequest()
      logFetch.mockResolvedValueOnce(logResponse(`1_0`, change(`a`, `running`)))
      logFetch.mockImplementationOnce(live.fetch)
      await live.received

      const snapshot = holdRequest()
      snapshotFetch.mockImplementationOnce(snapshot.fetch)
      const loading = stream.requestSnapshot({ where: `id = 'b'`, limit: 1 })
      await snapshot.received

      const replay = holdRequest()
      logFetch.mockImplementationOnce(replay.fetch)
      snapshot.respond(snapshotResponse(1, `5_0`, change(`b`, `completed`)))
      await loading
      await replay.received

      const covered = change(`b`, `running`, 3)
      const retiring = withLsn(change(`c`, `running`), `7`)
      const retry = holdRequest()
      logFetch.mockImplementationOnce(retry.fetch)
      replay.respond(
        logResponse(
          `8_0`,
          covered,
          retiring,
          withLsn(change(`d`, `running`), `not-a-number`)
        )
      )

      expect((await retry.received).searchParams.get(`offset`)).toBe(`1_0`)
      const retried = logResponse(
        `8_0`,
        covered,
        retiring,
        withLsn(change(`d`, `running`), `8`)
      )
      retried.headers.set(`electric-cursor`, `cursor-1`)
      retry.respond(retried)

      await vi.waitFor(() =>
        expect(seenChanges).toEqual([
          `run-a:running`,
          `run-b:completed`,
          `run-c:running`,
          `run-d:running`,
        ])
      )
    })

    it.each([`now`, `-1`] as const)(
      `adopts the snapshot position when the stream starts at %s`,
      async (initialOffset) => {
        const { stream, logFetch, snapshotFetch, seenChanges } =
          createStream(initialOffset)
        const snapshot = holdRequest()
        snapshotFetch.mockImplementationOnce(snapshot.fetch)
        const loading = stream.requestSnapshot({ limit: 1 })
        await snapshot.received

        const replay = holdRequest()
        logFetch.mockImplementationOnce(replay.fetch)
        snapshot.respond(snapshotResponse(1, `5_0`, change(`a`, `running`)))
        await loading
        const resumeUrl = await replay.received
        expect(resumeUrl.searchParams.get(`offset`)).toBe(`5_0`)
        expect(resumeUrl.searchParams.get(`handle`)).toBe(`handle-1`)
        replay.respond(logResponse(`6_0`, change(`a`, `completed`, 11)))

        await vi.waitFor(() =>
          expect(seenChanges).toEqual([`run-a:running`, `run-a:completed`])
        )
      }
    )

    it.each([`earlier`, `later`] as const)(
      `replays from the earliest cold snapshot when %s completes first`,
      async (first) => {
        const { stream, logFetch, snapshotFetch, seenChanges } =
          createStream(`now`)
        const earlier = holdRequest()
        const later = holdRequest()
        snapshotFetch
          .mockImplementationOnce(earlier.fetch)
          .mockImplementationOnce(later.fetch)
        const earlierLoad = stream.requestSnapshot({
          where: `id = 'a'`,
          limit: 1,
        })
        const laterLoad = stream.requestSnapshot({
          where: `id = 'b'`,
          limit: 1,
        })
        await Promise.all([earlier.received, later.received])

        const completions = [
          {
            request: earlier,
            load: earlierLoad,
            response: snapshotResponse(1, `3_0`, change(`a`, `running`)),
          },
          {
            request: later,
            load: laterLoad,
            response: snapshotResponse(2, `5_0`, change(`b`, `running`)),
          },
        ]
        if (first === `later`) completions.reverse()
        const replay = holdRequest()
        logFetch.mockImplementationOnce(replay.fetch)
        completions[0].request.respond(completions[0].response)
        await completions[0].load
        expect(stream.isPaused()).toBe(true)
        completions[1].request.respond(completions[1].response)
        await completions[1].load

        expect((await replay.received).searchParams.get(`offset`)).toBe(`3_0`)
        replay.respond(logResponse(`5_0`, change(`a`, `completed`, 11)))
        const snapshots =
          first === `earlier`
            ? [`run-a:running`, `run-b:running`]
            : [`run-b:running`, `run-a:running`]
        await vi.waitFor(() =>
          expect(seenChanges).toEqual([...snapshots, `run-a:completed`])
        )
      }
    )

    it.each([
      // First completion, second completion, expected resume offset.
      [`7_2`, `7_10`, `7_2`],
      [`7_10`, `7_2`, `7_2`],
      [`9007199254740992_0`, `9007199254740993_0`, `9007199254740992_0`],
      [`9007199254740993_0`, `9007199254740992_0`, `9007199254740992_0`],
      [`5_0`, `5_0`, `5_0`],
      [`0_inf`, `0_inf`, `0_inf`],
      [`0_2`, `0_inf`, `0_2`],
      [`0_inf`, `0_2`, `0_2`],
      [`0_inf`, `1_0`, `0_inf`],
      [`1_0`, `0_inf`, `0_inf`],
    ] as const)(
      `cold snapshots at %s then %s resume at %s`,
      async (first, second, expected) => {
        const { stream, logFetch, snapshotFetch } = createStream(`now`)
        const firstRequest = holdRequest()
        const secondRequest = holdRequest()
        snapshotFetch
          .mockImplementationOnce(firstRequest.fetch)
          .mockImplementationOnce(secondRequest.fetch)
        const firstLoad = stream.requestSnapshot({ limit: 1 })
        const secondLoad = stream.requestSnapshot({ limit: 2 })
        await Promise.all([firstRequest.received, secondRequest.received])

        const replay = holdRequest()
        logFetch.mockImplementationOnce(replay.fetch)
        firstRequest.respond(snapshotResponse(1, first))
        await firstLoad
        secondRequest.respond(snapshotResponse(2, second))
        await secondLoad
        expect((await replay.received).searchParams.get(`offset`)).toBe(
          expected
        )
      }
    )

    it(`resumes a live stream on the new handle when a snapshot hits a 409`, async () => {
      // The 409 moves the stream to the new handle. Its old offset belongs to
      // the old handle, so the snapshot's earlier offset replaces it.
      const { stream, logFetch, snapshotFetch } = createStream(`10_0`)
      const conflict = holdRequest()
      const retried = holdRequest()
      snapshotFetch
        .mockImplementationOnce(conflict.fetch)
        .mockImplementationOnce(retried.fetch)
      const loading = stream.requestSnapshot({ limit: 1 })
      await conflict.received

      conflict.respond(
        new Response(`[{"headers":{"control":"must-refetch"}}]`, {
          status: 409,
          headers: {
            'content-type': `application/json`,
            'electric-handle': `handle-2`,
          },
        })
      )
      await retried.received

      const replay = holdRequest()
      logFetch.mockImplementationOnce(replay.fetch)
      const retriedResponse = snapshotResponse(1, `0_inf`)
      retried.respond(
        new Response(retriedResponse.body, {
          headers: {
            ...Object.fromEntries(retriedResponse.headers),
            'electric-handle': `handle-2`,
          },
        })
      )
      await loading

      const resumeUrl = await replay.received
      expect(resumeUrl.searchParams.get(`handle`)).toBe(`handle-2`)
      expect(resumeUrl.searchParams.get(`offset`)).toBe(`0_inf`)
    })
  })

  describe(`parse failure and retry regressions`, () => {
    it(`rolls the offset back when a live chunk body fails to parse, so the retry re-requests it`, async () => {
      const requestedOffsets: string[] = []
      let requestCount = 0

      const fetchMock = vi.fn(
        async (url: RequestInfo | URL, init?: RequestInit) => {
          await new Promise((r) => setTimeout(r, 0))
          if (init?.signal?.aborted) return Response.error()
          requestedOffsets.push(
            new URL(url.toString()).searchParams.get(`offset`) ?? ``
          )
          requestCount++

          // Request 1: establish the shape and reach up-to-date at offset 0_0.
          if (requestCount === 1) {
            return new Response(
              JSON.stringify([
                {
                  key: `k1`,
                  value: { id: 1 },
                  headers: { operation: `insert` },
                  offset: `0_0`,
                },
                { headers: { control: `up-to-date` }, offset: `0_0` },
              ]),
              {
                status: 200,
                headers: {
                  'electric-handle': `handle-1`,
                  'electric-offset': `0_0`,
                  'electric-schema': `{"id":"int4"}`,
                  'electric-up-to-date': ``,
                },
              }
            )
          }

          // Request 2: a live chunk whose offset header advances to 5_0 but
          // whose body is a non-array (proxy/CDN corruption). Parsing throws
          // after the header offset was already committed by #onInitialResponse.
          if (requestCount === 2) {
            return new Response(`{"corrupted":"not an array"}`, {
              status: 200,
              headers: {
                'electric-handle': `handle-1`,
                'electric-offset': `5_0`,
                'electric-schema': `{"id":"int4"}`,
                'electric-cursor': `cursor-2`,
              },
            })
          }

          // Request 3+: hang until aborted so the retry's offset can be read.
          return new Promise<Response>((_resolve, reject) => {
            init?.signal?.addEventListener(
              `abort`,
              () => reject(new Error(`AbortError`)),
              { once: true }
            )
          })
        }
      )

      const stream = new ShapeStream({
        url: shapeUrl,
        params: { table: `test` },
        signal: aborter.signal,
        fetchClient: fetchMock,
        backoffOptions: {
          initialDelay: 2,
          maxDelay: 2,
          multiplier: 1,
          maxRetries: Infinity,
        },
        onError: () => ({}),
      })
      stream.subscribe(
        () => {},
        () => {}
      )

      await vi.waitFor(() => expect(stream.isUpToDate).toBe(true))
      expect(stream.lastOffset).toBe(`0_0`)

      await vi.waitFor(() => expect(requestCount).toBeGreaterThanOrEqual(3))

      // The retry must re-request the chunk at 0_0, not resume from the advanced
      // 5_0 header — stepping past an unparsed body silently drops those rows.
      expect(requestedOffsets[2]).toBe(`0_0`)
      expect(requestedOffsets).not.toContain(`5_0`)
      expect(stream.lastOffset).toBe(`0_0`)
    })

    it(`does not advance a warm stream's offset to a subset snapshot's position`, async () => {
      let mainRequestCount = 0

      const fetchMock = vi.fn(
        async (url: RequestInfo | URL, init?: RequestInit) => {
          const requestUrl = new URL(url.toString())

          // Subset snapshot: offset 99_0 sits far ahead of the stream's
          // established position.
          if (requestUrl.searchParams.has(`subset__limit`)) {
            return new Response(
              JSON.stringify({
                metadata: {
                  snapshot_mark: 1,
                  xmin: `1`,
                  xmax: `2`,
                  xip_list: [],
                  database_lsn: `0`,
                },
                data: [
                  {
                    key: `snap-1`,
                    value: { id: `1` },
                    headers: { operation: `insert` },
                  },
                ],
              }),
              {
                status: 200,
                headers: {
                  'content-type': `application/json`,
                  'electric-handle': `handle-1`,
                  'electric-offset': `99_0`,
                  'electric-schema': `{"id":{"type":"text"}}`,
                },
              }
            )
          }

          await new Promise((r) => setTimeout(r, 0))
          mainRequestCount++

          // Request 1: reach up-to-date at offset 0_5 (an established position).
          if (mainRequestCount === 1) {
            return new Response(
              JSON.stringify([
                { headers: { control: `up-to-date` }, offset: `0_5` },
              ]),
              {
                status: 200,
                headers: {
                  'content-type': `application/json`,
                  'electric-handle': `handle-1`,
                  'electric-offset': `0_5`,
                  'electric-schema': `{"id":{"type":"text"}}`,
                  'electric-up-to-date': ``,
                },
              }
            )
          }

          // Later live requests hang until aborted (pause lock or teardown).
          return new Promise<Response>((_resolve, reject) => {
            init?.signal?.addEventListener(
              `abort`,
              () => reject(new Error(`AbortError`)),
              { once: true }
            )
          })
        }
      )

      const stream = new ShapeStream({
        url: shapeUrl,
        params: { table: `test` },
        log: `changes_only`,
        signal: aborter.signal,
        fetchClient: fetchMock,
      })
      stream.subscribe(() => {})

      await vi.waitFor(() => expect(stream.isUpToDate).toBe(true))
      expect(stream.lastOffset).toBe(`0_5`)

      await stream.requestSnapshot({ limit: 1, orderBy: `id ASC` })

      // The snapshot must not drag the stream past updates to rows an earlier
      // subset already loaded.
      expect(stream.lastOffset).toBe(`0_5`)
    })

    it(`passes the stream's abort signal to snapshot fetches`, async () => {
      let snapshotSeen = false
      let capturedSnapshotSignal: AbortSignal | null = null

      const fetchMock = vi.fn(
        async (url: RequestInfo | URL, init?: RequestInit) => {
          const requestUrl = new URL(url.toString())

          if (requestUrl.searchParams.has(`subset__limit`)) {
            snapshotSeen = true
            capturedSnapshotSignal = init?.signal ?? null
            // Model a hung snapshot request: it settles only when its signal
            // aborts, so the test can observe whether the stream's abort reaches it.
            return await new Promise<Response>((_resolve, reject) => {
              init?.signal?.addEventListener(`abort`, () =>
                reject(new DOMException(`Aborted`, `AbortError`))
              )
            })
          }

          await new Promise((r) => setTimeout(r, 0))
          return new Response(
            JSON.stringify([
              { headers: { control: `up-to-date` }, offset: `0_0` },
            ]),
            {
              status: 200,
              headers: {
                'content-type': `application/json`,
                'electric-handle': `handle-1`,
                'electric-offset': `0_0`,
                'electric-schema': `{"id":{"type":"text"}}`,
                'electric-up-to-date': ``,
              },
            }
          )
        }
      )

      const stream = new ShapeStream({
        url: shapeUrl,
        params: { table: `test` },
        log: `changes_only`,
        signal: aborter.signal,
        fetchClient: fetchMock,
      })

      const snapshotPromise = stream
        .requestSnapshot({ limit: 1 })
        .catch(() => {})

      // Wait until the snapshot fetch is in flight, then abort the stream.
      // Without the stream signal the snapshot fetch (and its default-infinite
      // retry loop) keeps running and holds the pause lock; with it, aborting the
      // stream aborts the in-flight snapshot fetch. The fetch pipeline wraps the
      // passed signal, so assert propagation, not reference identity.
      await vi.waitFor(() => expect(snapshotSeen).toBe(true))
      expect(capturedSnapshotSignal).not.toBeNull()
      expect(capturedSnapshotSignal!.aborted).toBe(false)
      aborter.abort()
      expect(capturedSnapshotSignal!.aborted).toBe(true)
      await snapshotPromise
    })
  })

  const nextError = (stream: ShapeStream) =>
    new Promise<Error>((resolve) => {
      stream.subscribe(() => {}, resolve)
    })

  describe(`response body read failures`, () => {
    const backoffOptions = {
      initialDelay: 2,
      maxDelay: 2,
      multiplier: 1,
      maxRetries: Infinity,
    }

    // A request that stays open until the stream aborts it.
    const hangUntilAborted = (init?: RequestInit) =>
      new Promise<Response>((_resolve, reject) => {
        init?.signal?.addEventListener(
          `abort`,
          () => reject(new Error(`AbortError`)),
          { once: true }
        )
      })

    // A response whose headers arrive but whose body errors after the first
    // bytes, like a connection dropped mid-body.
    function cutOffResponse(headers: Record<string, string>): Response {
      const body = new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(new TextEncoder().encode(`[`))
          controller.error(new TypeError(`terminated`))
        },
      })
      return new Response(body, { status: 200, headers })
    }

    const upToDateRows = (id: number) =>
      JSON.stringify([
        { key: `k${id}`, value: { id }, headers: { operation: `insert` } },
        { headers: { control: `up-to-date` } },
      ])

    const initialHeaders = {
      'electric-handle': `handle-1`,
      'electric-offset': `0_0`,
      'electric-schema': `{"id":"int4"}`,
      'electric-up-to-date': ``,
    }
    const liveHeaders = (cursor: string) => ({
      ...initialHeaders,
      'electric-offset': `1_0`,
      'electric-cursor': cursor,
    })

    it(`retries an initial request whose body is cut off, without ending the stream`, async () => {
      const requestedOffsets: Array<string | null> = []
      let requestCount = 0
      const onFailedAttempt = vi.fn()

      const stream = new ShapeStream({
        url: shapeUrl,
        params: { table: `test` },
        signal: aborter.signal,
        backoffOptions: { ...backoffOptions, onFailedAttempt },
        fetchClient: async (url, init) => {
          requestCount++
          requestedOffsets.push(
            new URL(url.toString()).searchParams.get(`offset`)
          )
          if (requestCount === 1) return cutOffResponse(initialHeaders)
          if (requestCount === 2) {
            return new Response(upToDateRows(1), {
              status: 200,
              headers: initialHeaders,
            })
          }
          return hangUntilAborted(init)
        },
      })

      const messages: Array<Message<Row>> = []
      const errors: Array<Error> = []
      stream.subscribe(
        (batch) => {
          messages.push(...batch)
        },
        (err) => errors.push(err)
      )

      await vi.waitFor(() => expect(stream.isUpToDate).toBe(true))

      expect(errors).toEqual([])
      expect(stream.error).toBeNull()
      expect(messages.filter(isChangeMessage)).toHaveLength(1)
      // The retry asks for the same chunk again, from the same offset.
      expect(requestedOffsets.slice(0, 2)).toEqual([`-1`, `-1`])
      expect(onFailedAttempt).toHaveBeenCalledTimes(1)
    })

    it(`retries a live request whose body is cut off and keeps receiving updates`, async () => {
      const requests: Array<{ live: boolean; offset: string | null }> = []
      let liveCount = 0
      let requestCount = 0

      const stream = new ShapeStream({
        url: shapeUrl,
        params: { table: `test` },
        signal: aborter.signal,
        backoffOptions,
        fetchClient: async (url, init) => {
          requestCount++
          const params = new URL(url.toString()).searchParams
          const live = params.get(`live`) === `true`
          requests.push({ live, offset: params.get(`offset`) })

          if (!live) {
            return new Response(upToDateRows(1), {
              status: 200,
              headers: initialHeaders,
            })
          }
          liveCount++
          if (liveCount === 1) return cutOffResponse(liveHeaders(`c1`))
          if (liveCount === 2) {
            return new Response(upToDateRows(2), {
              status: 200,
              headers: liveHeaders(`c2`),
            })
          }
          return hangUntilAborted(init)
        },
      })

      const ids: Array<unknown> = []
      const errors: Array<Error> = []
      stream.subscribe(
        (batch) => {
          for (const m of batch) if (isChangeMessage(m)) ids.push(m.value.id)
        },
        (err) => errors.push(err)
      )

      await vi.waitFor(() => expect(ids).toEqual([1, 2]))

      expect(errors).toEqual([])
      expect(stream.error).toBeNull()
      // Both live attempts used the offset the stream had before the cut-off.
      const liveRequests = requests.filter((r) => r.live)
      expect(liveRequests[0].offset).toBe(`0_0`)
      expect(liveRequests[1].offset).toBe(`0_0`)
      expect(requestCount).toBeGreaterThanOrEqual(3)
    })

    it(`reports the error once the retries are exhausted`, async () => {
      let requestCount = 0
      const stream = new ShapeStream({
        url: shapeUrl,
        params: { table: `test` },
        signal: aborter.signal,
        backoffOptions: { ...backoffOptions, maxRetries: 2 },
        fetchClient: async () => {
          requestCount++
          return cutOffResponse(initialHeaders)
        },
      })

      const error = await nextError(stream)
      expect(error).toBeInstanceOf(FetchError)
      expect((error as FetchError).status).toBe(200)
      // The first attempt plus two retries.
      expect(requestCount).toBe(3)
    })

    it(`counts failed requests and cut-off bodies against one retry budget`, async () => {
      let requestCount = 0
      const stream = new ShapeStream({
        url: shapeUrl,
        params: { table: `test` },
        signal: aborter.signal,
        backoffOptions: { ...backoffOptions, maxRetries: 2 },
        fetchClient: async () => {
          requestCount++
          // Two network failures, then a body that is cut off, over and over.
          if (requestCount % 3 !== 0) throw new TypeError(`fetch failed`)
          return cutOffResponse(initialHeaders)
        },
      })

      const error = await nextError(stream)

      expect(error).toBeInstanceOf(FetchError)
      expect((error as FetchError).status).toBe(200)
      expect(requestCount).toBe(3)
    })

    it(`counts the retries of a prefetched chunk against the budget of its body`, async () => {
      const requestedOffsets: Array<string | null> = []
      let secondChunkRequests = 0
      const stream = new ShapeStream({
        url: shapeUrl,
        params: { table: `test` },
        signal: aborter.signal,
        backoffOptions: { ...backoffOptions, maxRetries: 1 },
        fetchClient: async (url) => {
          const offset = new URL(url.toString()).searchParams.get(`offset`)
          requestedOffsets.push(offset)
          if (offset === `-1`) {
            // Not up to date, so the chunk after it is prefetched.
            return new Response(`[]`, {
              status: 200,
              headers: {
                'electric-handle': `handle-1`,
                'electric-offset': `0_1`,
                'electric-schema': `{"id":"int4"}`,
              },
            })
          }
          // The prefetched chunk fails before its headers, then its body is
          // cut off.
          secondChunkRequests++
          if (secondChunkRequests === 1) throw new TypeError(`fetch failed`)
          return cutOffResponse({ ...initialHeaders, 'electric-offset': `0_2` })
        },
      })

      const error = await nextError(stream)
      expect(error).toBeInstanceOf(FetchError)
      expect((error as FetchError).status).toBe(200)

      // Several backoff periods later there is still no third request for the
      // chunk: the one retry of the budget went to the failed prefetch.
      await new Promise((resolve) => setTimeout(resolve, 100))
      expect(requestedOffsets).toEqual([`-1`, `0_1`, `0_1`])
    })

    it(`ends the stream, without a retry, when a fetchClient already read the body`, async () => {
      // A body that was consumed before the client reads it can never be read,
      // so retrying it forever (maxRetries is Infinity by default) would hang.
      let requestCount = 0
      const onError = vi.fn()
      const stream = new ShapeStream({
        url: shapeUrl,
        params: { table: `test` },
        signal: aborter.signal,
        backoffOptions,
        onError,
        fetchClient: async () => {
          requestCount++
          const res = new Response(upToDateRows(1), {
            status: 200,
            headers: initialHeaders,
          })
          await res.text()
          return res
        },
      })

      const error = await nextError(stream)

      expect(error).toBeInstanceOf(FetchError)
      expect((error as FetchError).status).toBe(200)
      expect(error.message).toContain(`Body is unusable`)
      expect(onError).toHaveBeenCalledTimes(1)
      expect(stream.isConnected()).toBe(false)

      // Several backoff periods later there is still only the one request.
      await new Promise((resolve) => setTimeout(resolve, 100))
      expect(requestCount).toBe(1)
    })
  })

  describe(`non-Error failures`, () => {
    // maxRetries: 0 makes the fetch layer give up on the first rejection, so the
    // raw rejection value reaches the stream's own error handling.
    const giveUpImmediately = {
      initialDelay: 1,
      maxDelay: 1,
      multiplier: 1,
      maxRetries: 0,
    }

    function streamRejectingWith(
      reason: unknown,
      extra: Partial<ShapeStreamOptions> = {}
    ) {
      return new ShapeStream({
        url: shapeUrl,
        params: { table: `test` },
        signal: aborter.signal,
        backoffOptions: giveUpImmediately,
        fetchClient: () => Promise.reject(reason),
        ...extra,
      })
    }

    it.each([
      [`a string`, `connection reset`],
      [`a plain object`, { code: `E_RESET` }],
    ])(
      `tells subscribers when the stream stops on %s and there is no onError`,
      async (_, reason) => {
        const stream = streamRejectingWith(reason)

        const error = await nextError(stream)

        expect(error).toBeInstanceOf(Error)
        expect(error.cause).toBe(reason)
        expect(stream.error).toBe(reason)
      }
    )

    it(`keeps the message of a thrown string`, async () => {
      const stream = streamRejectingWith(`connection reset`)

      const error = await nextError(stream)

      expect(error.message).toBe(`connection reset`)
    })

    it(`tells subscribers when onError declines to retry`, async () => {
      const reason = { code: `E_RESET` }
      const onError = vi.fn()
      const stream = streamRejectingWith(reason, { onError })

      const error = await nextError(stream)

      expect(error.cause).toBe(reason)
      expect(onError).toHaveBeenCalledWith(reason)
    })

    it(`tells subscribers when the onError retries are exhausted`, async () => {
      const warnSpy = vi.spyOn(console, `warn`).mockImplementation(() => {})
      const reason = `still failing`
      const stream = streamRejectingWith(reason, { onError: () => ({}) })

      try {
        const error = await nextError(stream)

        expect(error.cause).toBe(reason)
        expect(warnSpy).toHaveBeenCalledWith(
          `[Electric]`,
          expect.stringContaining(`retry loop exhausted`),
          expect.any(Error)
        )
      } finally {
        warnSpy.mockRestore()
      }
    })

    it(`passes an Error through to subscribers unchanged`, async () => {
      const reason = new Error(`boom`)
      const stream = streamRejectingWith(reason)

      const error = await nextError(stream)

      expect(error).toBe(reason)
    })
  })

  describe(`HTTP URL warning`, () => {
    let windowWasPresent: boolean
    let originalWindow: typeof globalThis.window
    let warnSpy: ReturnType<typeof vi.spyOn>

    beforeEach(() => {
      // Track whether window was originally present
      windowWasPresent = `window` in globalThis
      originalWindow = globalThis.window
      // Reset the warning flag before each test
      _resetHttpWarningForTesting()
      warnSpy = vi.spyOn(console, `warn`).mockImplementation(() => {})
    })

    afterEach(() => {
      // Clean up globals properly - delete if it wasn't present, restore if it was
      if (windowWasPresent) {
        globalThis.window = originalWindow
      } else {
        // @ts-expect-error - intentionally removing window
        delete globalThis.window
      }
      warnSpy.mockRestore()
      _resetHttpWarningForTesting()
    })

    it(`should warn when using HTTP URL in browser environment`, () => {
      // Mock browser environment with location
      globalThis.window = {
        location: { href: `http://example.com/page` },
      } as typeof globalThis.window

      const fetchWrapper = (): Promise<Response> =>
        Promise.resolve(Response.error())

      new ShapeStream({
        url: `http://example.com/v1/shape`,
        params: { table: `foo` },
        signal: aborter.signal,
        fetchClient: fetchWrapper,
        warnOnHttp: true,
      })

      expect(warnSpy).toHaveBeenCalledTimes(1)
      expect(warnSpy).toHaveBeenCalledWith(
        expect.stringContaining(`[Electric] Using HTTP (not HTTPS)`)
      )
      expect(warnSpy).toHaveBeenCalledWith(expect.stringContaining(`HTTP/1.1`))
      expect(warnSpy).toHaveBeenCalledWith(
        expect.stringContaining(`https://electric-sql.com/r/electric-http2`)
      )
    })

    it(`should not warn when using HTTPS URL`, () => {
      // Mock browser environment
      globalThis.window = {
        location: { href: `https://example.com/page` },
      } as typeof globalThis.window

      const fetchWrapper = (): Promise<Response> =>
        Promise.resolve(Response.error())

      new ShapeStream({
        url: `https://example.com/v1/shape`,
        params: { table: `foo` },
        signal: aborter.signal,
        fetchClient: fetchWrapper,
        warnOnHttp: true,
      })

      expect(warnSpy).not.toHaveBeenCalled()
    })

    it(`should not warn in non-browser environment`, () => {
      // Remove window to simulate Node.js environment
      // @ts-expect-error - intentionally removing window
      delete globalThis.window

      const fetchWrapper = (): Promise<Response> =>
        Promise.resolve(Response.error())

      new ShapeStream({
        url: `http://example.com/v1/shape`,
        params: { table: `foo` },
        signal: aborter.signal,
        fetchClient: fetchWrapper,
        warnOnHttp: true,
      })

      expect(warnSpy).not.toHaveBeenCalled()
    })

    it(`should not warn when warnOnHttp is false`, () => {
      // Mock browser environment
      globalThis.window = {
        location: { href: `http://example.com/page` },
      } as typeof globalThis.window

      const fetchWrapper = (): Promise<Response> =>
        Promise.resolve(Response.error())

      new ShapeStream({
        url: `http://example.com/v1/shape`,
        params: { table: `foo` },
        signal: aborter.signal,
        fetchClient: fetchWrapper,
        warnOnHttp: false,
      })

      expect(warnSpy).not.toHaveBeenCalled()
    })

    describe(`relative URL handling`, () => {
      it(`should warn when relative URL is used and window.location is http`, () => {
        // Mock browser environment with HTTP location
        globalThis.window = {
          location: { href: `http://example.com/page` },
        } as typeof globalThis.window

        const fetchWrapper = (): Promise<Response> =>
          Promise.resolve(Response.error())

        new ShapeStream({
          url: `/v1/shape`,
          params: { table: `foo` },
          signal: aborter.signal,
          fetchClient: fetchWrapper,
          warnOnHttp: true,
        })

        expect(warnSpy).toHaveBeenCalledTimes(1)
        expect(warnSpy).toHaveBeenCalledWith(
          expect.stringContaining(`[Electric] Using HTTP (not HTTPS)`)
        )
      })

      it(`should not warn when relative URL is used and window.location is https`, () => {
        // Mock browser environment with HTTPS location
        globalThis.window = {
          location: { href: `https://example.com/page` },
        } as typeof globalThis.window

        const fetchWrapper = (): Promise<Response> =>
          Promise.resolve(Response.error())

        new ShapeStream({
          url: `/v1/shape`,
          params: { table: `foo` },
          signal: aborter.signal,
          fetchClient: fetchWrapper,
          warnOnHttp: true,
        })

        expect(warnSpy).not.toHaveBeenCalled()
      })

      it(`should resolve relative URL with query params correctly`, () => {
        // Mock browser environment with HTTP location
        globalThis.window = {
          location: { href: `http://example.com/app/` },
        } as typeof globalThis.window

        const fetchWrapper = (): Promise<Response> =>
          Promise.resolve(Response.error())

        new ShapeStream({
          url: `/v1/shape?table=foo`,
          params: { table: `foo` },
          signal: aborter.signal,
          fetchClient: fetchWrapper,
          warnOnHttp: true,
        })

        expect(warnSpy).toHaveBeenCalledTimes(1)
      })
    })

    describe(`warn-once behavior`, () => {
      it(`should only warn once even when creating multiple ShapeStreams`, () => {
        // Mock browser environment
        globalThis.window = {
          location: { href: `http://example.com/page` },
        } as typeof globalThis.window

        const fetchWrapper = (): Promise<Response> =>
          Promise.resolve(Response.error())

        // Create first stream - should warn
        new ShapeStream({
          url: `http://example.com/v1/shape`,
          params: { table: `foo` },
          signal: aborter.signal,
          fetchClient: fetchWrapper,
          warnOnHttp: true,
        })

        expect(warnSpy).toHaveBeenCalledTimes(1)

        // Create second stream with same HTTP URL - should NOT warn again
        const aborter2 = new AbortController()
        new ShapeStream({
          url: `http://example.com/v1/shape`,
          params: { table: `bar` },
          signal: aborter2.signal,
          fetchClient: fetchWrapper,
          warnOnHttp: true,
        })
        aborter2.abort()

        expect(warnSpy).toHaveBeenCalledTimes(1)

        // Create third stream with different HTTP URL - should still NOT warn
        const aborter3 = new AbortController()
        new ShapeStream({
          url: `http://other.com/v1/shape`,
          params: { table: `baz` },
          signal: aborter3.signal,
          fetchClient: fetchWrapper,
          warnOnHttp: true,
        })
        aborter3.abort()

        expect(warnSpy).toHaveBeenCalledTimes(1)
      })

      it(`should warn again after reset (for testing purposes)`, () => {
        // Mock browser environment
        globalThis.window = {
          location: { href: `http://example.com/page` },
        } as typeof globalThis.window

        const fetchWrapper = (): Promise<Response> =>
          Promise.resolve(Response.error())

        // First stream - should warn
        new ShapeStream({
          url: `http://example.com/v1/shape`,
          params: { table: `foo` },
          signal: aborter.signal,
          fetchClient: fetchWrapper,
          warnOnHttp: true,
        })

        expect(warnSpy).toHaveBeenCalledTimes(1)

        // Reset the flag
        _resetHttpWarningForTesting()

        // Second stream after reset - should warn again
        const aborter2 = new AbortController()
        new ShapeStream({
          url: `http://example.com/v1/shape`,
          params: { table: `bar` },
          signal: aborter2.signal,
          fetchClient: fetchWrapper,
          warnOnHttp: true,
        })
        aborter2.abort()

        expect(warnSpy).toHaveBeenCalledTimes(2)
      })
    })
  })
})
