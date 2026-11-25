import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { ShapeStream } from '../src'
import { expiredShapesCache } from '../src/expired-shapes-cache'
import { upToDateTracker } from '../src/up-to-date-tracker'

describe(`online event`, () => {
  const shapeUrl = `https://example.com/v1/shape`
  let aborter: AbortController

  beforeEach(() => {
    localStorage.clear()
    expiredShapesCache.clear()
    upToDateTracker.clear()
    aborter = new AbortController()
    vi.useFakeTimers()
    // Full jitter then waits almost the whole delay, and the online jitter
    // almost the whole 500ms.
    vi.spyOn(Math, `random`).mockReturnValue(0.99)
  })

  afterEach(() => {
    aborter.abort()
    vi.useRealTimers()
    vi.restoreAllMocks()
    vi.unstubAllGlobals()
  })

  const upToDateResponse = () =>
    new Response(JSON.stringify([{ headers: { control: `up-to-date` } }]), {
      status: 200,
      headers: {
        'electric-handle': `handle-1`,
        'electric-offset': `0_0`,
        'electric-schema': `{"id":"int4"}`,
        'electric-cursor': `cursor-1`,
        'electric-up-to-date': ``,
      },
    })

  // Fails the first request like a dropped connection, so the fetch layer
  // enters a long backoff, then answers every later request.
  function offlineThenOnline() {
    const calls: Array<AbortSignal | undefined> = []
    const fetchClient = vi.fn(
      (_url: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
        calls.push(init?.signal ?? undefined)
        if (calls.length === 1) return Promise.reject(new TypeError(`offline`))
        return Promise.resolve(upToDateResponse())
      }
    )
    return { fetchClient, calls }
  }

  // Answers the first request 503 with a Retry-After, then every later one.
  function throttledThenOk(retryAfterSeconds: number) {
    const fetchClient = vi.fn(
      (): Promise<Response> =>
        Promise.resolve(
          fetchClient.mock.calls.length === 1
            ? new Response(null, {
                status: 503,
                headers: { 'retry-after': String(retryAfterSeconds) },
              })
            : upToDateResponse()
        )
    )
    return fetchClient
  }

  function startStream(
    fetchClient: typeof fetch,
    backoffOptions = { initialDelay: 30_000, maxDelay: 60_000, multiplier: 2 }
  ) {
    const stream = new ShapeStream({
      url: shapeUrl,
      params: { table: `test` },
      signal: aborter.signal,
      fetchClient,
      subscribe: false,
      backoffOptions,
    })
    stream.subscribe(() => {})
    return stream
  }

  it(`ends the retry backoff early when the browser comes back online`, async () => {
    const { fetchClient, calls } = offlineThenOnline()
    const stream = startStream(fetchClient)

    await vi.advanceTimersByTimeAsync(0)
    expect(fetchClient).toHaveBeenCalledTimes(1)
    // Without the event the retry waits out the backoff (about 30s).
    await vi.advanceTimersByTimeAsync(5_000)
    expect(fetchClient).toHaveBeenCalledTimes(1)
    expect(stream.isConnected()).toBe(false)

    window.dispatchEvent(new Event(`online`))
    // Held back by the random delay, so not instantaneous.
    await vi.advanceTimersByTimeAsync(100)
    expect(fetchClient).toHaveBeenCalledTimes(1)

    await vi.advanceTimersByTimeAsync(500)
    expect(fetchClient).toHaveBeenCalledTimes(2)
    expect(calls[0]?.aborted).toBe(true)
    await vi.waitFor(() => expect(stream.isUpToDate).toBe(true))
  })

  it(`does not retry before the Retry-After deadline when the browser comes back online`, async () => {
    const fetchClient = throttledThenOk(10)
    // A short client backoff, so the 10 seconds come from the server.
    const stream = startStream(fetchClient, {
      initialDelay: 1_000,
      maxDelay: 2_000,
      multiplier: 2,
    })

    await vi.advanceTimersByTimeAsync(2_000)
    expect(fetchClient).toHaveBeenCalledTimes(1)

    window.dispatchEvent(new Event(`online`))
    // Past the random delay of the event, still before the deadline: the 10s
    // of the header and the 990ms of Retry-After jitter (random is 0.99).
    await vi.advanceTimersByTimeAsync(8_988)
    expect(fetchClient).toHaveBeenCalledTimes(1)

    // The deadline is the earliest the retry may come.
    await vi.advanceTimersByTimeAsync(3)
    expect(fetchClient).toHaveBeenCalledTimes(2)
    await vi.waitFor(() => expect(stream.isUpToDate).toBe(true))
  })

  it(`still ends the client's own backoff early once the Retry-After deadline has passed`, async () => {
    // The client backoff is about 29s, the server asks for 2s and the Retry-After
    // jitter adds 198ms.
    const fetchClient = throttledThenOk(2)
    startStream(fetchClient)

    await vi.advanceTimersByTimeAsync(500)
    window.dispatchEvent(new Event(`online`))
    await vi.advanceTimersByTimeAsync(1_697)
    expect(fetchClient).toHaveBeenCalledTimes(1)

    await vi.advanceTimersByTimeAsync(2)
    expect(fetchClient).toHaveBeenCalledTimes(2)
  })

  it(`keeps streams that share a Retry-After apart when the browser comes back online`, async () => {
    // Each waiter draws two random numbers: its client backoff, kept at zero,
    // and its Retry-After jitter, 0.7 and 0.8 of the 1s the 10s allows.
    const draws = [0, 0.7, 0, 0.8]
    vi.spyOn(Math, `random`).mockImplementation(() => draws.shift() ?? 0.99)
    const shortBackoff = { initialDelay: 1_000, maxDelay: 2_000, multiplier: 2 }
    const first = throttledThenOk(10)
    startStream(first, shortBackoff)
    await vi.advanceTimersByTimeAsync(0)
    const second = throttledThenOk(10)
    startStream(second, shortBackoff)
    await vi.advanceTimersByTimeAsync(0)
    expect(draws).toEqual([])

    await vi.advanceTimersByTimeAsync(1_000)
    window.dispatchEvent(new Event(`online`))

    // Each stream retries at the end of its own wait, 10.7s and 10.8s, not at
    // the 10s both were given.
    await vi.advanceTimersByTimeAsync(9_649)
    expect(first).toHaveBeenCalledTimes(1)
    expect(second).toHaveBeenCalledTimes(1)
    await vi.advanceTimersByTimeAsync(100)
    expect(first).toHaveBeenCalledTimes(2)
    expect(second).toHaveBeenCalledTimes(1)
    await vi.advanceTimersByTimeAsync(100)
    expect(second).toHaveBeenCalledTimes(2)

    // Neither request is sent twice.
    await vi.advanceTimersByTimeAsync(5_000)
    expect(first).toHaveBeenCalledTimes(2)
    expect(second).toHaveBeenCalledTimes(2)
  })

  it(`does not send the retry twice when the restart falls at the same moment as the stream's own retry`, async () => {
    // The server asks for 10s, the client backoff is short, so the stream's own
    // retry and the restart that waited for the deadline are due together. The
    // retry takes a while to answer, so it is still in flight when the restart
    // comes.
    const fetchClient = vi.fn(
      (_url: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
        if (fetchClient.mock.calls.length === 1) {
          return Promise.resolve(
            new Response(null, {
              status: 503,
              headers: { 'retry-after': `10` },
            })
          )
        }
        return new Promise<Response>((resolve, reject) => {
          const answer = setTimeout(() => resolve(upToDateResponse()), 50)
          init?.signal?.addEventListener(`abort`, () => {
            clearTimeout(answer)
            reject(new Error(`aborted`))
          })
        })
      }
    )
    startStream(fetchClient, {
      initialDelay: 1_000,
      maxDelay: 2_000,
      multiplier: 2,
    })
    await vi.advanceTimersByTimeAsync(2_000)
    window.dispatchEvent(new Event(`online`))

    await vi.advanceTimersByTimeAsync(12_000)

    expect(fetchClient).toHaveBeenCalledTimes(2)
  })

  describe(`a snapshot request that fails while the stream waits on a Retry-After`, () => {
    // The stream's first request is told to wait 10 seconds. Snapshot requests
    // are answered from `snapshotResponses`, one for each attempt.
    function streamWaitingOnRetryAfter(
      snapshotResponses: Array<() => Response>
    ) {
      const mainRequests = vi.fn()
      const fetchClient = vi.fn(
        (input: RequestInfo | URL): Promise<Response> => {
          if (input.toString().includes(`subset__`)) {
            return Promise.resolve(snapshotResponses.shift()!())
          }
          mainRequests()
          return Promise.resolve(
            mainRequests.mock.calls.length === 1
              ? new Response(null, {
                  status: 503,
                  headers: { 'retry-after': `10` },
                })
              : upToDateResponse()
          )
        }
      )
      // A short client backoff, so the 10 seconds come from the server.
      const stream = startStream(fetchClient, {
        initialDelay: 1_000,
        maxDelay: 2_000,
        multiplier: 2,
      })
      return { stream, mainRequests }
    }

    it.each([
      [`400`, [() => new Response(null, { status: 400 })]],
      [
        `503 with a shorter Retry-After`,
        [
          () =>
            new Response(null, {
              status: 503,
              headers: { 'retry-after': `1` },
            }),
          () => new Response(null, { status: 400 }),
        ],
      ],
    ])(
      `does not let a snapshot that fails with %s shorten the stream's wait`,
      async (_, snapshotResponses) => {
        const { stream, mainRequests } =
          streamWaitingOnRetryAfter(snapshotResponses)
        await vi.advanceTimersByTimeAsync(1_000)
        expect(mainRequests).toHaveBeenCalledTimes(1)

        const snapshot = stream.fetchSnapshot({ limit: 1 }).catch(() => {})
        await vi.advanceTimersByTimeAsync(100)
        window.dispatchEvent(new Event(`online`))

        // Past the random delay of the event and the end of the snapshot,
        // still before the stream's own deadline: the 10 seconds of the header
        // and 990ms of Retry-After jitter (random is 0.99).
        await vi.advanceTimersByTimeAsync(9_889)
        await snapshot
        expect(mainRequests).toHaveBeenCalledTimes(1)

        await vi.advanceTimersByTimeAsync(2)
        expect(mainRequests).toHaveBeenCalledTimes(2)
      }
    )
  })

  it(`leaves a connected stream alone`, async () => {
    const fetchClient = vi.fn(
      (_url: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
        if (fetchClient.mock.calls.length === 1) {
          return Promise.resolve(upToDateResponse())
        }
        // A held-open live request.
        return new Promise<Response>((_resolve, reject) => {
          init?.signal?.addEventListener(
            `abort`,
            () => reject(new Error(`aborted`)),
            { once: true }
          )
        })
      }
    )
    const stream = new ShapeStream({
      url: shapeUrl,
      params: { table: `test` },
      signal: aborter.signal,
      fetchClient,
    })
    stream.subscribe(() => {})

    await vi.advanceTimersByTimeAsync(0)
    expect(stream.isConnected()).toBe(true)
    const requests = fetchClient.mock.calls.length

    window.dispatchEvent(new Event(`online`))
    await vi.advanceTimersByTimeAsync(1_000)

    expect(fetchClient).toHaveBeenCalledTimes(requests)
  })

  it(`stops listening once the stream is torn down`, async () => {
    const addSpy = vi.spyOn(window, `addEventListener`)
    const removeSpy = vi.spyOn(window, `removeEventListener`)
    const { fetchClient } = offlineThenOnline()
    const stream = startStream(fetchClient)

    await vi.advanceTimersByTimeAsync(0)
    const onlineHandler = addSpy.mock.calls.find(
      ([type]) => type === `online`
    )?.[1]
    expect(onlineHandler).toBeDefined()

    stream.unsubscribeAll()

    expect(removeSpy).toHaveBeenCalledWith(`online`, onlineHandler)
    window.dispatchEvent(new Event(`online`))
    await vi.advanceTimersByTimeAsync(1_000)
    expect(fetchClient).toHaveBeenCalledTimes(1)
  })

  it(`drops a restart that is still waiting on its random delay when torn down`, async () => {
    const { fetchClient } = offlineThenOnline()
    const stream = startStream(fetchClient)
    await vi.advanceTimersByTimeAsync(0)

    window.dispatchEvent(new Event(`online`))
    expect(vi.getTimerCount()).toBeGreaterThan(0)
    stream.unsubscribeAll()
    await vi.advanceTimersByTimeAsync(0)

    expect(vi.getTimerCount()).toBe(0)
    expect(fetchClient).toHaveBeenCalledTimes(1)
  })

  describe(`when the stream is paused and resumed before its request loop settles`, () => {
    // Hidden and then visible again with no tick in between, while the first
    // request is in flight and takes a moment to unwind after it is aborted.
    // The loop the pause aborted therefore settles only after the resumed
    // loop is running.
    function startPausableStream(fetchClient: typeof fetch) {
      let notifyVisibility: ((state: `visible` | `hidden`) => void) | undefined
      const stream = new ShapeStream({
        url: shapeUrl,
        params: { table: `test` },
        signal: aborter.signal,
        fetchClient,
        subscribe: false,
        runtimeVisibility: {
          getCurrentState: () => `visible`,
          subscribe: (handler) => {
            notifyVisibility = handler
            return () => {
              notifyVisibility = undefined
            }
          },
        },
        backoffOptions: {
          initialDelay: 30_000,
          maxDelay: 60_000,
          multiplier: 2,
        },
      })
      stream.subscribe(() => {})
      return {
        stream,
        pauseAndResume() {
          notifyVisibility?.(`hidden`)
          notifyVisibility?.(`visible`)
        },
      }
    }

    // The first request is held open until it is aborted, and then takes
    // 10ms to fail. The request the resumed stream sends fails like a dropped
    // connection, so it enters a long backoff. Every request after that is
    // answered.
    function fetchThatFailsAfterResume() {
      const fetchClient = vi.fn(
        (_url: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
          const calls = fetchClient.mock.calls.length
          if (calls === 1) {
            return new Promise<Response>((_resolve, reject) => {
              init?.signal?.addEventListener(
                `abort`,
                () => setTimeout(() => reject(new Error(`aborted`)), 10),
                { once: true }
              )
            })
          }
          if (calls === 2) return Promise.reject(new TypeError(`offline`))
          return Promise.resolve(upToDateResponse())
        }
      )
      return fetchClient
    }

    it(`still ends the retry backoff early when the browser comes back online`, async () => {
      const fetchClient = fetchThatFailsAfterResume()
      const { pauseAndResume } = startPausableStream(fetchClient)
      await vi.advanceTimersByTimeAsync(0)
      expect(fetchClient).toHaveBeenCalledTimes(1)

      pauseAndResume()
      await vi.advanceTimersByTimeAsync(0)
      // The resumed stream's request failed and the retry waits about 30s.
      expect(fetchClient).toHaveBeenCalledTimes(2)
      // The aborted loop settles in this time, while the resumed one waits.
      await vi.advanceTimersByTimeAsync(5_000)
      expect(fetchClient).toHaveBeenCalledTimes(2)

      window.dispatchEvent(new Event(`online`))
      await vi.advanceTimersByTimeAsync(600)

      expect(fetchClient).toHaveBeenCalledTimes(3)
    })

    it(`leaves no online listener and no timer once torn down`, async () => {
      const addSpy = vi.spyOn(window, `addEventListener`)
      const removeSpy = vi.spyOn(window, `removeEventListener`)
      const fetchClient = fetchThatFailsAfterResume()
      const { stream, pauseAndResume } = startPausableStream(fetchClient)
      await vi.advanceTimersByTimeAsync(0)
      pauseAndResume()
      await vi.advanceTimersByTimeAsync(10)
      expect(fetchClient).toHaveBeenCalledTimes(2)

      stream.unsubscribeAll()
      await vi.advanceTimersByTimeAsync(0)

      const onlineListeners = addSpy.mock.calls.filter(
        ([type]) => type === `online`
      )
      expect(onlineListeners).toHaveLength(1)
      expect(removeSpy).toHaveBeenCalledWith(`online`, onlineListeners[0]![1])
      expect(vi.getTimerCount()).toBe(0)
      window.dispatchEvent(new Event(`online`))
      await vi.advanceTimersByTimeAsync(1_000)
      expect(fetchClient).toHaveBeenCalledTimes(2)
    })
  })

  it(`works in runtimes without a window`, async () => {
    vi.stubGlobal(`window`, undefined)
    const { fetchClient } = offlineThenOnline()

    expect(() => startStream(fetchClient)).not.toThrow()
    await vi.advanceTimersByTimeAsync(0)
    expect(fetchClient).toHaveBeenCalledTimes(1)
  })

  it(`works when window cannot listen for events`, async () => {
    vi.stubGlobal(`window`, {})
    const { fetchClient } = offlineThenOnline()

    expect(() => startStream(fetchClient)).not.toThrow()
    await vi.advanceTimersByTimeAsync(0)
    expect(fetchClient).toHaveBeenCalledTimes(1)
  })
})
