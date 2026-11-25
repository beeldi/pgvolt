import {
  describe,
  beforeEach,
  afterEach,
  it,
  expect,
  vi,
  type Mock,
} from 'vitest'
import { setTimeout as sleep } from 'node:timers/promises'
import {
  FetchError,
  FetchBodyReadError,
  FetchBackoffAbortError,
  ResponseTooLargeError,
} from '../src/error'
import {
  createFetchWithBackoff,
  BackoffDefaults,
  createFetchWithChunkBuffer,
  createFetchWithConsumedMessages,
  consumeResponseBody,
  observeResponseBodyChunks,
  parseRetryAfterHeader,
  retryOnBodyReadFailure,
} from '../src/fetch'
import {
  CHUNK_LAST_OFFSET_HEADER,
  CHUNK_UP_TO_DATE_HEADER,
  SHAPE_HANDLE_HEADER,
  SNAPSHOT_HEADER,
} from '../src/constants'

// Makes every setTimeout fire right away and returns the delays it was given.
function recordTimeoutDelays(): Array<number | undefined> {
  const delays: Array<number | undefined> = []
  vi.spyOn(globalThis, `setTimeout`).mockImplementation(((
    fn: () => void,
    ms?: number
  ) => {
    delays.push(ms)
    queueMicrotask(fn)
    return 0
  }) as unknown as typeof setTimeout)
  return delays
}

describe(`createFetchWithBackoff`, () => {
  const initialDelay = 10
  const maxDelay = 100
  let mockFetchClient: Mock<typeof fetch>

  beforeEach(() => {
    mockFetchClient = vi.fn()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it(`should return a successful response on the first attempt`, async () => {
    const mockResponse = new Response(null, { status: 200, statusText: `OK` })
    mockFetchClient.mockResolvedValue(mockResponse)

    const fetchWithBackoff = createFetchWithBackoff(mockFetchClient)

    const result = await fetchWithBackoff(`https://example.com`)

    expect(mockFetchClient).toHaveBeenCalledTimes(1)
    expect(result.ok).toBe(true)
    expect(result).toEqual(mockResponse)
  })

  it(`should retry the request on a 500 response and succeed after a retry`, async () => {
    const mockErrorResponse = new Response(null, { status: 500 })
    const mockSuccessResponse = new Response(null, {
      status: 200,
      statusText: `OK`,
    })
    mockFetchClient
      .mockResolvedValueOnce(mockErrorResponse)
      .mockResolvedValueOnce(mockSuccessResponse)

    const fetchWithBackoff = createFetchWithBackoff(mockFetchClient, {
      ...BackoffDefaults,
      initialDelay,
    })

    const result = await fetchWithBackoff(`https://example.com`)

    expect(mockFetchClient).toHaveBeenCalledTimes(2)
    expect(result.ok).toBe(true)
  })

  it(`should retry the request on a 429 response and succeed after a retry`, async () => {
    const mockErrorResponse = new Response(null, { status: 429 })
    const mockSuccessResponse = new Response(null, {
      status: 200,
      statusText: `OK`,
    })
    mockFetchClient
      .mockResolvedValueOnce(mockErrorResponse)
      .mockResolvedValueOnce(mockSuccessResponse)

    const fetchWithBackoff = createFetchWithBackoff(mockFetchClient, {
      ...BackoffDefaults,
      initialDelay,
    })

    const result = await fetchWithBackoff(`https://example.com`)

    expect(mockFetchClient).toHaveBeenCalledTimes(2)
    expect(result.ok).toBe(true)
  })

  it(`should apply exponential backoff and retry until maxDelay is reached`, async () => {
    const mockErrorResponse = new Response(null, { status: 500 })
    const mockSuccessResponse = new Response(null, {
      status: 200,
      statusText: `OK`,
    })
    mockFetchClient
      .mockResolvedValueOnce(mockErrorResponse)
      .mockResolvedValueOnce(mockErrorResponse)
      .mockResolvedValueOnce(mockErrorResponse)
      .mockResolvedValueOnce(mockSuccessResponse)

    const multiplier = 2

    const fetchWithBackoff = createFetchWithBackoff(mockFetchClient, {
      initialDelay,
      maxDelay,
      multiplier,
    })

    const result = await fetchWithBackoff(`https://example.com`)

    expect(mockFetchClient).toHaveBeenCalledTimes(4)
    expect(result.ok).toBe(true)
  })

  it(`should stop retrying and throw an error on a 400 response`, async () => {
    const mockErrorResponse = new Response(null, {
      status: 400,
      statusText: `Bad Request`,
    })
    mockFetchClient.mockResolvedValue(mockErrorResponse)

    const fetchWithBackoff = createFetchWithBackoff(mockFetchClient)

    await expect(fetchWithBackoff(`https://example.com`)).rejects.toThrow(
      FetchError
    )
    expect(mockFetchClient).toHaveBeenCalledTimes(1)
  })

  it(`should throw FetchBackoffAborted if the abort signal is triggered`, async () => {
    const mockAbortController = new AbortController()
    const signal = mockAbortController.signal
    const mockErrorResponse = new Response(null, { status: 500 })
    mockFetchClient.mockImplementation(
      () => new Promise((res) => setTimeout(() => res(mockErrorResponse), 10))
    )

    const fetchWithBackoff = createFetchWithBackoff(mockFetchClient, {
      ...BackoffDefaults,
      initialDelay: 1000,
    })

    setTimeout(() => mockAbortController.abort(), 5)

    await expect(
      fetchWithBackoff(`https://example.com`, { signal })
    ).rejects.toThrow(FetchBackoffAbortError)

    expect(mockFetchClient).toHaveBeenCalledTimes(1)
  })

  it(`should abort retry backoff immediately when the signal aborts`, async () => {
    vi.useFakeTimers()

    const mockAbortController = new AbortController()
    const signal = mockAbortController.signal
    mockFetchClient.mockRejectedValue(new TypeError(`Network request failed`))

    const fetchWithBackoff = createFetchWithBackoff(mockFetchClient, {
      initialDelay: 10_000,
      maxDelay: 10_000,
      multiplier: 1,
      maxRetries: Infinity,
    })

    const fetchPromise = fetchWithBackoff(`https://example.com`, { signal })
    await vi.advanceTimersByTimeAsync(0)
    expect(mockFetchClient).toHaveBeenCalledTimes(1)

    const rejectionExpectation = expect(fetchPromise).rejects.toThrow(
      FetchBackoffAbortError
    )

    mockAbortController.abort()
    await vi.advanceTimersByTimeAsync(0)

    await rejectionExpectation
    expect(mockFetchClient).toHaveBeenCalledTimes(1)
  })

  it(`should not retry when a client error (4xx) occurs`, async () => {
    const mockErrorResponse = new Response(null, {
      status: 403,
      statusText: `Forbidden`,
    })
    mockFetchClient.mockResolvedValue(mockErrorResponse)

    const fetchWithBackoff = createFetchWithBackoff(
      mockFetchClient,
      BackoffDefaults
    )

    await expect(fetchWithBackoff(`https://example.com`)).rejects.toThrow(
      FetchError
    )
    expect(mockFetchClient).toHaveBeenCalledTimes(1)
  })

  it(`should honor retry-after header from 503 response`, async () => {
    const retryAfterSeconds = 2
    const mockErrorResponse = new Response(null, {
      status: 503,
      statusText: `Service Unavailable`,
      headers: new Headers({ 'retry-after': `${retryAfterSeconds}` }),
    })
    const mockSuccessResponse = new Response(null, {
      status: 200,
      statusText: `OK`,
    })
    mockFetchClient
      .mockResolvedValueOnce(mockErrorResponse)
      .mockResolvedValueOnce(mockSuccessResponse)

    const fetchWithBackoff = createFetchWithBackoff(mockFetchClient, {
      ...BackoffDefaults,
      initialDelay: 1, // Very short client delay
    })

    const startTime = Date.now()
    const result = await fetchWithBackoff(`https://example.com`)
    const elapsed = Date.now() - startTime

    // Should have waited at least retryAfterSeconds (minus small tolerance for test execution)
    expect(elapsed).toBeGreaterThanOrEqual(retryAfterSeconds * 1000 - 100)
    expect(mockFetchClient).toHaveBeenCalledTimes(2)
    expect(result.ok).toBe(true)
  })

  describe(`retry-after jitter`, () => {
    const retryAfterMs = 60_000

    // Runs one failed attempt followed by a success and returns the delay
    // passed to setTimeout for the backoff wait.
    async function backoffWaitFor(random: number): Promise<number> {
      vi.spyOn(Math, `random`).mockReturnValue(random)
      const waits = recordTimeoutDelays()

      mockFetchClient
        .mockResolvedValueOnce(
          new Response(null, {
            status: 503,
            headers: new Headers({ 'retry-after': `${retryAfterMs / 1000}` }),
          })
        )
        .mockResolvedValueOnce(new Response(null, { status: 200 }))

      const fetchWithBackoff = createFetchWithBackoff(mockFetchClient, {
        ...BackoffDefaults,
        initialDelay: 1,
      })
      await fetchWithBackoff(`https://example.com`)

      expect(waits).toHaveLength(1)
      return waits[0]!
    }

    afterEach(() => {
      vi.restoreAllMocks()
    })

    it(`should add positive jitter on top of a dominating retry-after`, async () => {
      // 10% of 60s is 6s, capped at 5s, so a random value of 0.5 adds 2.5s
      expect(await backoffWaitFor(0.5)).toBe(retryAfterMs + 2_500)
    })

    it(`should never wait less than retry-after`, async () => {
      expect(await backoffWaitFor(0)).toBe(retryAfterMs)
    })

    it(`should keep the added jitter within the cap`, async () => {
      expect(await backoffWaitFor(0.999999)).toBeLessThanOrEqual(
        retryAfterMs + 5_000
      )
    })
  })

  // it(`should retry multiple times and eventually throw if no success`, async () => {
  //   const mockErrorResponse = new Response(null, { status: 500 })
  //   mockFetchClient.mockImplementation(
  //     () => new Promise((res) => setTimeout(() => res(mockErrorResponse), 10))
  //   )

  //   const fetchWithBackoff = createFetchWithBackoff(mockFetchClient, {
  //     ...BackoffDefaults,
  //     initialDelay,
  //     maxDelay,
  //   })

  //   await expect(fetchWithBackoff(`https://example.com`)).rejects.toThrow(
  //     FetchError
  //   )
  //   expect(mockFetchClient.mock.calls.length).greaterThan(1)
  // })
})

describe(`parseRetryAfterHeader`, () => {
  it(`should return 0 for undefined header`, () => {
    expect(parseRetryAfterHeader(undefined)).toBe(0)
  })

  it(`should return 0 for empty string`, () => {
    expect(parseRetryAfterHeader(``)).toBe(0)
  })

  it(`should parse delta-seconds format correctly`, () => {
    expect(parseRetryAfterHeader(`120`)).toBe(120_000) // 120 seconds = 120,000 ms
    expect(parseRetryAfterHeader(`1`)).toBe(1_000)
    expect(parseRetryAfterHeader(`60`)).toBe(60_000)
  })

  it(`should return 0 for invalid delta-seconds values`, () => {
    expect(parseRetryAfterHeader(`-10`)).toBe(0) // Negative values
    expect(parseRetryAfterHeader(`0`)).toBe(0) // Zero
    expect(parseRetryAfterHeader(`abc`)).toBe(0) // Non-numeric
  })

  it(`should parse HTTP-date format correctly`, () => {
    const futureDate = new Date(Date.now() + 30_000) // 30 seconds in the future
    const httpDate = futureDate.toUTCString()
    const result = parseRetryAfterHeader(httpDate)

    // Should be approximately 30 seconds, allow some tolerance for test execution time
    expect(result).toBeGreaterThan(29_000)
    expect(result).toBeLessThan(31_000)
  })

  it(`should handle clock skew for past dates`, () => {
    const pastDate = new Date(Date.now() - 10_000) // 10 seconds in the past
    const httpDate = pastDate.toUTCString()

    // Should clamp to 0 for past dates
    expect(parseRetryAfterHeader(httpDate)).toBe(0)
  })

  it(`should cap very large HTTP-date values at 1 hour`, () => {
    const farFutureDate = new Date(Date.now() + 7200_000) // 2 hours in the future
    const httpDate = farFutureDate.toUTCString()

    // Should be capped at 1 hour (3600000 ms)
    expect(parseRetryAfterHeader(httpDate)).toBe(3600_000)
  })

  it(`should return 0 for invalid HTTP-date format`, () => {
    expect(parseRetryAfterHeader(`not a date`)).toBe(0)
    expect(parseRetryAfterHeader(`2024-13-45`)).toBe(0) // Invalid date
  })

  it(`should handle edge case of very large delta-seconds`, () => {
    // Very large number (more than 1 hour worth of seconds)
    expect(parseRetryAfterHeader(`7200`)).toBe(7200_000) // 2 hours in ms (not capped in delta-seconds format)
  })

  it(`should handle decimal numbers in delta-seconds format`, () => {
    // HTTP spec requires delta-seconds to be integers, but parsing as Number allows decimals
    expect(parseRetryAfterHeader(`30.5`)).toBe(30_500)
  })
})

describe(`createFetchWithChunkBuffer`, () => {
  const baseUrl = `https://example.com/v1/shape?table=foo`
  let mockFetch: Mock<typeof fetch>
  const responseHeaders = (headers: Record<string, string>) => {
    return new Headers(headers)
  }

  beforeEach(() => {
    mockFetch = vi.fn()
  })

  it(`should perform a basic fetch when no prefetch metadata is available`, async () => {
    const fetchWrapper = createFetchWithChunkBuffer(mockFetch)
    const mockResponse = new Response(`test response`, {
      status: 200,
    })

    mockFetch.mockResolvedValueOnce(mockResponse)

    const result = await fetchWrapper(baseUrl)
    expect(result).toBe(mockResponse)
    expect(mockFetch).toHaveBeenCalledTimes(1)
    expect(mockFetch).toHaveBeenCalledWith(baseUrl)
  })

  it(`should prefetch the next chunk when headers are present`, async () => {
    const fetchWrapper = createFetchWithChunkBuffer(mockFetch)
    const initialResponse = new Response(`initial chunk`, {
      status: 200,
      headers: responseHeaders({
        [SHAPE_HANDLE_HEADER]: `123`,
        [CHUNK_LAST_OFFSET_HEADER]: `456`,
      }),
    })

    const nextResponse = new Response(`next chunk`, {
      status: 200,
    })

    mockFetch.mockResolvedValueOnce(initialResponse)
    mockFetch.mockResolvedValueOnce(nextResponse)

    const result = await fetchWrapper(baseUrl)
    expect(result).toBe(initialResponse)

    // Check if the next chunk was prefetched
    const nextUrl = sortUrlParams(`${baseUrl}&handle=123&offset=456`)
    expect(mockFetch).toHaveBeenCalledWith(nextUrl, expect.anything())
  })

  it(`should not consume prefetched GET responses for POST requests with same URL`, async () => {
    const fetchWrapper = createFetchWithChunkBuffer(mockFetch)
    const initialResponse = new Response(`initial chunk`, {
      status: 200,
      headers: responseHeaders({
        [SHAPE_HANDLE_HEADER]: `123`,
        [CHUNK_LAST_OFFSET_HEADER]: `456`,
      }),
    })
    const prefetchedGetResponse = new Response(`prefetched GET chunk`, {
      status: 200,
    })
    const postResponse = new Response(`fresh POST response`, { status: 200 })

    mockFetch
      .mockResolvedValueOnce(initialResponse)
      .mockResolvedValueOnce(prefetchedGetResponse)
      .mockResolvedValueOnce(postResponse)

    await fetchWrapper(baseUrl)
    await sleep()

    const nextUrl = sortUrlParams(`${baseUrl}&handle=123&offset=456`)
    const result = await fetchWrapper(nextUrl, { method: `POST` })

    expect(await result.text()).toBe(`fresh POST response`)
    expect(mockFetch).toHaveBeenCalledTimes(3)
    expect(mockFetch).toHaveBeenNthCalledWith(
      3,
      nextUrl,
      expect.objectContaining({ method: `POST` })
    )
  })

  describe(`headers of a prefetched response`, () => {
    async function prefetchUnder(
      fetchWrapper: typeof fetch,
      headers: Record<string, string>
    ) {
      mockFetch
        .mockResolvedValueOnce(
          new Response(`initial chunk`, {
            status: 200,
            headers: responseHeaders({
              [SHAPE_HANDLE_HEADER]: `123`,
              [CHUNK_LAST_OFFSET_HEADER]: `456`,
            }),
          })
        )
        .mockResolvedValueOnce(
          new Response(`fetched with the old token`, { status: 200 })
        )
        .mockResolvedValue(
          new Response(`fetched with the new token`, { status: 200 })
        )
      await fetchWrapper(baseUrl, { headers })
      await sleep()
      return sortUrlParams(`${baseUrl}&handle=123&offset=456`)
    }

    it(`should not consume a response prefetched under other headers`, async () => {
      const fetchWrapper = createFetchWithChunkBuffer(mockFetch)
      const nextUrl = await prefetchUnder(fetchWrapper, {
        Authorization: `Bearer old`,
      })

      const result = await fetchWrapper(nextUrl, {
        headers: { Authorization: `Bearer new` },
      })

      expect(await result.text()).toBe(`fetched with the new token`)
      expect(mockFetch).toHaveBeenLastCalledWith(
        nextUrl,
        expect.objectContaining({ headers: { Authorization: `Bearer new` } })
      )
    })

    it(`should still consume a response prefetched under the same headers`, async () => {
      const fetchWrapper = createFetchWithChunkBuffer(mockFetch)
      const nextUrl = await prefetchUnder(fetchWrapper, {
        Authorization: `Bearer same`,
      })

      const result = await fetchWrapper(nextUrl, {
        // another case for the name, the same request
        headers: { authorization: `Bearer same` },
      })

      expect(await result.text()).toBe(`fetched with the old token`)
      // the initial request and the prefetch, nothing fetched afresh
      expect(mockFetch).toHaveBeenCalledTimes(2)
    })
  })

  it(`should not prefetch a snapshot (subset) GET response`, async () => {
    const fetchWrapper = createFetchWithChunkBuffer(mockFetch)
    const snapshotUrl = `https://example.com/v1/shape?table=foo&subset__limit=50`
    const snapshotResponse = new Response(`[]`, {
      status: 200,
      headers: responseHeaders({
        [SHAPE_HANDLE_HEADER]: `123`,
        [CHUNK_LAST_OFFSET_HEADER]: `456`,
        [SNAPSHOT_HEADER]: `true`,
      }),
    })

    mockFetch.mockResolvedValueOnce(snapshotResponse)

    const result = await fetchWrapper(snapshotUrl)
    await sleep()

    expect(result).toBe(snapshotResponse)
    // Snapshot responses must not trigger a speculative prefetch of the
    // same subset filters — the client only ever issues one such request.
    expect(mockFetch).toHaveBeenCalledTimes(1)
  })

  it(`should prefetch when the snapshot header is not true`, async () => {
    const fetchWrapper = createFetchWithChunkBuffer(mockFetch)
    const initialResponse = new Response(`initial chunk`, {
      status: 200,
      headers: responseHeaders({
        [SHAPE_HANDLE_HEADER]: `123`,
        [CHUNK_LAST_OFFSET_HEADER]: `456`,
        [SNAPSHOT_HEADER]: `false`,
      }),
    })
    const nextResponse = new Response(`next chunk`, { status: 200 })

    mockFetch.mockResolvedValueOnce(initialResponse)
    mockFetch.mockResolvedValueOnce(nextResponse)

    const result = await fetchWrapper(baseUrl)

    expect(result).toBe(initialResponse)
    const nextUrl = sortUrlParams(`${baseUrl}&handle=123&offset=456`)
    expect(mockFetch).toHaveBeenCalledWith(nextUrl, expect.anything())
  })

  it(`should not prefetch for non-GET requests`, async () => {
    const fetchWrapper = createFetchWithChunkBuffer(mockFetch)
    const postResponse = new Response(`snapshot chunk`, {
      status: 200,
      headers: responseHeaders({
        [SHAPE_HANDLE_HEADER]: `123`,
        [CHUNK_LAST_OFFSET_HEADER]: `456`,
      }),
    })

    mockFetch.mockResolvedValueOnce(postResponse)

    const result = await fetchWrapper(baseUrl, { method: `POST` })
    await sleep()

    expect(result).toBe(postResponse)
    expect(mockFetch).toHaveBeenCalledTimes(1)
    expect(mockFetch).toHaveBeenCalledWith(
      baseUrl,
      expect.objectContaining({ method: `POST` })
    )
  })

  it(`should abort prefetch queue when POST arrives and let subsequent GET start fresh`, async () => {
    const fetchWrapper = createFetchWithChunkBuffer(mockFetch)
    const initialResponse = new Response(`initial`, {
      status: 200,
      headers: responseHeaders({
        [SHAPE_HANDLE_HEADER]: `123`,
        [CHUNK_LAST_OFFSET_HEADER]: `456`,
      }),
    })
    const prefetchedResponse = new Response(`prefetched`, { status: 200 })
    const postResponse = new Response(`post`, { status: 200 })
    const freshGetResponse = new Response(`fresh get`, {
      status: 200,
      headers: responseHeaders({
        [SHAPE_HANDLE_HEADER]: `123`,
        [CHUNK_LAST_OFFSET_HEADER]: `789`,
      }),
    })

    mockFetch
      .mockResolvedValueOnce(initialResponse)
      .mockResolvedValueOnce(prefetchedResponse)
      .mockResolvedValueOnce(postResponse)
      .mockResolvedValueOnce(freshGetResponse)

    // GET triggers prefetch of nextUrl
    await fetchWrapper(baseUrl)
    await sleep()

    // POST clears the queue
    await fetchWrapper(baseUrl, { method: `POST` })

    // Subsequent GET should make a fresh request, not consume stale prefetch
    const nextUrl = sortUrlParams(`${baseUrl}&handle=123&offset=456`)
    const result = await fetchWrapper(nextUrl)
    expect(await result.text()).toBe(`fresh get`)
  })

  it(`should detect method from Request object input`, async () => {
    const fetchWrapper = createFetchWithChunkBuffer(mockFetch)
    const postResponse = new Response(`post response`, { status: 200 })
    mockFetch.mockResolvedValueOnce(postResponse)

    const request = new Request(baseUrl, { method: `POST` })
    const result = await fetchWrapper(request)
    await sleep()

    expect(result).toBe(postResponse)
    expect(mockFetch).toHaveBeenCalledTimes(1)
  })

  it(`should handle lowercase method in init`, async () => {
    const fetchWrapper = createFetchWithChunkBuffer(mockFetch)
    const response = new Response(`ok`, { status: 200 })
    mockFetch.mockResolvedValueOnce(response)

    const result = await fetchWrapper(baseUrl, { method: `post` })
    await sleep()

    expect(result).toBe(response)
    expect(mockFetch).toHaveBeenCalledTimes(1)
  })

  it(`should stop and resume prefetching after reaching maxChunksToPrefetch`, async () => {
    const maxPrefetchNum = 2
    const fetchWrapper = createFetchWithChunkBuffer(mockFetch, {
      maxChunksToPrefetch: maxPrefetchNum,
    })

    const responses = Array.from(
      // initial + prefetched + next prefetch after one consumed
      { length: 1 + maxPrefetchNum + 1 },
      (_, idx) =>
        new Response(`next chunk`, {
          status: 200,
          headers: responseHeaders({
            [SHAPE_HANDLE_HEADER]: `123`,
            [CHUNK_LAST_OFFSET_HEADER]: `${idx}`,
          }),
        })
    )
    responses.forEach((response) => mockFetch.mockResolvedValueOnce(response))

    // First request should trigger one prefetch
    await fetchWrapper(baseUrl)
    await sleep()

    // Check fetch call count: 1 for initial, maxPrefetchNum for prefetch
    expect(mockFetch).toHaveBeenCalledTimes(1 + maxPrefetchNum)
    expect(mockFetch).toHaveBeenNthCalledWith(
      2,
      sortUrlParams(`${baseUrl}&handle=123&offset=0`),
      expect.anything()
    )
    expect(mockFetch).toHaveBeenNthCalledWith(
      3,
      sortUrlParams(`${baseUrl}&handle=123&offset=1`),
      expect.anything()
    )

    // Second request consumes one of the prefetched responses and
    // next one fires up
    await fetchWrapper(sortUrlParams(`${baseUrl}&handle=123&offset=0`))
    await sleep()
    expect(mockFetch).toHaveBeenCalledTimes(1 + maxPrefetchNum + 1)
    expect(mockFetch).toHaveBeenNthCalledWith(
      4,
      sortUrlParams(`${baseUrl}&handle=123&offset=2`),
      expect.anything()
    )
  })

  it(`should stop prefetching as soon as responses are not advancing`, async () => {
    const fetchWrapper = createFetchWithChunkBuffer(mockFetch)
    const initialResponse = new Response(`initial chunk`, {
      status: 200,
      headers: responseHeaders({
        [SHAPE_HANDLE_HEADER]: `123`,
        [CHUNK_LAST_OFFSET_HEADER]: `456`,
      }),
    })

    const nextResponse = new Response(`next chunk`, {
      status: 200,
      headers: responseHeaders({
        [SHAPE_HANDLE_HEADER]: `123`,
        [CHUNK_LAST_OFFSET_HEADER]: `456`,
      }),
    })

    mockFetch.mockResolvedValueOnce(initialResponse)
    mockFetch.mockResolvedValueOnce(nextResponse)
    // mockFetch.mockResolvedValueOnce(nextResponse)

    const result = await fetchWrapper(baseUrl)
    expect(result).toBe(initialResponse)

    // fetch the next chunk as well
    const nextUrl = sortUrlParams(`${baseUrl}&handle=123&offset=456`)
    const nextResult = await fetchWrapper(nextUrl)
    expect(nextResult).toBe(nextResponse)

    expect(mockFetch).toHaveBeenNthCalledWith(1, baseUrl)
    expect(mockFetch).toHaveBeenNthCalledWith(2, nextUrl, expect.anything())
    expect(mockFetch).toHaveBeenCalledTimes(2)
  })

  it(`should not prefetch if response is not ok`, async () => {
    const fetchWrapper = createFetchWithChunkBuffer(mockFetch)
    const mockErrorResponse = new Response(`error`, {
      status: 500,
    })

    mockFetch.mockResolvedValueOnce(mockErrorResponse)

    const result = await fetchWrapper(baseUrl)
    expect(result).toBe(mockErrorResponse)

    // Ensure no prefetch was attempted
    expect(mockFetch).toHaveBeenCalledTimes(1)
  })

  it(`should handle failed prefetch attempts gracefully`, async () => {
    const fetchWrapper = createFetchWithChunkBuffer(mockFetch)

    const initialResponse = new Response(`initial chunk`, {
      status: 200,
      headers: responseHeaders({
        [SHAPE_HANDLE_HEADER]: `123`,
        [CHUNK_LAST_OFFSET_HEADER]: `456`,
      }),
    })

    mockFetch.mockResolvedValueOnce(initialResponse)
    mockFetch.mockRejectedValueOnce(new Error(`Prefetch failed`))

    const result = await fetchWrapper(baseUrl)
    expect(result).toBe(initialResponse)

    // Prefetch should have been attempted but failed
    const nextUrl = sortUrlParams(`${baseUrl}&handle=123&offset=456`)

    expect(mockFetch).toHaveBeenCalledWith(nextUrl, expect.anything())

    // One for the main request, one for the prefetch
    expect(mockFetch).toHaveBeenCalledTimes(2)
  })

  it(`should clear and abort prefetches on new entry`, async () => {
    const fetchWrapper = createFetchWithChunkBuffer(mockFetch, {
      maxChunksToPrefetch: 2,
    })

    Array.from({ length: 10 }, (_, idx) =>
      mockFetch.mockImplementationOnce(async () => {
        await sleep()
        return new Response(`chunk`, {
          status: 200,
          headers: responseHeaders({
            [SHAPE_HANDLE_HEADER]: `123`,
            [CHUNK_LAST_OFFSET_HEADER]: `${idx}`,
          }),
        })
      })
    )

    await fetchWrapper(baseUrl)

    // main + one prefetch - second prefetch not yet done
    expect(mockFetch).toHaveBeenCalledTimes(2)

    // requesting a different path should clear the prefetches
    const altUrl = `${baseUrl}_alt`
    await fetchWrapper(altUrl)
    await sleep()

    // main + 2 prefetches of new URL
    expect(mockFetch).toHaveBeenCalledTimes(5)

    // should have called the base + prefetch of base
    expect(mockFetch).toHaveBeenNthCalledWith(1, baseUrl)
    expect(mockFetch).toHaveBeenNthCalledWith(
      2,
      sortUrlParams(`${baseUrl}&handle=123&offset=0`),
      expect.anything()
    )

    // once interrupted it should have called the alt + the 2 prefetches
    expect(mockFetch).toHaveBeenNthCalledWith(3, altUrl)
    expect(mockFetch).toHaveBeenNthCalledWith(
      4,
      sortUrlParams(`${altUrl}&handle=123&offset=2`),
      expect.anything()
    )
    expect(mockFetch).toHaveBeenNthCalledWith(
      5,
      sortUrlParams(`${altUrl}&handle=123&offset=3`),
      expect.anything()
    )
  })

  it(`should respect wrapped client's aborter`, async () => {
    const fetchWrapper = createFetchWithChunkBuffer(mockFetch, {
      maxChunksToPrefetch: 2,
    })

    Array.from({ length: 10 }, (_, idx) =>
      mockFetch.mockImplementationOnce(async () => {
        await sleep()
        return new Response(`chunk`, {
          status: 200,
          headers: responseHeaders({
            [SHAPE_HANDLE_HEADER]: `123`,
            [CHUNK_LAST_OFFSET_HEADER]: `${idx}`,
          }),
        })
      })
    )

    const aborter = new AbortController()
    await fetchWrapper(baseUrl, { signal: aborter.signal })

    // main + one prefetch - second prefetch not yet done
    expect(mockFetch).toHaveBeenCalledTimes(2)
    aborter.abort()
    await sleep(10)

    // no new prefetches since main request was aborted
    expect(mockFetch).toHaveBeenCalledTimes(2)
  })

  it(`should not return aborted prefetch requests after queue is cleared`, async () => {
    // This test verifies the fix for issue #3460:
    // When a page is hidden, the stream pauses and aborts in-flight prefetches.
    // Previously, the aborted promises stayed in the queue and would be returned
    // on resume, causing the stream to incorrectly interpret the abort as fatal.
    const fetchWrapper = createFetchWithChunkBuffer(mockFetch, {
      maxChunksToPrefetch: 1,
    })

    const nextUrl = sortUrlParams(`${baseUrl}&handle=123&offset=0`)

    // Initial response that triggers prefetch of nextUrl
    const initialResponse = new Response(`initial chunk`, {
      status: 200,
      headers: responseHeaders({
        [SHAPE_HANDLE_HEADER]: `123`,
        [CHUNK_LAST_OFFSET_HEADER]: `0`,
      }),
    })

    // Fresh response for when we request nextUrl after clearing queue
    const freshResponse = new Response(`fresh chunk`, {
      status: 200,
      headers: responseHeaders({
        [SHAPE_HANDLE_HEADER]: `123`,
        [CHUNK_LAST_OFFSET_HEADER]: `1`,
      }),
    })

    // Create an outer abort controller that we'll use to abort the stream
    // This simulates ShapeStream's #requestAbortController
    const outerAbortController = new AbortController()

    mockFetch
      .mockResolvedValueOnce(initialResponse) // Initial request
      .mockImplementationOnce((_url, init) => {
        // Return a promise that hangs until the signal is aborted
        const signal = init?.signal as AbortSignal | undefined
        return new Promise<Response>((_, reject) => {
          if (signal) {
            signal.addEventListener(`abort`, () => {
              reject(new DOMException(`Aborted`, `AbortError`))
            })
          }
        })
      })
      .mockResolvedValueOnce(freshResponse) // Fresh request after queue cleared

    // Make initial request with abort signal - this triggers prefetch of nextUrl
    // The prefetch inherits the signal through chainAborter
    await fetchWrapper(baseUrl, { signal: outerAbortController.signal })
    await sleep() // Let prefetch start

    expect(mockFetch).toHaveBeenCalledTimes(2)
    expect(mockFetch).toHaveBeenNthCalledWith(2, nextUrl, expect.anything())

    // Simulate page hidden: abort the stream's request controller
    // This propagates through chainAborter to abort the prefetch's internal aborter
    outerAbortController.abort()
    await sleep()

    // Simulate page visible: request the same URL that was being prefetched
    // Before fix: this would return the aborted promise and throw
    // After fix: consume() checks aborter.signal.aborted and makes a fresh request
    const result = await fetchWrapper(nextUrl)

    expect(result).toBe(freshResponse)
    // 4 calls: initial, prefetch (aborted), fresh request, new prefetch triggered by fresh response
    expect(mockFetch).toHaveBeenCalledTimes(4)
    expect(mockFetch).toHaveBeenNthCalledWith(3, nextUrl)
  })
})

describe(`createFetchWithConsumedMessages`, () => {
  const mockFetch = vi.fn()

  afterEach(() => {
    vi.resetAllMocks()
  })

  it(`should return the original response for status codes < 200`, async () => {
    const mockResponse = {
      status: 199,
      text: vi.fn(),
      headers: new Headers(),
    }
    mockFetch.mockResolvedValue(mockResponse)

    const enhancedFetch = createFetchWithConsumedMessages(mockFetch)
    const result = await enhancedFetch(`http://example.com`)

    expect(result).toBe(mockResponse)
    expect(mockResponse.text).not.toHaveBeenCalled()
  })

  it(`should return the original response for status codes with no body (201, 204, 205)`, async () => {
    const mockResponse = {
      status: 204,
      text: vi.fn(),
      headers: new Headers(),
    }
    mockFetch.mockResolvedValue(mockResponse)

    const enhancedFetch = createFetchWithConsumedMessages(mockFetch)
    const result = await enhancedFetch(`http://example.com`)

    expect(result).toBe(mockResponse)
    expect(mockResponse.text).not.toHaveBeenCalled()
  })

  it(`should consume the body and return a new Response for successful status codes`, async () => {
    const mockText = `response body`
    const mockHeaders = new Headers({ 'content-type': `text/plain` })
    const mockResponse = {
      status: 200,
      text: vi.fn().mockResolvedValue(mockText),
      headers: mockHeaders,
    }
    mockFetch.mockResolvedValue(mockResponse)

    const enhancedFetch = createFetchWithConsumedMessages(mockFetch)
    const result = await enhancedFetch(`http://example.com`)

    expect(result).not.toBe(mockResponse)
    expect(result.status).toBe(200)
    expect(await result.text()).toBe(mockText)
    expect(mockResponse.text).toHaveBeenCalled()
  })

  it(`should throw FetchError when reading body fails`, async () => {
    const mockError = new Error(`Failed to read body`)
    const mockHeaders = new Headers({ 'content-type': `text/plain` })
    const mockResponse = {
      status: 200,
      text: vi.fn().mockRejectedValue(mockError),
      headers: mockHeaders,
    }
    mockFetch.mockResolvedValue(mockResponse)

    const enhancedFetch = createFetchWithConsumedMessages(mockFetch)
    const url = `http://example.com`

    await expect(() => enhancedFetch(url)).rejects.toThrow(FetchError)

    try {
      await enhancedFetch(url)
    } catch (error) {
      expect(error).toBeInstanceOf(FetchError)
      if (error instanceof FetchError) {
        expect(error.status).toBe(200)
        expect(error.url).toBe(url)
        expect(error.headers).toEqual(Object.fromEntries(mockHeaders.entries()))
        expect(error.message).toBe(mockError.message)
      }
    }
  })

  it(`should handle non-Error rejection values when reading body`, async () => {
    const mockResponse = {
      status: 200,
      text: vi.fn().mockRejectedValue(`some error string`),
      headers: new Headers(),
    }
    mockFetch.mockResolvedValue(mockResponse)

    const enhancedFetch = createFetchWithConsumedMessages(mockFetch)
    const url = `http://example.com`

    await expect(() => enhancedFetch(url)).rejects.toThrow(FetchError)

    try {
      await enhancedFetch(url)
    } catch (error) {
      expect(error).toBeInstanceOf(FetchError)
      if (error instanceof FetchError) {
        expect(error.message).toBe(`some error string`)
      }
    }
  })

  it(`should handle unknown rejection values when reading body`, async () => {
    const mockResponse = {
      status: 200,
      text: vi.fn().mockRejectedValue({ some: `object` }),
      headers: new Headers(),
    }
    mockFetch.mockResolvedValue(mockResponse)

    const enhancedFetch = createFetchWithConsumedMessages(mockFetch)
    const url = `http://example.com`

    await expect(() => enhancedFetch(url)).rejects.toThrow(FetchError)

    try {
      await enhancedFetch(url)
    } catch (error) {
      expect(error).toBeInstanceOf(FetchError)
      if (error instanceof FetchError) {
        expect(error.message).toBe(`failed to read body`)
      }
    }
  })
  it(`should not flag a body that was already read as a FetchBodyReadError`, async () => {
    // A custom fetchClient that read the body before returning the response.
    // That can never succeed, so it must not be retried.
    const res = new Response(`[]`, {
      status: 200,
      headers: { 'electric-handle': `h1` },
    })
    await res.text()

    const error = await createFetchWithConsumedMessages(async () => res)(
      `http://example.com`
    ).catch((e) => e)

    expect(error).toBeInstanceOf(FetchError)
    expect(error).not.toBeInstanceOf(FetchBodyReadError)
    expect(error.status).toBe(200)
    expect(error.url).toBe(`http://example.com`)
    expect(error.headers[`electric-handle`]).toBe(`h1`)
    expect(error.message).toContain(`Body is unusable`)
  })

  it(`should not flag a locked body as a FetchBodyReadError`, async () => {
    const res = new Response(`[]`, { status: 200 })
    res.body!.getReader()

    const error = await createFetchWithConsumedMessages(async () => res)(
      `http://example.com`
    ).catch((e) => e)

    expect(error).toBeInstanceOf(FetchError)
    expect(error).not.toBeInstanceOf(FetchBodyReadError)
  })

  it(`should still flag a body that is cut off after its first bytes`, async () => {
    // Unread when the client started; marked used only once it fails.
    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(new TextEncoder().encode(`[`))
        controller.error(new TypeError(`terminated`))
      },
    })
    const res = new Response(body, { status: 200 })

    await expect(
      createFetchWithConsumedMessages(async () => res)(`http://example.com`)
    ).rejects.toBeInstanceOf(FetchBodyReadError)
  })
})

describe(`retryOnBodyReadFailure`, () => {
  const backoff = { initialDelay: 1, maxDelay: 2, multiplier: 2 }
  const signal = new AbortController().signal
  const bodyReadError = () =>
    new FetchBodyReadError(200, undefined, undefined, {}, `http://example.com`)

  afterEach(() => {
    vi.restoreAllMocks()
  })

  it(`should return the result when the operation succeeds`, async () => {
    const operation = vi.fn().mockResolvedValue(`ok`)
    await expect(
      retryOnBodyReadFailure(operation, backoff, signal)
    ).resolves.toBe(`ok`)
    expect(operation).toHaveBeenCalledTimes(1)
  })

  it(`should run the operation again after a body read failure`, async () => {
    const onFailedAttempt = vi.fn()
    const operation = vi
      .fn()
      .mockRejectedValueOnce(bodyReadError())
      .mockRejectedValueOnce(bodyReadError())
      .mockResolvedValue(`ok`)

    await expect(
      retryOnBodyReadFailure(operation, { ...backoff, onFailedAttempt }, signal)
    ).resolves.toBe(`ok`)
    expect(operation).toHaveBeenCalledTimes(3)
    expect(onFailedAttempt).toHaveBeenCalledTimes(2)
  })

  it(`should wait with backoff between attempts and honour Retry-After`, async () => {
    vi.spyOn(Math, `random`).mockReturnValue(0.5)
    const waits = recordTimeoutDelays()
    const operation = vi
      .fn()
      .mockRejectedValueOnce(
        new FetchBodyReadError(
          200,
          undefined,
          undefined,
          { 'retry-after': `7` },
          `http://example.com`
        )
      )
      .mockResolvedValue(`ok`)

    await retryOnBodyReadFailure(operation, backoff, signal)

    // 7s from the header, plus half of the 10% (700ms) Retry-After jitter.
    expect(waits).toEqual([7_350])
  })

  it(`should report the earliest time a retry may come, with its own Retry-After jitter`, async () => {
    vi.spyOn(Math, `random`).mockReturnValue(0.5)
    recordTimeoutDelays()
    const onRetryWait = vi.fn()
    const operation = vi
      .fn()
      .mockRejectedValueOnce(
        new FetchBodyReadError(
          200,
          undefined,
          undefined,
          { 'retry-after': `7` },
          `http://example.com`
        )
      )
      .mockRejectedValueOnce(bodyReadError())
      .mockResolvedValue(`ok`)

    await retryOnBodyReadFailure(operation, { ...backoff, onRetryWait }, signal)

    // 7s from the header and half of the 10% jitter; then nothing, as the
    // second failure carries no Retry-After.
    expect(onRetryWait.mock.calls).toEqual([
      [{ notBeforeMs: 7_350 }],
      [{ notBeforeMs: 0 }],
    ])
  })

  it(`should not retry errors that are not body read failures`, async () => {
    const onFailedAttempt = vi.fn()
    const failure = new FetchError(
      404,
      undefined,
      undefined,
      {},
      `http://example.com`
    )
    const operation = vi.fn().mockRejectedValue(failure)

    await expect(
      retryOnBodyReadFailure(operation, { ...backoff, onFailedAttempt }, signal)
    ).rejects.toBe(failure)
    expect(operation).toHaveBeenCalledTimes(1)
    expect(onFailedAttempt).not.toHaveBeenCalled()
  })

  it(`should give up and rethrow after maxRetries`, async () => {
    const failure = bodyReadError()
    const operation = vi.fn().mockRejectedValue(failure)

    await expect(
      retryOnBodyReadFailure(operation, { ...backoff, maxRetries: 2 }, signal)
    ).rejects.toBe(failure)
    expect(operation).toHaveBeenCalledTimes(3)
  })

  it(`should stop with FetchBackoffAbortError when the signal aborts the wait`, async () => {
    const controller = new AbortController()
    const operation = vi.fn().mockRejectedValue(bodyReadError())
    vi.spyOn(Math, `random`).mockReturnValue(0.999)
    // Long enough that only the abort can end the wait.
    const pending = retryOnBodyReadFailure(
      operation,
      { initialDelay: 60_000, maxDelay: 60_000, multiplier: 1 },
      controller.signal
    )

    await sleep(5)
    controller.abort()

    await expect(pending).rejects.toBeInstanceOf(FetchBackoffAbortError)
    expect(operation).toHaveBeenCalledTimes(1)
  })

  it(`should not start another attempt once the signal is already aborted`, async () => {
    const controller = new AbortController()
    const operation = vi.fn().mockImplementation(async () => {
      controller.abort()
      throw bodyReadError()
    })

    await expect(
      retryOnBodyReadFailure(operation, backoff, controller.signal)
    ).rejects.toBeInstanceOf(FetchBackoffAbortError)
    expect(operation).toHaveBeenCalledTimes(1)
  })

  describe(`around a request that has its own fetch backoff`, () => {
    const url = `http://example.com`

    // Calls alternate between a network failure and a body that is cut off.
    function failingRequest(options: {
      initialDelay: number
      maxDelay: number
      multiplier: number
      maxRetries: number
    }) {
      const controller = new AbortController()
      const fetchClient = vi.fn(async () => {
        if (fetchClient.mock.calls.length % 2 === 1) {
          throw new TypeError(`fetch failed`)
        }
        return new Response(
          new ReadableStream<Uint8Array>({
            start(controller) {
              controller.enqueue(new TextEncoder().encode(`[`))
              controller.error(new TypeError(`terminated`))
            },
          }),
          { status: 200 }
        )
      })
      const request = createFetchWithBackoff(fetchClient, options)
      const result = retryOnBodyReadFailure(
        async () =>
          consumeResponseBody(
            await request(url, { signal: controller.signal }),
            url,
            controller.signal
          ),
        options,
        controller.signal
      )
      return { fetchClient, result }
    }

    it(`should spend one retry budget on failed requests and cut-off bodies`, async () => {
      const { fetchClient, result } = failingRequest({
        ...backoff,
        maxRetries: 3,
      })

      await expect(result).rejects.toBeInstanceOf(FetchBodyReadError)

      // The first attempt plus three retries, however they failed.
      expect(fetchClient).toHaveBeenCalledTimes(4)
    })

    it(`should keep growing the backoff after a cut-off body`, async () => {
      vi.spyOn(Math, `random`).mockReturnValue(0.5)
      const waits = recordTimeoutDelays()
      const { result } = failingRequest({
        initialDelay: 100,
        maxDelay: 1_000,
        multiplier: 2,
        maxRetries: 4,
      })

      await expect(result).rejects.toBeInstanceOf(TypeError)

      // Half of the delay each time, which doubles on every retry.
      expect(waits).toEqual([50, 100, 200, 400])
    })
  })

  describe(`around a response that was prefetched`, () => {
    const baseUrl = `http://example.com/v1/shape?table=foo`
    const nextUrl = sortUrlParams(`${baseUrl}&handle=h1&offset=1_0`)

    // The first chunk has a next one, which the chunk buffer prefetches. Its
    // first fetch fails before the headers and its second is cut off in the
    // body, after which every fetch works.
    function prefetchedChunkThatFailsTwice(options: {
      initialDelay: number
      maxDelay: number
      multiplier: number
      maxRetries?: number
    }) {
      let nextChunkFetches = 0
      const fetchClient = vi.fn(async (input: RequestInfo | URL) => {
        if (input.toString() !== nextUrl) {
          return new Response(`[]`, {
            status: 200,
            headers: {
              [SHAPE_HANDLE_HEADER]: `h1`,
              [CHUNK_LAST_OFFSET_HEADER]: `1_0`,
            },
          })
        }
        nextChunkFetches++
        if (nextChunkFetches === 1) throw new TypeError(`fetch failed`)
        const headers = {
          [SHAPE_HANDLE_HEADER]: `h1`,
          [CHUNK_LAST_OFFSET_HEADER]: `2_0`,
          [CHUNK_UP_TO_DATE_HEADER]: ``,
        }
        if (nextChunkFetches > 2)
          return new Response(`[]`, { status: 200, headers })
        return new Response(
          new ReadableStream<Uint8Array>({
            start(controller) {
              controller.enqueue(new TextEncoder().encode(`[`))
              controller.error(new TypeError(`terminated`))
            },
          }),
          { status: 200, headers }
        )
      })
      const client = createFetchWithChunkBuffer(
        createFetchWithBackoff(fetchClient, options)
      )

      // A request for the next chunk, made after the first chunk was fetched.
      async function readNextChunk() {
        await client(baseUrl, { signal: new AbortController().signal })
        const controller = new AbortController()
        return retryOnBodyReadFailure(
          async () =>
            consumeResponseBody(
              await client(nextUrl, { signal: controller.signal }),
              nextUrl,
              controller.signal
            ),
          options,
          controller.signal
        )
      }
      return { fetchClient, readNextChunk }
    }

    it(`should spend the retries of the prefetch from the budget of the body`, async () => {
      const { fetchClient, readNextChunk } = prefetchedChunkThatFailsTwice({
        ...backoff,
        maxRetries: 1,
      })

      await expect(readNextChunk()).rejects.toBeInstanceOf(FetchBodyReadError)

      // The first chunk, the failed fetch of the next one and its retry. The
      // one retry of the budget is spent, so the cut-off body is not retried.
      expect(fetchClient).toHaveBeenCalledTimes(3)
    })

    it(`should carry the grown backoff of the prefetch over to the body`, async () => {
      vi.spyOn(Math, `random`).mockReturnValue(0.5)
      const waits = recordTimeoutDelays()
      const { readNextChunk } = prefetchedChunkThatFailsTwice({
        initialDelay: 100,
        maxDelay: 1_000,
        multiplier: 2,
      })

      await readNextChunk()

      // Half of 100 before the prefetch retried, then half of the 200 it grew
      // to before the body was read again.
      expect(waits).toEqual([50, 100])
    })
  })
})

describe(`consumeResponseBody size limit`, () => {
  const url = `https://example.com/v1/shape?offset=-1&token=secret-value`

  const streamedResponse = (chunks: Array<Uint8Array>) => {
    const cancel = vi.fn()
    let next = 0
    const body = new ReadableStream<Uint8Array>({
      pull(controller) {
        if (next === chunks.length) return controller.close()
        controller.enqueue(chunks[next++])
      },
      cancel,
    })
    return { response: new Response(body, { status: 200 }), cancel }
  }

  const bytes = (length: number) => new Uint8Array(length).fill(97)

  it(`returns a body of exactly the limit`, async () => {
    const { response } = streamedResponse([bytes(4), bytes(6)])

    const consumed = await consumeResponseBody(response, url, undefined, 10)

    await expect(consumed.text()).resolves.toBe(`a`.repeat(10))
  })

  it(`decodes a character split across two chunks`, async () => {
    const encoded = new TextEncoder().encode(`é`)
    const { response } = streamedResponse([
      encoded.slice(0, 1),
      encoded.slice(1),
    ])

    const consumed = await consumeResponseBody(response, url, undefined, 10)

    await expect(consumed.text()).resolves.toBe(`é`)
  })

  it(`stops reading and fails past the limit`, async () => {
    const { response, cancel } = streamedResponse([
      bytes(6),
      bytes(6),
      bytes(6),
    ])

    const error = await consumeResponseBody(response, url, undefined, 10).catch(
      (e) => e
    )

    expect(error).toBeInstanceOf(ResponseTooLargeError)
    expect(error.maxBytes).toBe(10)
    expect(error.message).not.toContain(`secret-value`)
    expect(cancel).toHaveBeenCalledOnce()
  })

  it(`is not retried like a body cut off mid-read`, async () => {
    const operation = vi.fn(() =>
      consumeResponseBody(
        streamedResponse([bytes(11)]).response,
        url,
        undefined,
        10
      )
    )

    await expect(
      retryOnBodyReadFailure(
        operation,
        BackoffDefaults,
        new AbortController().signal
      )
    ).rejects.toBeInstanceOf(ResponseTooLargeError)
    expect(operation).toHaveBeenCalledOnce()
  })

  it(`refuses a declared length over the limit when the body cannot be streamed`, async () => {
    const text = vi.fn(async () => `a`.repeat(11))
    const response = {
      status: 200,
      body: null,
      bodyUsed: false,
      headers: new Headers({ 'content-length': `11` }),
      text,
    } as unknown as Response

    await expect(
      consumeResponseBody(response, url, undefined, 10)
    ).rejects.toBeInstanceOf(ResponseTooLargeError)
    expect(text).not.toHaveBeenCalled()
  })

  it(`fails a body that cannot be streamed once it is known to be too long`, async () => {
    const response = {
      status: 200,
      body: null,
      bodyUsed: false,
      headers: new Headers(),
      text: async () => `a`.repeat(11),
    } as unknown as Response

    await expect(
      consumeResponseBody(response, url, undefined, 10)
    ).rejects.toBeInstanceOf(ResponseTooLargeError)
  })

  it.each([
    [`two-byte characters`, `é`.repeat(6)],
    [`surrogate pairs`, `😀`.repeat(3)],
  ])(
    `counts UTF-8 bytes of %s when the body cannot be streamed`,
    async (_name, body) => {
      const response = {
        status: 200,
        body: null,
        bodyUsed: false,
        headers: new Headers(),
        text: async () => body,
      } as unknown as Response

      await expect(
        consumeResponseBody(response, url, undefined, 10)
      ).rejects.toBeInstanceOf(ResponseTooLargeError)
    }
  )

  it(`keeps a body that cannot be streamed when its UTF-8 bytes fit the limit`, async () => {
    const response = {
      status: 200,
      body: null,
      bodyUsed: false,
      headers: new Headers(),
      text: async () => `é`.repeat(5),
    } as unknown as Response

    const consumed = await consumeResponseBody(response, url, undefined, 10)

    expect(await consumed.text()).toBe(`é`.repeat(5))
  })
})

describe(`observeResponseBodyChunks`, () => {
  const encode = (text: string) => new TextEncoder().encode(text)

  function streamedResponse(chunks: string[], init?: ResponseInit) {
    return new Response(
      new ReadableStream<Uint8Array>({
        start(controller) {
          for (const chunk of chunks) controller.enqueue(encode(chunk))
          controller.close()
        },
      }),
      init
    )
  }

  it(`calls back once per chunk and passes the body through unchanged`, async () => {
    const onChunk = vi.fn()
    const res = observeResponseBodyChunks(
      streamedResponse([`a`, `: keep-alive\n\n`, `b`]),
      onChunk
    )

    expect(onChunk).not.toHaveBeenCalled()
    expect(await res.text()).toBe(`a: keep-alive\n\nb`)
    expect(onChunk).toHaveBeenCalledTimes(3)
  })

  it(`keeps the status, status text and headers`, () => {
    const res = observeResponseBodyChunks(
      streamedResponse([`a`], {
        status: 203,
        statusText: `Non-Authoritative`,
        headers: { 'electric-handle': `h1` },
      }),
      () => {}
    )

    expect(res.status).toBe(203)
    expect(res.statusText).toBe(`Non-Authoritative`)
    expect(res.headers.get(`electric-handle`)).toBe(`h1`)
  })

  it(`returns a response without a body as it is`, () => {
    const original = new Response(null, { status: 204 })

    expect(observeResponseBodyChunks(original, () => {})).toBe(original)
  })

  it(`cancels the underlying body when the consumer cancels`, async () => {
    const cancelled = vi.fn()
    const res = observeResponseBodyChunks(
      new Response(
        new ReadableStream<Uint8Array>({
          pull(controller) {
            controller.enqueue(encode(`a`))
          },
          cancel: cancelled,
        })
      ),
      () => {}
    )

    const reader = res.body!.getReader()
    await reader.read()
    await reader.cancel(`stop`)

    expect(cancelled).toHaveBeenCalledWith(`stop`)
  })

  it(`cancels a body that is stalled while the consumer waits for a chunk`, async () => {
    const cancelled = vi.fn()
    const res = observeResponseBodyChunks(
      new Response(
        new ReadableStream<Uint8Array>({
          pull: () => new Promise<void>(() => {}),
          cancel: cancelled,
        })
      ),
      () => {}
    )

    const reader = res.body!.getReader()
    const pendingRead = reader.read()
    await reader.cancel(`stop`)

    expect(cancelled).toHaveBeenCalledWith(`stop`)
    expect(await pendingRead).toEqual({ done: true, value: undefined })
  })

  describe(`with the signal of the request`, () => {
    // A source that ignores everything but a cancel, like the body of a
    // wedged connection.
    function wedgedBody(cancel: (reason?: unknown) => void) {
      return new Response(
        new ReadableStream<Uint8Array>({
          pull: () => new Promise<void>(() => {}),
          cancel,
        })
      )
    }

    it(`cancels the underlying body and fails a pending read when the signal aborts`, async () => {
      const cancelled = vi.fn()
      const controller = new AbortController()
      const res = observeResponseBodyChunks(
        wedgedBody(cancelled),
        () => {},
        controller.signal
      )
      const pendingRead = res.body!.getReader().read()

      controller.abort(`stop`)

      await expect(pendingRead).rejects.toBe(`stop`)
      expect(cancelled).toHaveBeenCalledWith(`stop`)
    })

    it(`cancels the underlying body of a request that was already aborted`, async () => {
      const cancelled = vi.fn()
      const controller = new AbortController()
      controller.abort(`stop`)
      const res = observeResponseBodyChunks(
        wedgedBody(cancelled),
        () => {},
        controller.signal
      )

      expect(cancelled).toHaveBeenCalledWith(`stop`)
      await expect(res.body!.getReader().read()).rejects.toBe(`stop`)
    })

    it(`stops listening to the signal once the body has ended`, async () => {
      const controller = new AbortController()
      const removeListener = vi.spyOn(controller.signal, `removeEventListener`)
      const res = observeResponseBodyChunks(
        streamedResponse([`a`]),
        () => {},
        controller.signal
      )

      expect(await res.text()).toBe(`a`)

      expect(removeListener).toHaveBeenCalledWith(`abort`, expect.any(Function))
    })

    it(`stops listening to the signal once the consumer cancels`, async () => {
      const controller = new AbortController()
      const removeListener = vi.spyOn(controller.signal, `removeEventListener`)
      const res = observeResponseBodyChunks(
        wedgedBody(() => {}),
        () => {},
        controller.signal
      )

      await res.body!.cancel(`stop`)

      expect(removeListener).toHaveBeenCalledWith(`abort`, expect.any(Function))
    })

    it(`stops listening to the signal once the body fails`, async () => {
      const controller = new AbortController()
      const removeListener = vi.spyOn(controller.signal, `removeEventListener`)
      const res = observeResponseBodyChunks(
        new Response(
          new ReadableStream<Uint8Array>({
            pull() {
              throw new Error(`connection reset`)
            },
          })
        ),
        () => {},
        controller.signal
      )

      await expect(res.text()).rejects.toThrow(`connection reset`)

      expect(removeListener).toHaveBeenCalledWith(`abort`, expect.any(Function))
    })
  })

  it(`passes a body read error on to the consumer`, async () => {
    const res = observeResponseBodyChunks(
      new Response(
        new ReadableStream<Uint8Array>({
          pull() {
            throw new Error(`connection reset`)
          },
        })
      ),
      () => {}
    )

    await expect(res.text()).rejects.toThrow(`connection reset`)
  })
})

function sortUrlParams(url: string): string {
  const parsedUrl = new URL(url)
  parsedUrl.searchParams.sort()
  return parsedUrl.toString()
}
