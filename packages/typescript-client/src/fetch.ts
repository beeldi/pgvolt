import {
  CHUNK_LAST_OFFSET_HEADER,
  CHUNK_UP_TO_DATE_HEADER,
  EXPIRED_HANDLE_QUERY_PARAM,
  LIVE_CACHE_BUSTER_HEADER,
  LIVE_QUERY_PARAM,
  OFFSET_QUERY_PARAM,
  SHAPE_SCHEMA_HEADER,
  SHAPE_HANDLE_HEADER,
  SHAPE_HANDLE_QUERY_PARAM,
  SNAPSHOT_HEADER,
  SUBSET_PARAM_LIMIT,
  SUBSET_PARAM_OFFSET,
  SUBSET_PARAM_ORDER_BY,
  SUBSET_PARAM_WHERE,
  SUBSET_PARAM_WHERE_PARAMS,
} from './constants'
import {
  FetchError,
  FetchBodyReadError,
  FetchBackoffAbortError,
  MissingHeadersError,
  ResponseTooLargeError,
} from './error'
import { DEFAULT_MAX_RESPONSE_BYTES, readBodyUpTo } from './response-body'

// Some specific 4xx and 5xx HTTP status codes that we definitely
// want to retry
const HTTP_RETRY_STATUS_CODES = [429]

export interface BackoffOptions {
  /**
   * Initial delay before retrying in milliseconds
   */
  initialDelay: number
  /**
   * Maximum retry delay in milliseconds
   * After reaching this, delay stays constant (e.g., retry every 60s)
   */
  maxDelay: number
  multiplier: number
  onFailedAttempt?: () => void
  debug?: boolean
  /**
   * Maximum number of retry attempts before giving up.
   * Set to Infinity (default) for indefinite retries - needed for offline scenarios
   * where clients may go offline and come back later.
   */
  maxRetries?: number
}

/**
 * The backoff options as the stream passes them down to the fetch layers. The
 * hook is for the stream's own use, so it is not part of `BackoffOptions`.
 */
export interface InternalBackoffOptions extends BackoffOptions {
  /**
   * Called before each wait to retry, with the earliest time the retry may
   * come, in milliseconds from now: the time the server asked for with
   * `Retry-After` plus the spread this retry drew on top of it (0 if the
   * server did not ask for anything). The wait itself can be longer, when the
   * client's own backoff is. It carries the spread so that a caller that
   * restarts the request at this time does not undo the spread between
   * clients that were given the same `Retry-After`.
   */
  onRetryWait?: (wait: { notBeforeMs: number }) => void
}

export const BackoffDefaults = {
  initialDelay: 1_000,
  maxDelay: 32_000,
  multiplier: 2,
  maxRetries: Infinity, // Retry forever - clients may go offline and come back
}

/**
 * Parse Retry-After header value and return delay in milliseconds
 * Supports both delta-seconds format and HTTP-date format
 * Returns 0 if header is not present or invalid
 */
export function parseRetryAfterHeader(retryAfter: string | undefined): number {
  if (!retryAfter) return 0

  // Try parsing as seconds (delta-seconds format)
  const retryAfterSec = Number(retryAfter)
  if (Number.isFinite(retryAfterSec) && retryAfterSec > 0) {
    return retryAfterSec * 1000
  }

  // Try parsing as HTTP-date
  const retryDate = Date.parse(retryAfter)
  if (!isNaN(retryDate)) {
    // Handle clock skew: clamp to non-negative, cap at reasonable max
    const deltaMs = retryDate - Date.now()
    return Math.max(0, Math.min(deltaMs, 3600_000)) // Cap at 1 hour
  }

  return 0
}

// Positive jitter added when the server sends Retry-After: up to 10% of the
// requested delay, capped at 5 seconds.
const RETRY_AFTER_JITTER_RATIO = 0.1
const RETRY_AFTER_JITTER_MAX_MS = 5_000

async function abortableSleep(
  waitMs: number,
  signal?: AbortSignal
): Promise<void> {
  if (waitMs <= 0) return
  if (signal?.aborted) throw new FetchBackoffAbortError()

  await new Promise<void>((resolve, reject) => {
    let settled = false
    const done = (err?: Error) => {
      if (settled) return
      settled = true
      clearTimeout(timer)
      signal?.removeEventListener(`abort`, onAbort)
      err ? reject(err) : resolve()
    }
    const onAbort = () => done(new FetchBackoffAbortError())
    const timer = setTimeout(() => done(), waitMs)
    signal?.addEventListener(`abort`, onAbort, { once: true })
    if (signal?.aborted) onAbort()
  })
}

/**
 * A watchdog on a request, told when that request starts and stops waiting to
 * retry. `resume` follows `pause` even when the wait is cut short by an abort.
 */
export interface RetryWaitWatcher {
  pause: () => void
  resume: () => void
}

const retryWaitWatchers = new WeakMap<AbortSignal, RetryWaitWatcher>()

/**
 * Tells `watcher` whenever a request sent with `signal` waits to retry, so a
 * timeout on that request does not count the wait against the request.
 * Returns the function that stops the watching. It is keyed by signal, not
 * passed in the request init, for the same reason as `sharedRetryWaiters`.
 */
export function watchRetryWaits(
  signal: AbortSignal | null | undefined,
  watcher: RetryWaitWatcher
): () => void {
  if (!signal) return () => {}
  retryWaitWatchers.set(signal, watcher)
  return () => {
    if (retryWaitWatchers.get(signal) === watcher) {
      retryWaitWatchers.delete(signal)
    }
  }
}

/**
 * Returns a function that waits before the next attempt of a failed request.
 * It rethrows the error once `maxRetries` is exhausted, and throws
 * `FetchBackoffAbortError` if `signal` aborts the wait. The attempt count and
 * the growing delay live in the closure, so make one per retry loop.
 */
function createRetryWaiter(backoffOptions: InternalBackoffOptions) {
  const {
    initialDelay,
    maxDelay,
    multiplier,
    debug = false,
    maxRetries = Infinity,
    onRetryWait,
  } = backoffOptions
  let delay = initialDelay
  let attempt = 0

  return async (e: unknown, signal?: AbortSignal | null): Promise<void> => {
    // Check max retries
    attempt++
    if (attempt > maxRetries) {
      if (debug) {
        console.log(`Max retries reached (${attempt}/${maxRetries}), giving up`)
      }
      throw e
    }

    // Calculate wait time honoring server-driven backoff as a floor
    // Precedence: max(serverMinimum, min(clientMaxDelay, backoffWithJitter))

    // 1. Parse server-provided Retry-After (if present)
    const serverMinimumMs =
      e instanceof FetchError && e.headers
        ? parseRetryAfterHeader(e.headers[`retry-after`])
        : 0

    // 2. Calculate client backoff with full jitter strategy
    // Full jitter: random_between(0, min(cap, exponential_backoff))
    // See: https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
    const jitter = Math.random() * delay // random value between 0 and current delay
    const clientBackoffMs = Math.min(jitter, maxDelay) // cap at maxDelay

    // 3. Add bounded positive jitter on top of the server minimum, so clients
    // that got the same Retry-After don't retry in lockstep. It only adds, so
    // a retry never comes before the time the server asked for.
    const retryAfterJitterMs =
      serverMinimumMs > 0
        ? Math.random() *
          Math.min(
            serverMinimumMs * RETRY_AFTER_JITTER_RATIO,
            RETRY_AFTER_JITTER_MAX_MS
          )
        : 0

    onRetryWait?.({ notBeforeMs: serverMinimumMs + retryAfterJitterMs })

    // 4. Server minimum is the floor, client cap is the ceiling
    const waitMs =
      Math.max(serverMinimumMs, clientBackoffMs) + retryAfterJitterMs

    if (debug) {
      const source = serverMinimumMs > 0 ? `server+client` : `client`
      console.log(
        `Retry attempt #${attempt} after ${waitMs}ms (${source}, serverMin=${serverMinimumMs}ms, clientBackoff=${clientBackoffMs}ms, retryAfterJitter=${retryAfterJitterMs}ms)`
      )
    }

    // Wait for the calculated duration, but let stream-level aborts
    // interrupt the backoff immediately (e.g. system wake reconnects). A
    // watchdog on this request stands still meanwhile: the wait is deliberate,
    // and a Retry-After can be longer than the watchdog's timeout.
    const watcher = signal ? retryWaitWatchers.get(signal) : undefined
    watcher?.pause()
    try {
      await abortableSleep(waitMs, signal ?? undefined)
    } finally {
      watcher?.resume()
    }

    // Increase the delay for the next attempt (capped at maxDelay)
    delay = Math.min(delay * multiplier, maxDelay)
  }
}

type RetryWaiter = ReturnType<typeof createRetryWaiter>

/**
 * The retry waiter of a request whose body read is retried, by the signal of
 * that request, so the fetch backoff inside it draws on the same budget and
 * delay. It is keyed by signal rather than passed in the request init because
 * the chunk buffer copies the init for its prefetches, which must not share:
 * a prefetch keeps its own budget on its own signal until its response is
 * consumed, and `waitersBehind` then carries that budget to the request.
 */
const sharedRetryWaiters = new WeakMap<AbortSignal, RetryWaiter>()

/**
 * The retry waiter behind a response, which is the one that fetched it and
 * counted the retries it took, and then behind the error of a body that failed
 * to read. A request that consumes a prefetched response was not the one that
 * fetched it, so this is how the retries of the prefetch reach the retries of
 * the body read.
 */
const waitersBehind = new WeakMap<object, RetryWaiter>()

export function createFetchWithBackoff(
  fetchClient: typeof fetch,
  backoffOptions: InternalBackoffOptions = BackoffDefaults
): typeof fetch {
  const { onFailedAttempt } = backoffOptions
  return async (...args: Parameters<typeof fetch>): Promise<Response> => {
    const url = args[0]
    const options = args[1]

    const waitBeforeRetry =
      (options?.signal && sharedRetryWaiters.get(options.signal)) ||
      createRetryWaiter(backoffOptions)

    while (true) {
      try {
        const result = await fetchClient(...args)
        if (result.ok) {
          waitersBehind.set(result, waitBeforeRetry)
          return result
        }

        const err = await FetchError.fromResponse(result, url.toString())

        throw err
      } catch (e) {
        onFailedAttempt?.()
        if (options?.signal?.aborted) {
          throw new FetchBackoffAbortError()
        } else if (
          e instanceof FetchError &&
          !HTTP_RETRY_STATUS_CODES.includes(e.status) &&
          e.status >= 400 &&
          e.status < 500
        ) {
          // Any client errors cannot be backed off on, leave it to the caller to handle.
          throw e
        } else {
          await waitBeforeRetry(e, options?.signal)
        }
      }
    }
  }
}

/**
 * Runs `operation`, which sends a request with `signal` and reads its body,
 * and runs it again when reading the body fails (`FetchBodyReadError`). Any
 * other error is rethrown.
 *
 * The retries share one budget and one backoff with the `createFetchWithBackoff`
 * retries of the requests inside `operation`: `maxRetries` holds for the whole
 * request, and the delay keeps growing across a failed body. When the failed
 * body came from a prefetched response, which was fetched, and retried, before
 * this request existed, they carry on from the retries that response took.
 */
export async function retryOnBodyReadFailure<T>(
  operation: () => Promise<T>,
  backoffOptions: InternalBackoffOptions,
  signal: AbortSignal
): Promise<T> {
  let waitBeforeRetry = createRetryWaiter(backoffOptions)
  sharedRetryWaiters.set(signal, waitBeforeRetry)

  try {
    while (true) {
      try {
        return await operation()
      } catch (e) {
        if (!(e instanceof FetchBodyReadError)) throw e
        backoffOptions.onFailedAttempt?.()
        if (signal.aborted) throw new FetchBackoffAbortError()
        // For a response this request fetched itself that is the waiter it
        // already has.
        waitBeforeRetry = waitersBehind.get(e) ?? waitBeforeRetry
        sharedRetryWaiters.set(signal, waitBeforeRetry)
        await waitBeforeRetry(e, signal)
      }
    }
  } finally {
    sharedRetryWaiters.delete(signal)
  }
}

const NO_BODY_STATUS_CODES = [201, 204, 205]

export async function consumeResponseBody(
  res: Response,
  url: string,
  signal?: AbortSignal,
  maxBytes: number = DEFAULT_MAX_RESPONSE_BYTES,
  onChunk?: () => void
): Promise<Response> {
  // Checked before reading, because a body that fails half way is marked used
  // too. A body a custom fetchClient already read can never be read again, so
  // retrying cannot help and the error must end the stream.
  const unreadable = res.bodyUsed || !!res.body?.locked

  try {
    if (res.status < 200 || NO_BODY_STATUS_CODES.includes(res.status)) {
      return res
    }

    const { text, complete } = await readBodyUpTo(res, maxBytes, onChunk)
    if (!complete) throw new ResponseTooLargeError(url, maxBytes)
    return new Response(text, res)
  } catch (err) {
    if (err instanceof ResponseTooLargeError) throw err
    if (signal?.aborted) {
      throw new FetchBackoffAbortError()
    }

    const ErrorClass = unreadable ? FetchError : FetchBodyReadError
    const error = new ErrorClass(
      res.status,
      undefined,
      undefined,
      Object.fromEntries([...res.headers.entries()]),
      url,
      err instanceof Error
        ? err.message
        : typeof err === `string`
          ? err
          : `failed to read body`
    )
    const waiter = waitersBehind.get(res)
    if (waiter) waitersBehind.set(error, waiter)
    throw error
  }
}

/**
 * Returns a copy of `res` whose body calls `onChunk` for each chunk read from
 * the network. The SSE parser drops keep-alive comments before they reach any
 * message callback, so this is the only place that sees every byte the server
 * sends. `onChunk` throwing fails the copy.
 *
 * It reads the original body only as the consumer asks for chunks, and a
 * cancel reaches the original body before it resolves. `pipeThrough` would
 * pass the cancel back on a later tick on some Node versions, and the request
 * watchdog needs a stalled body cancelled at once.
 *
 * When `signal`, the signal of the request, aborts, it cancels the original
 * body and fails the copy with the reason of the abort, like a fetch does. A
 * body that ignores the abort of its request would otherwise stay open, with
 * a read pending, for as long as the connection lives.
 */
export function observeResponseBodyChunks(
  res: Response,
  onChunk: (chunk: Uint8Array) => void,
  signal?: AbortSignal
): Response {
  if (!res.body) return res

  const reader = res.body.getReader()
  let stopWatchingSignal = () => {}
  const body = new ReadableStream<Uint8Array>(
    {
      start(controller) {
        if (!signal) return
        const onAbort = () => {
          reader.cancel(signal.reason).catch(() => {})
          controller.error(signal.reason)
        }
        if (signal.aborted) return onAbort()
        signal.addEventListener(`abort`, onAbort, { once: true })
        stopWatchingSignal = () => signal.removeEventListener(`abort`, onAbort)
      },
      async pull(controller) {
        let result: ReadableStreamReadResult<Uint8Array>
        try {
          result = await reader.read()
        } catch (error) {
          stopWatchingSignal()
          throw error
        }
        // The abort cancelled the original body, which ends the pending read,
        // and has already failed the copy.
        if (signal?.aborted) return
        if (result.done) {
          stopWatchingSignal()
          controller.close()
          return
        }
        onChunk(result.value)
        controller.enqueue(result.value)
      },
      cancel(reason) {
        stopWatchingSignal()
        return reader.cancel(reason)
      },
    },
    { highWaterMark: 0 }
  )
  return new Response(body, res)
}

// Ensure body can actually be read in its entirety
export function createFetchWithConsumedMessages(
  fetchClient: typeof fetch,
  maxResponseBytes: number = DEFAULT_MAX_RESPONSE_BYTES
) {
  return async (...args: Parameters<typeof fetch>): Promise<Response> => {
    const url = args[0]
    const res = await fetchClient(...args)
    return consumeResponseBody(
      res,
      url.toString(),
      args[1]?.signal ?? undefined,
      maxResponseBytes
    )
  }
}

interface ChunkPrefetchOptions {
  maxChunksToPrefetch: number
}

const ChunkPrefetchDefaults = {
  maxChunksToPrefetch: 2,
}

/**
 * Creates a fetch client that prefetches subsequent log chunks for
 * consumption by the shape stream without waiting for the chunk bodies
 * themselves to be loaded.
 *
 * @param fetchClient the client to wrap
 * @param prefetchOptions options to configure prefetching
 * @returns wrapped client with prefetch capabilities
 */
export function createFetchWithChunkBuffer(
  fetchClient: typeof fetch,
  prefetchOptions: ChunkPrefetchOptions = ChunkPrefetchDefaults
): typeof fetch {
  const { maxChunksToPrefetch } = prefetchOptions

  let prefetchQueue: PrefetchQueue | undefined

  const prefetchClient = async (...args: Parameters<typeof fetchClient>) => {
    const url = args[0].toString()
    const method = getRequestMethod(args[0], args[1])

    // Prefetch is only valid for GET requests. The prefetch queue matches
    // requests by URL alone and ignores HTTP method/body, so a POST request
    // with the same URL would incorrectly consume the prefetched stream
    // response instead of making its own request.
    if (method !== `GET`) {
      prefetchQueue?.abort()
      prefetchQueue = undefined
      return fetchClient(...args)
    }

    // try to consume from the prefetch queue first, and if request is
    // not present abort the prefetch queue as it must no longer be valid
    const prefetchedRequest = prefetchQueue?.consume(...args)
    if (prefetchedRequest) {
      return prefetchedRequest
    }

    // Clear the prefetch queue after aborting to prevent returning
    // stale/aborted requests on future calls with the same URL
    prefetchQueue?.abort()
    prefetchQueue = undefined

    // perform request and fire off prefetch queue if request is eligible
    const response = await fetchClient(...args)
    const nextUrl = getNextChunkUrl(url, response)
    if (nextUrl) {
      prefetchQueue = new PrefetchQueue({
        fetchClient,
        maxPrefetchedRequests: maxChunksToPrefetch,
        url: nextUrl,
        requestInit: args[1],
      })
    }

    return response
  }

  return prefetchClient
}

export const requiredElectricResponseHeaders = [
  CHUNK_LAST_OFFSET_HEADER,
  SHAPE_HANDLE_HEADER,
]

export const requiredLiveResponseHeaders = [LIVE_CACHE_BUSTER_HEADER]

export const requiredNonLiveResponseHeaders = [SHAPE_SCHEMA_HEADER]

export function createFetchWithResponseHeadersCheck(
  fetchClient: typeof fetch
): typeof fetch {
  return async (...args: Parameters<typeof fetchClient>) => {
    const response = await fetchClient(...args)

    if (response.ok) {
      // Check that the necessary Electric headers are present on the response
      const headers = response.headers
      const missingHeaders: Array<string> = []

      const addMissingHeaders = (requiredHeaders: Array<string>) =>
        missingHeaders.push(...requiredHeaders.filter((h) => !headers.has(h)))

      const input = args[0]
      const urlString = input.toString()
      const url = new URL(urlString)

      // Snapshot responses (subset params) return a JSON object and do not include Electric chunk headers
      const isSnapshotRequest = [
        SUBSET_PARAM_WHERE,
        SUBSET_PARAM_WHERE_PARAMS,
        SUBSET_PARAM_LIMIT,
        SUBSET_PARAM_OFFSET,
        SUBSET_PARAM_ORDER_BY,
      ].some((p) => url.searchParams.has(p))
      if (isSnapshotRequest) {
        return response
      }

      addMissingHeaders(requiredElectricResponseHeaders)
      if (url.searchParams.get(LIVE_QUERY_PARAM) === `true`) {
        addMissingHeaders(requiredLiveResponseHeaders)
      }

      if (
        !url.searchParams.has(LIVE_QUERY_PARAM) ||
        url.searchParams.get(LIVE_QUERY_PARAM) === `false`
      ) {
        addMissingHeaders(requiredNonLiveResponseHeaders)
      }

      if (missingHeaders.length > 0) {
        throw new MissingHeadersError(urlString, missingHeaders)
      }
    }

    return response
  }
}

class PrefetchQueue {
  readonly #fetchClient: typeof fetch
  readonly #maxPrefetchedRequests: number
  readonly #prefetchQueue = new Map<
    string,
    [Promise<Response>, AbortController, string]
  >()
  #queueHeadUrl: string | void
  #queueTailUrl: string | void

  constructor(options: {
    url: Parameters<typeof fetch>[0]
    requestInit: Parameters<typeof fetch>[1]
    maxPrefetchedRequests: number
    fetchClient?: typeof fetch
  }) {
    this.#fetchClient =
      options.fetchClient ??
      ((...args: Parameters<typeof fetch>) => fetch(...args))
    this.#maxPrefetchedRequests = options.maxPrefetchedRequests
    this.#queueHeadUrl = options.url.toString()
    this.#queueTailUrl = this.#queueHeadUrl
    this.#prefetch(options.url, options.requestInit)
  }

  abort(): void {
    this.#prefetchQueue.forEach(([_, aborter]) => aborter.abort())
    this.#prefetchQueue.clear()
  }

  consume(...args: Parameters<typeof fetch>): Promise<Response> | void {
    const url = args[0].toString()

    const entry = this.#prefetchQueue.get(url)
    // only consume if request is in queue and is the queue "head"
    // if request is in the queue but not the head, the queue is being
    // consumed out of order and should be restarted
    if (!entry || url !== this.#queueHeadUrl) return

    const [request, aborter, identity] = entry
    // Don't return aborted requests - they will reject with AbortError
    if (aborter.signal.aborted) {
      this.#prefetchQueue.delete(url)
      return
    }
    // A response fetched under other headers (a new token, say) must not
    // answer this request. Leave the entry for the caller, which aborts the
    // whole queue when nothing is consumed and fetches afresh.
    if (identity !== requestIdentity(args[0], args[1])) return
    this.#prefetchQueue.delete(url)

    // fire off new prefetch since request has been consumed
    request
      .then((response) => {
        const nextUrl = getNextChunkUrl(url, response)
        this.#queueHeadUrl = nextUrl
        if (
          this.#queueTailUrl &&
          !this.#prefetchQueue.has(this.#queueTailUrl)
        ) {
          this.#prefetch(this.#queueTailUrl, args[1])
        }
      })
      .catch(() => {})

    return request
  }

  #prefetch(...args: Parameters<typeof fetch>): void {
    const url = args[0].toString()

    // only prefetch when queue is not full
    if (this.#prefetchQueue.size >= this.#maxPrefetchedRequests) return

    // initialize aborter per request, to avoid aborting consumed requests that
    // are still streaming their bodies to the consumer
    const aborter = new AbortController()

    try {
      const { signal, cleanup } = chainAborter(aborter, args[1]?.signal)
      const request = this.#fetchClient(url, { ...(args[1] ?? {}), signal })
      this.#prefetchQueue.set(url, [
        request,
        aborter,
        requestIdentity(args[0], args[1]),
      ])
      request
        .then((response) => {
          // only keep prefetching if response chain is uninterrupted
          if (!response.ok || aborter.signal.aborted) return

          const nextUrl = getNextChunkUrl(url, response)

          // only prefetch when there is a next URL
          if (!nextUrl || nextUrl === url) {
            this.#queueTailUrl = undefined
            return
          }

          this.#queueTailUrl = nextUrl
          return this.#prefetch(nextUrl, args[1])
        })
        .catch(() => {})
        .finally(cleanup)
    } catch (_) {
      // ignore prefetch errors
    }
  }
}

/**
 * What besides the URL decides the response of a request: the headers it
 * carries, which hold the credentials, and the credentials mode. Two requests
 * with the same identity may share a prefetched response.
 */
function requestIdentity(
  input: Parameters<typeof fetch>[0],
  init?: Parameters<typeof fetch>[1]
): string {
  const headers = new Headers(
    typeof Request !== `undefined` && input instanceof Request
      ? input.headers
      : undefined
  )
  new Headers(init?.headers).forEach((value, name) => {
    headers.set(name, value)
  })
  const credentials =
    init?.credentials ??
    (typeof Request !== `undefined` && input instanceof Request
      ? input.credentials
      : ``)
  // A Headers iterates its names lowercased and sorted.
  return JSON.stringify([credentials, [...headers.entries()]])
}

/**
 * Generate the next chunk's URL if the url and response are valid
 */
function getNextChunkUrl(url: string, res: Response): string | void {
  const shapeHandle = res.headers.get(SHAPE_HANDLE_HEADER)
  const lastOffset = res.headers.get(CHUNK_LAST_OFFSET_HEADER)
  const isUpToDate = res.headers.has(CHUNK_UP_TO_DATE_HEADER)
  const isSnapshot = res.headers.get(SNAPSHOT_HEADER) === `true`

  // only prefetch if shape handle and offset for next chunk are available, and
  // response is not already up-to-date
  // snapshot (subset) responses are one-shot and must never be prefetched:
  // there is no "next chunk" URL for them, only a repeat of the same request
  if (!shapeHandle || !lastOffset || isUpToDate || isSnapshot) return

  const nextUrl = new URL(url)

  // don't prefetch live requests, rushing them will only
  // potentially miss more recent data
  if (nextUrl.searchParams.has(LIVE_QUERY_PARAM)) return

  // don't prefetch if the response handle is the expired handle from the request
  // this can happen when a proxy serves a stale cached response despite the
  // expired_handle cache buster parameter
  const expiredHandle = nextUrl.searchParams.get(EXPIRED_HANDLE_QUERY_PARAM)
  if (expiredHandle && shapeHandle === expiredHandle) {
    console.warn(
      `[Electric] Received stale cached response with expired shape handle. ` +
        `This should not happen and indicates a proxy/CDN caching misconfiguration. ` +
        `The response contained handle "${shapeHandle}" which was previously marked as expired. ` +
        `Check that your proxy includes all query parameters (especially 'handle' and 'offset') in its cache key. ` +
        `Skipping prefetch to prevent infinite 409 loop.`
    )
    return
  }

  nextUrl.searchParams.set(SHAPE_HANDLE_QUERY_PARAM, shapeHandle)
  nextUrl.searchParams.set(OFFSET_QUERY_PARAM, lastOffset)
  nextUrl.searchParams.sort()
  return nextUrl.toString()
}

/**
 * Chains an abort controller on an optional source signal's
 * aborted state - if the source signal is aborted, the provided abort
 * controller will also abort
 */
function chainAborter(
  aborter: AbortController,
  sourceSignal?: AbortSignal | null
): {
  signal: AbortSignal
  cleanup: () => void
} {
  let cleanup = noop
  if (!sourceSignal) {
    // no-op, nothing to chain to
  } else if (sourceSignal.aborted) {
    // source signal is already aborted, abort immediately
    aborter.abort()
  } else {
    // chain to source signal abort event, and add callback to unlink
    // the aborter to avoid memory leaks
    const abortParent = () => aborter.abort()
    sourceSignal.addEventListener(`abort`, abortParent, {
      once: true,
      signal: aborter.signal,
    })
    cleanup = () => sourceSignal.removeEventListener(`abort`, abortParent)
  }

  return {
    signal: aborter.signal,
    cleanup,
  }
}

function noop() {}

function getRequestMethod(
  input: Parameters<typeof fetch>[0],
  init?: Parameters<typeof fetch>[1]
): string {
  if (init?.method) {
    return init.method.toUpperCase()
  }

  if (typeof Request !== `undefined` && input instanceof Request) {
    return input.method.toUpperCase()
  }

  return `GET`
}
