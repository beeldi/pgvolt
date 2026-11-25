import {
  Message,
  Offset,
  Schema,
  Row,
  MaybePromise,
  GetExtensions,
  ChangeMessage,
  SnapshotMetadata,
  SubsetParams,
} from './types'
import { MessageParser, Parser, TransformFunction } from './parser'
import {
  ColumnMapper,
  encodeWhereClause,
  quoteIdentifier,
} from './column-mapper'
import {
  getOffset,
  isUpToDateMessage,
  isChangeMessage,
  bigintSafeStringify,
  compareLogOffsets,
  toError,
} from './helpers'
import {
  FetchError,
  FetchBackoffAbortError,
  MissingShapeUrlError,
  InvalidSignalError,
  MissingShapeHandleError,
  InvalidShapeOptionsError,
  ReservedParamError,
  MissingHeadersError,
  StaleCacheError,
} from './error'
import {
  BackoffDefaults,
  BackoffOptions,
  InternalBackoffOptions,
  consumeResponseBody,
  createFetchWithBackoff,
  createFetchWithChunkBuffer,
  createFetchWithConsumedMessages,
  createFetchWithResponseHeadersCheck,
  observeResponseBodyChunks,
  retryOnBodyReadFailure,
  watchRetryWaits,
} from './fetch'
import {
  CHUNK_LAST_OFFSET_HEADER,
  LIVE_CACHE_BUSTER_HEADER,
  LIVE_CACHE_BUSTER_QUERY_PARAM,
  EXPIRED_HANDLE_QUERY_PARAM,
  COLUMNS_QUERY_PARAM,
  LIVE_QUERY_PARAM,
  OFFSET_QUERY_PARAM,
  SHAPE_HANDLE_HEADER,
  SHAPE_HANDLE_QUERY_PARAM,
  SHAPE_SCHEMA_HEADER,
  WHERE_QUERY_PARAM,
  WHERE_PARAMS_PARAM,
  TABLE_QUERY_PARAM,
  REPLICA_PARAM,
  FORCE_DISCONNECT_AND_REFRESH,
  PAUSE_STREAM,
  SYSTEM_WAKE,
  LIVE_REQUEST_TIMEOUT,
  STREAM_STOPPED,
  EXPERIMENTAL_LIVE_SSE_QUERY_PARAM,
  LIVE_SSE_QUERY_PARAM,
  ELECTRIC_PROTOCOL_QUERY_PARAMS,
  LOG_MODE_QUERY_PARAM,
  SUBSET_PARAM_WHERE,
  SUBSET_PARAM_WHERE_PARAMS,
  SUBSET_PARAM_LIMIT,
  SUBSET_PARAM_OFFSET,
  SUBSET_PARAM_ORDER_BY,
  SUBSET_PARAM_WHERE_EXPR,
  SUBSET_PARAM_ORDER_BY_EXPR,
  CACHE_BUSTER_QUERY_PARAM,
} from './constants'
import { compileExpression, compileOrderBy } from './expression-compiler'
import {
  EventSourceMessage,
  fetchEventSource,
} from '@microsoft/fetch-event-source'
import { expiredShapesCache } from './expired-shapes-cache'
import { upToDateTracker } from './up-to-date-tracker'
import { DEFAULT_MAX_RESPONSE_BYTES } from './response-body'
import { SnapshotTracker } from './snapshot-tracker'
import {
  createInitialState,
  ErrorState,
  PausedState,
  ShapeStreamState,
} from './shape-stream-state'
import { PauseLock } from './pause-lock'
import { getDefaultRuntimeVisibilityAdapterFactory } from './runtime-visibility'

const RESERVED_PARAMS: Set<ReservedParamKeys> = new Set([
  LIVE_CACHE_BUSTER_QUERY_PARAM,
  SHAPE_HANDLE_QUERY_PARAM,
  LIVE_QUERY_PARAM,
  OFFSET_QUERY_PARAM,
  CACHE_BUSTER_QUERY_PARAM,
])

const TROUBLESHOOTING_URL = `https://electric-sql.com/docs/guides/troubleshooting`

function createCacheBuster(): string {
  return `${Date.now()}-${Math.random().toString(36).substring(2, 9)}`
}

type Replica = `full` | `default`
export type LogMode = `changes_only` | `full`

/**
 * PostgreSQL-specific shape parameters that can be provided externally
 */
export interface PostgresParams<T extends Row<unknown> = Row> {
  /** The root table for the shape. Not required if you set the table in your proxy. */
  table?: string

  /**
   * The columns to include in the shape.
   * Must include primary keys, and can only include valid columns.
   * Defaults to all columns of the type `T`. If provided, must include primary keys, and can only include valid columns.

   */
  columns?: (keyof T)[]

  /** The where clauses for the shape */
  where?: string

  /**
   * Positional where clause paramater values. These will be passed to the server
   * and will substitute `$i` parameters in the where clause.
   *
   * It can be an array (note that positional arguments start at 1, the array will be mapped
   * accordingly), or an object with keys matching the used positional parameters in the where clause.
   *
   * If where clause is `id = $1 or id = $2`, params must have keys `"1"` and `"2"`, or be an array with length 2.
   */
  params?: Record<`${number}`, string> | string[]

  /**
   * If `replica` is `default` (the default) then Electric will only send the
   * changed columns in an update.
   *
   * If it's `full` Electric will send the entire row with both changed and
   * unchanged values. `old_value` will also be present on update messages,
   * containing the previous value for changed columns.
   *
   * Setting `replica` to `full` will result in higher bandwidth
   * usage and so is not generally recommended.
   */
  replica?: Replica
}
type SerializableParamValue = string | string[] | Record<string, string>
type ParamValue =
  | SerializableParamValue
  | (() => SerializableParamValue | Promise<SerializableParamValue>)

/**
 * External params type - what users provide.
 * Excludes reserved parameters to prevent dynamic variations that could cause stream shape changes.
 */
export type ExternalParamsRecord<T extends Row<unknown> = Row> = {
  [K in string]: ParamValue | undefined
} & Partial<PostgresParams<T>> & { [K in ReservedParamKeys]?: never }

type ReservedParamKeys =
  | typeof LIVE_CACHE_BUSTER_QUERY_PARAM
  | typeof SHAPE_HANDLE_QUERY_PARAM
  | typeof LIVE_QUERY_PARAM
  | typeof OFFSET_QUERY_PARAM
  | typeof CACHE_BUSTER_QUERY_PARAM
  | `subset__${string}`

/**
 * External headers type - what users provide.
 * Allows string or function values for any header.
 */
export type ExternalHeadersRecord = {
  [key: string]: string | (() => string | Promise<string>)
}

/**
 * Internal params type - used within the library.
 * All values are converted to strings.
 */
type InternalParamsRecord = {
  [K in string as K extends ReservedParamKeys ? never : K]:
    | string
    | Record<string, string>
}

/**
 * Helper function to resolve a function or value to its final value
 */
export async function resolveValue<T>(
  value: T | (() => T | Promise<T>)
): Promise<T> {
  if (typeof value === `function`) {
    return (value as () => T | Promise<T>)()
  }
  return value
}

/**
 * Helper function to convert external params to internal format
 */
async function toInternalParams(
  params: ExternalParamsRecord<Row>
): Promise<InternalParamsRecord> {
  const entries = Object.entries(params)
  const resolvedEntries = await Promise.all(
    entries.map(async ([key, value]) => {
      if (value === undefined) return [key, undefined]
      const resolvedValue = await resolveValue(value)
      return [
        key,
        Array.isArray(resolvedValue) ? resolvedValue.join(`,`) : resolvedValue,
      ]
    })
  )

  return Object.fromEntries(
    resolvedEntries.filter(([_, value]) => value !== undefined)
  )
}

/**
 * Helper function to resolve headers
 */
async function resolveHeaders(
  headers?: ExternalHeadersRecord
): Promise<Record<string, string>> {
  if (!headers) return {}

  const entries = Object.entries(headers)
  const resolvedEntries = await Promise.all(
    entries.map(async ([key, value]) => [key, await resolveValue(value)])
  )

  return Object.fromEntries(resolvedEntries)
}

type RetryOpts = {
  params?: ExternalParamsRecord
  headers?: ExternalHeadersRecord
}

type ShapeStreamErrorHandler = (
  error: Error
) => void | RetryOpts | Promise<void | RetryOpts>

/**
 * Options for constructing a ShapeStream.
 */
export type RuntimeVisibilityState = `visible` | `hidden`

export type RuntimeVisibilityAdapter = {
  getCurrentState?: () => RuntimeVisibilityState | undefined
  subscribe: (callback: (state: RuntimeVisibilityState) => void) => () => void
}

type ReactNativeAppStateStatus = `active` | `background` | `inactive` | null

type ReactNativeAppStateLike = {
  currentState: ReactNativeAppStateStatus
  addEventListener: (
    type: `change`,
    listener: (state: ReactNativeAppStateStatus) => void
  ) => { remove: () => void }
}

const reactNativeAppStateToVisibility = (
  state: ReactNativeAppStateStatus
): RuntimeVisibilityState | undefined => {
  if (state === null) return undefined
  return state === `active` ? `visible` : `hidden`
}

export function createReactNativeRuntimeVisibilityAdapter(
  AppState: ReactNativeAppStateLike
): RuntimeVisibilityAdapter {
  return {
    getCurrentState: () =>
      reactNativeAppStateToVisibility(AppState.currentState),
    subscribe: (callback) => {
      const subscription = AppState.addEventListener(`change`, (state) => {
        const visibilityState = reactNativeAppStateToVisibility(state)
        if (visibilityState) callback(visibilityState)
      })
      return () => subscription.remove()
    },
  }
}

function getDefaultRuntimeVisibilityAdapter():
  | RuntimeVisibilityAdapter
  | undefined {
  return getDefaultRuntimeVisibilityAdapterFactory()?.()
}

export interface ShapeStreamOptions<T = never> {
  /**
   * The full URL to where the Shape is served. This can either be the Electric server
   * directly or a proxy. E.g. for a local Electric instance, you might set `http://localhost:3000/v1/shape`
   */
  url: string

  /**
   * The "offset" on the shape log. This is typically not set as the ShapeStream
   * will handle this automatically. A common scenario where you might pass an offset
   * is if you're maintaining a local cache of the log. If you've gone offline
   * and are re-starting a ShapeStream to catch-up to the latest state of the Shape,
   * you'd pass in the last offset and shapeHandle you'd seen from the Electric server
   * so it knows at what point in the shape to catch you up from.
   */
  offset?: Offset

  /**
   * Similar to `offset`, this isn't typically used unless you're maintaining
   * a cache of the shape log.
   */
  handle?: string

  /**
   * HTTP headers to attach to requests made by the client.
   * Values can be strings or functions (sync or async) that return strings.
   * Function values are resolved in parallel when needed, making this useful
   * for authentication tokens or other dynamic headers.
   */
  headers?: ExternalHeadersRecord

  /**
   * Additional request parameters to attach to the URL.
   * Values can be strings, string arrays, or functions (sync or async) that return these types.
   * Function values are resolved in parallel when needed, making this useful
   * for user-specific parameters or dynamic filters.
   *
   * These will be merged with Electric's standard parameters.
   * Note: You cannot use Electric's reserved parameter names
   * (offset, handle, live, cursor).
   *
   * PostgreSQL-specific options like table, where, columns, and replica
   * should be specified here.
   */
  params?: ExternalParamsRecord

  /**
   * Automatically fetch updates to the Shape. If you just want to sync the current
   * shape and stop, pass false.
   */
  subscribe?: boolean

  /**
   * @deprecated No longer experimental, use {@link liveSse} instead.
   */
  experimentalLiveSse?: boolean

  /**
   * Use Server-Sent Events (SSE) for live updates.
   */
  liveSse?: boolean

  /**
   * Initial data loading mode
   */
  log?: LogMode

  signal?: AbortSignal
  fetchClient?: typeof fetch

  /**
   * Runtime lifecycle adapter for environments without `document.visibilitychange`,
   * such as React Native. Hidden state pauses the stream and aborts in-flight
   * requests; visible state resumes with a non-live catch-up request.
   */
  runtimeVisibility?: RuntimeVisibilityAdapter
  backoffOptions?: BackoffOptions

  /**
   * Largest response body the client holds in memory, in bytes. A longer
   * response ends the stream with `ResponseTooLargeError`, which reaches
   * `onError` and is not retried on its own.
   *
   * On a live SSE request it limits what is held back for the next up-to-date
   * or for slow subscribers: past it the connection closes, and the stream
   * catches up with a long poll or reconnects once subscribers have caught up.
   * Bytes are counted per network read, so what is held can pass the limit by
   * at most one read.
   *
   * Defaults to 256 MiB, well above the 10 MB chunks Electric sends by default.
   */
  maxResponseBytes?: number

  /**
   * Maximum time in milliseconds to wait for a live long-poll request or
   * refresh catch-up request before aborting it and reconnecting. This guards
   * against runtimes (notably some React Native fetch implementations) where an
   * in-flight fetch can hang indefinitely across app lifecycle or network
   * transitions.
   *
   * Live SSE requests are watched too, but the timer re-arms whenever the
   * response's headers or any body bytes arrive, so it only fires on an SSE
   * connection that has gone silent (for example, half-open after a network
   * change).
   *
   * The timer stands still while a request waits to retry a failed attempt, so
   * a long `Retry-After` is honoured, and the next attempt gets a full window.
   *
   * The default (45s) is intentionally longer than Electric's server-side
   * long-poll timeout (20s) and SSE keep-alive interval (21s), so firing the
   * watchdog indicates a request that failed to settle rather than a normal
   * long-poll waiting for data or an idle SSE stream. Keep any custom value
   * above both.
   *
   * Must be a positive finite number. Set to `false` to disable the watchdog.
   */
  liveRequestTimeoutMs?: number | false
  parser?: Parser<T>

  /**
   * Function to transform rows after parsing (e.g., for encryption, type coercion).
   * Applied to data received from Electric.
   *
   * **Note**: If you're using `transformer` solely for column name transformation
   * (e.g., snake_case → camelCase), consider using `columnMapper` instead, which
   * provides bidirectional transformation and automatically encodes WHERE clauses.
   *
   * **Execution order** when both are provided:
   * 1. `columnMapper.decode` runs first (renames columns)
   * 2. `transformer` runs second (transforms values)
   *
   * @example
   * ```typescript
   * // For column renaming only - use columnMapper
   * import { snakeCamelMapper } from '@electric-sql/client'
   * const stream = new ShapeStream({ columnMapper: snakeCamelMapper() })
   * ```
   *
   * @example
   * ```typescript
   * // For value transformation (encryption, etc.) - use transformer
   * const stream = new ShapeStream({
   *   transformer: (row) => ({
   *     ...row,
   *     encrypted_field: decrypt(row.encrypted_field)
   *   })
   * })
   * ```
   *
   * @example
   * ```typescript
   * // Use both together
   * const stream = new ShapeStream({
   *   columnMapper: snakeCamelMapper(), // Runs first: renames columns
   *   transformer: (row) => ({         // Runs second: transforms values
   *     ...row,
   *     encryptedData: decrypt(row.encryptedData)
   *   })
   * })
   * ```
   */
  transformer?: TransformFunction<T>

  /**
   * Bidirectional column name mapper for transforming between database column names
   * (e.g., snake_case) and application column names (e.g., camelCase).
   *
   * The mapper handles both:
   * - **Decoding**: Database → Application (applied to query results)
   * - **Encoding**: Application → Database (applied to WHERE clauses)
   *
   * @example
   * ```typescript
   * // Most common case: snake_case ↔ camelCase
   * import { snakeCamelMapper } from '@electric-sql/client'
   *
   * const stream = new ShapeStream({
   *   url: 'http://localhost:3000/v1/shape',
   *   params: { table: 'todos' },
   *   columnMapper: snakeCamelMapper()
   * })
   * ```
   *
   * @example
   * ```typescript
   * // Custom mapping
   * import { createColumnMapper } from '@electric-sql/client'
   *
   * const stream = new ShapeStream({
   *   columnMapper: createColumnMapper({
   *     user_id: 'userId',
   *     project_id: 'projectId',
   *     created_at: 'createdAt'
   *   })
   * })
   * ```
   */
  columnMapper?: ColumnMapper

  /**
   * A function for handling shapestream errors.
   *
   * **Automatic retries**: The client automatically retries 5xx server errors, network
   * errors, and 429 rate limits with exponential backoff. The `onError` callback is
   * only invoked after these automatic retries are exhausted, or for non-retryable
   * errors like 4xx client errors.
   *
   * When not provided, non-retryable errors will be thrown and syncing will stop.
   *
   * **Return value behavior**:
   * - Return an **object** (RetryOpts or empty `{}`) to retry syncing:
   *   - `{}` - Retry with the same params and headers
   *   - `{ params }` - Retry with modified params
   *   - `{ headers }` - Retry with modified headers (e.g., refreshed auth token)
   *   - `{ params, headers }` - Retry with both modified
   * - Return **void** or **undefined** to stop the stream permanently
   *
   * **Important**: If you want syncing to continue after an error (e.g., to retry
   * on network failures), you MUST return at least an empty object `{}`. Simply
   * logging the error and returning nothing will stop syncing.
   *
   * Retries requested by `onError` use exponential backoff with full jitter,
   * using the stream's `backoffOptions` timings, and are still bounded by a
   * consecutive retry guard to prevent permanent retry loops.
   *
   * Supports async functions that return `Promise<void | RetryOpts>`.
   *
   * @example
   * ```typescript
   * // Retry on network errors, stop on others
   * onError: (error) => {
   *   console.error('Stream error:', error)
   *   if (error instanceof FetchError && error.status >= 500) {
   *     return {} // Retry with same params
   *   }
   *   // Return void to stop on other errors
   * }
   * ```
   *
   * @example
   * ```typescript
   * // Refresh auth token on 401
   * onError: async (error) => {
   *   if (error instanceof FetchError && error.status === 401) {
   *     const newToken = await refreshAuthToken()
   *     return { headers: { Authorization: `Bearer ${newToken}` } }
   *   }
   *   return {} // Retry other errors
   * }
   * ```
   */
  onError?: ShapeStreamErrorHandler

  /**
   * HTTP method to use for subset snapshot requests (`requestSnapshot`/`fetchSnapshot`).
   *
   * - `'GET'` (default): Sends subset params as URL query parameters. May fail with
   *   HTTP 414 errors for large queries with many parameters.
   * - `'POST'`: Sends subset params in request body as JSON. Recommended for queries
   *   with large parameter lists (e.g., `WHERE id = ANY($1)` with hundreds of IDs).
   *
   * This can be overridden per-request by passing `method` in the subset params.
   *
   * @example
   * ```typescript
   * const stream = new ShapeStream({
   *   url: 'http://localhost:3000/v1/shape',
   *   params: { table: 'items' },
   *   subsetMethod: 'POST', // Use POST for all subset requests
   * })
   * ```
   */
  subsetMethod?: `GET` | `POST`

  /**
   * Warn in browser console when using HTTP URLs (default: true).
   * HTTP limits browsers to 6 concurrent connections which can cause
   * slow shapes with multiple subscriptions.
   */
  warnOnHttp?: boolean
}

export interface ShapeStreamInterface<T extends Row<unknown> = Row> {
  subscribe(
    callback: (
      messages: Message<T>[]
    ) => MaybePromise<void> | { columns?: (keyof T)[] },
    onError?: (error: FetchError | Error) => void
  ): () => void
  unsubscribeAll(): void

  isLoading(): boolean
  lastSyncedAt(): number | undefined
  lastSynced(): number
  isConnected(): boolean
  hasStarted(): boolean

  isUpToDate: boolean
  lastOffset: Offset
  shapeHandle?: string
  error?: unknown
  mode: LogMode

  forceDisconnectAndRefresh(): Promise<void>

  requestSnapshot(params: SubsetParams): Promise<{
    metadata: SnapshotMetadata
    data: Array<Message<T>>
  }>

  fetchSnapshot(opts: SubsetParams): Promise<{
    metadata: SnapshotMetadata
    data: Array<ChangeMessage<T>>
  }>
}

/**
 * Creates a canonical shape key from a URL excluding only Electric protocol parameters
 */
export function canonicalShapeKey(url: URL): string {
  const cleanUrl = new URL(url.origin + url.pathname)

  // Copy all params except Electric protocol ones that vary between requests.
  // Use append() so duplicate keys (e.g. ?table=a&table=b) are preserved.
  for (const [key, value] of url.searchParams) {
    if (!ELECTRIC_PROTOCOL_QUERY_PARAMS.includes(key)) {
      cleanUrl.searchParams.append(key, value)
    }
  }

  cleanUrl.searchParams.sort()
  return cleanUrl.toString()
}

/**
 * Reads updates to a shape from Electric using HTTP requests and long polling or
 * Server-Sent Events (SSE).
 * Notifies subscribers when new messages come in. Doesn't maintain any history of the
 * log but does keep track of the offset position and is the best way
 * to consume the HTTP `GET /v1/shape` api.
 *
 * @constructor
 * @param {ShapeStreamOptions} options - configure the shape stream
 * @example
 * Register a callback function to subscribe to the messages.
 * ```
 * const stream = new ShapeStream(options)
 * stream.subscribe(messages => {
 *   // messages is 1 or more row updates
 * })
 * ```
 *
 * To use Server-Sent Events (SSE) for real-time updates:
 * ```
 * const stream = new ShapeStream({
 *   url: `http://localhost:3000/v1/shape`,
 *   liveSse: true
 * })
 * ```
 *
 * To abort the stream, abort the `signal`
 * passed in via the `ShapeStreamOptions`.
 * ```
 * const aborter = new AbortController()
 * const issueStream = new ShapeStream({
 *   url: `${BASE_URL}/${table}`
 *   subscribe: true,
 *   signal: aborter.signal,
 * })
 * // Later...
 * aborter.abort()
 * ```
 */

export class ShapeStream<T extends Row<unknown> = Row>
  implements ShapeStreamInterface<T>
{
  static readonly Replica = {
    FULL: `full` as Replica,
    DEFAULT: `default` as Replica,
  }

  readonly options: ShapeStreamOptions<GetExtensions<T>>
  #error: unknown = null

  readonly #fetchClient: typeof fetch
  readonly #sseFetchClient: typeof fetch
  readonly #backoffOptions: InternalBackoffOptions
  // The latest time (ms since the epoch) the server has asked a request to wait
  // until with Retry-After, plus the spread that request drew on top of it, so
  // an early restart at this time lands where this stream's own retry would
  // rather than where every stream with the same Retry-After would. A deadline
  // in the past does nothing, so it is never cleared. It only grows, and any
  // request of this stream may raise it, so a failing snapshot request cannot
  // shorten the wait of the stream's own.
  #retryNotBefore = 0
  // Counts the requests the stream has sent to the server, retries included.
  #fetchAttempts = 0
  readonly #messageParser: MessageParser<T>

  readonly #subscribers = new Map<
    object,
    [
      (messages: Message<T>[]) => MaybePromise<void>,
      ((error: Error) => void) | undefined,
    ]
  >()

  #started = false
  #stopped = false
  #syncState: ShapeStreamState
  #connected: boolean = false
  #mode: LogMode
  #onError?: ShapeStreamErrorHandler
  #requestAbortController?: AbortController
  #restartAbortControllers = new WeakSet<AbortController>()
  #refreshCount = 0
  #refreshCatchUpWatchdogActive = false
  #snapshotCounter = 0

  get #isRefreshing(): boolean {
    return this.#refreshCount > 0
  }
  #tickPromise?: Promise<void>
  #tickPromiseResolver?: () => void
  #tickPromiseRejecter?: (reason?: unknown) => void
  #messageChain = Promise.resolve<void[]>([]) // promise chain for incoming messages
  // Tracks when subscriber callbacks are actively being delivered from
  // #messageChain. requestSnapshot can inject a nested batch from inside a
  // subscriber; in that reentrant case #publish uses this as an intentional
  // escape hatch to deliver the nested snapshot batch immediately rather than
  // queueing it behind the subscriber that is awaiting it.
  #isPublishing = false
  #snapshotTracker = new SnapshotTracker()
  // Highest snapshot database_lsn applied per row key. Overlapping subset
  // snapshots complete in an arbitrary order; without this an older snapshot
  // finishing last would overwrite a newer row, and the newer snapshot's dedup
  // would suppress the live replay that could repair it. Keyed application by
  // database_lsn makes the outcome order-independent.
  #snapshotAppliedLsnByKey = new Map<string, bigint>()
  #pauseLock: PauseLock
  #currentFetchUrl?: URL // Current fetch URL for computing shape key
  #lastSseConnectionStartTime?: number
  #minSseConnectionDuration = 1000 // Minimum expected SSE connection duration (1 second)
  #maxShortSseConnections = 3 // Fall back to long polling after this many short connections
  #sseBackoffBaseDelay = 100 // Base delay for exponential backoff (ms)
  #sseBackoffMaxDelay = 5000 // Maximum delay cap (ms)
  #liveRequestTimeoutMs: number | false
  #unsubscribeFromVisibilityChanges?: () => void
  #unsubscribeFromWakeDetection?: () => void
  #unsubscribeFromOnlineEvent?: () => void
  // Counts #start calls so a teardown can tell whether a newer #start has taken over.
  #startGeneration = 0
  #maxStaleCacheRetries = 3
  // Fast-loop detection: track recent non-live responses to detect tight retry
  // loops caused by proxy/CDN misconfiguration or stale client-side caches
  #recentRequestEntries: Array<{ timestamp: number; offset: string }> = []
  #fastLoopWindowMs = 500
  #fastLoopThreshold = 5
  #fastLoopBackoffBaseMs = 100
  #fastLoopBackoffMaxMs = 5_000
  #fastLoopConsecutiveCount = 0
  #fastLoopMaxCount = 5
  #pendingRequestShapeCacheBuster?: string
  #maxSnapshotRetries = 5
  readonly #maxResponseBytes: number
  #undeliveredSseBytes = 0
  #expiredShapeRecoveryKey: string | null = null
  #pendingSelfHealCheck: { shapeKey: string; staleHandle: string } | null = null
  #consecutiveErrorRetries = 0
  #maxConsecutiveErrorRetries = 50
  #onErrorBackoff: {
    initialDelay: number
    maxDelay: number
    multiplier: number
  }

  constructor(options: ShapeStreamOptions<GetExtensions<T>>) {
    this.options = { subscribe: true, ...options }
    validateOptions(this.options)
    this.#syncState = createInitialState({
      offset: this.options.offset ?? `-1`,
      handle: this.options.handle,
    })

    this.#pauseLock = new PauseLock({
      onAcquired: () => {
        this.#syncState = this.#syncState.pause()
        if (this.#started) {
          this.#requestAbortController?.abort(PAUSE_STREAM)
        }
      },
      onReleased: () => {
        if (!this.#started) return
        if (this.options.signal?.aborted) return
        // Don't transition syncState here — let #requestShape handle
        // the PausedState→previous transition so it can detect
        // resumingFromPause and avoid live long-polling.
        this.#start().catch(() => {
          // Errors from #start are handled internally via onError.
          // This catch prevents unhandled promise rejection in Node/Bun.
        })
      },
    })

    // Build transformer chain: columnMapper.decode -> transformer
    // columnMapper transforms column names, transformer transforms values
    let transformer: TransformFunction<GetExtensions<T>> | undefined

    if (options.columnMapper) {
      const applyColumnMapper = (
        row: Row<GetExtensions<T>>
      ): Row<GetExtensions<T>> => {
        // `Object.fromEntries` defines data properties, so every column stays
        // an ordinary field of the row whatever its name.
        return Object.fromEntries(
          Object.entries(row).map(([dbKey, value]) => [
            options.columnMapper!.decode(dbKey),
            value,
          ])
        ) as Row<GetExtensions<T>>
      }

      transformer = options.transformer
        ? (row: Row<GetExtensions<T>>) =>
            options.transformer!(applyColumnMapper(row))
        : applyColumnMapper
    } else {
      transformer = options.transformer
    }

    this.#messageParser = new MessageParser<T>(options.parser, transformer)

    this.#onError = this.options.onError
    this.#mode = this.options.log ?? `full`
    // Default exceeds Electric's 20s server long-poll timeout, so this only
    // fires when the runtime request appears wedged rather than normally held.
    this.#liveRequestTimeoutMs = this.options.liveRequestTimeoutMs ?? 45_000
    this.#maxResponseBytes =
      this.options.maxResponseBytes ?? DEFAULT_MAX_RESPONSE_BYTES

    const runtimeFetchClient =
      options.fetchClient ??
      ((...args: Parameters<typeof fetch>) => fetch(...args))
    const baseFetchClient: typeof fetch = (...args) => {
      this.#fetchAttempts++
      return runtimeFetchClient(...args)
    }

    const backOffOpts: InternalBackoffOptions = {
      ...(options.backoffOptions ?? BackoffDefaults),
      onFailedAttempt: () => {
        this.#connected = false
        options.backoffOptions?.onFailedAttempt?.()
      },
      onRetryWait: ({ notBeforeMs }) => {
        this.#retryNotBefore = Math.max(
          this.#retryNotBefore,
          Date.now() + notBeforeMs
        )
      },
    }
    this.#backoffOptions = backOffOpts
    this.#onErrorBackoff = {
      initialDelay: backOffOpts.initialDelay,
      maxDelay: backOffOpts.maxDelay,
      multiplier: backOffOpts.multiplier,
    }

    const fetchWithBackoffClient = createFetchWithBackoff(
      baseFetchClient,
      backOffOpts
    )

    this.#sseFetchClient = createFetchWithResponseHeadersCheck(
      createFetchWithChunkBuffer(fetchWithBackoffClient)
    )

    this.#fetchClient = createFetchWithConsumedMessages(
      this.#sseFetchClient,
      this.#maxResponseBytes
    )

    this.#subscribeToVisibilityChanges()
  }

  get shapeHandle() {
    return this.#syncState.handle
  }

  get error() {
    return this.#error
  }

  get isUpToDate() {
    return this.#syncState.isUpToDate
  }

  get lastOffset() {
    return this.#syncState.offset
  }

  get mode() {
    return this.#mode
  }

  async #start(): Promise<void> {
    const generation = ++this.#startGeneration
    this.#started = true
    this.#subscribeToWakeDetection()
    this.#subscribeToOnlineEvent()

    try {
      await this.#requestShape()
    } catch (err) {
      this.#error = err
      if (err instanceof Error) {
        this.#syncState = this.#syncState.toErrorState(err)
      }
      // A custom fetchClient can reject with a non-Error; subscribers must
      // still be told the stream stopped.
      const error = toError(err)

      // Check if onError handler wants to retry
      if (this.#onError) {
        const retryOpts = await this.#onError(err as Error)
        // Guard against null (typeof null === "object" in JavaScript)
        const isRetryable = !(err instanceof MissingHeadersError)
        if (retryOpts && typeof retryOpts === `object` && isRetryable) {
          // Update params/headers but don't reset offset
          // We want to continue from where we left off, not refetch everything
          if (retryOpts.params) {
            // Merge new params with existing params to preserve other parameters
            this.options.params = {
              ...(this.options.params ?? {}),
              ...retryOpts.params,
            }
          }

          if (retryOpts.headers) {
            // Merge new headers with existing headers to preserve other headers
            this.options.headers = {
              ...(this.options.headers ?? {}),
              ...retryOpts.headers,
            }
          }

          // Bound the onError retry loop to prevent unbounded retries
          this.#consecutiveErrorRetries++
          if (
            this.#consecutiveErrorRetries > this.#maxConsecutiveErrorRetries
          ) {
            console.warn(
              `[Electric]`,
              `onError retry loop exhausted after ${this.#maxConsecutiveErrorRetries} consecutive retries. ` +
                `The error was never resolved by the onError handler. ` +
                `Error: ${err instanceof Error ? err.message : String(err)}`,
              new Error(`stack trace`)
            )
            this.#sendErrorToSubscribers(error)
            this.#teardownForGood(generation)
            return
          }

          // Clear the error since we're retrying
          this.#error = null
          if (this.#syncState instanceof ErrorState) {
            this.#syncState = this.#syncState.retry()
          }
          this.#fastLoopConsecutiveCount = 0
          this.#recentRequestEntries = []

          await this.#backoffOnErrorRetry(this.#consecutiveErrorRetries)
          if (this.options.signal?.aborted) {
            this.#teardownForGood(generation)
            return
          }

          // Restart from current offset
          this.#started = false
          return this.#start()
        }
        // onError returned void, meaning it doesn't want to retry
        // This is an unrecoverable error, notify subscribers
        this.#sendErrorToSubscribers(error)
        this.#teardownForGood(generation)
        return
      }

      // No onError handler provided, this is an unrecoverable error
      // Notify subscribers and throw
      this.#sendErrorToSubscribers(error)
      this.#teardownForGood(generation)
      throw err
    }

    // The loop also ends for a pause, which the visibility listener resumes.
    if (
      this.#stopped ||
      this.options.signal?.aborted ||
      (!this.options.subscribe && this.#syncState.isUpToDate)
    ) {
      this.#teardownForGood(generation)
    } else {
      this.#teardown(generation)
    }
  }

  // `generation` is the #start that is tearing down. A pause aborts that start's request loop, and
  // the resume can begin a new #start before the aborted loop has settled. The new start finds the
  // wake and online subscriptions still in place and reuses them, so when the aborted loop then
  // reaches its teardown it must leave them alone: dropping them would leave the resumed stream
  // with no wake detection and no online listener. Only the newest #start drops them. Callers on
  // the terminal paths (#stop and the stop check in #requestShape) pass no generation and always
  // drop them. The visibility listener is NOT dropped here: #teardown also runs on a temporary
  // visibility pause, and the listener is registered once in the constructor — not per-start — so
  // removing it would leave nothing to resume the stream when the tab reappears. It is dropped
  // only on the terminal paths, through #teardownForGood.
  #teardown(generation?: number) {
    this.#connected = false
    this.#tickPromiseRejecter?.()
    if (generation !== undefined && generation !== this.#startGeneration) return
    this.#unsubscribeFromWakeDetection?.()
    this.#unsubscribeFromOnlineEvent?.()
  }

  // #teardown for a stream that will not run again: nothing is left to resume, so the visibility
  // listener goes too, and no longer keeps the stream and its options reachable.
  #teardownForGood(generation?: number) {
    this.#teardown(generation)
    this.#unsubscribeFromVisibilityChanges?.()
  }

  async #backoffOnErrorRetry(retryAttempt: number): Promise<void> {
    // Non-429 4xx errors intentionally bypass fetch-layer backoff so
    // `onError` can repair auth/params; network failures that exhaust fetch
    // backoff also reach this path. If the handler asks to retry we still
    // need pacing to avoid tight retry loops on persistent failures.
    // Uses the same full-jitter exponential backoff strategy as the fetch layer.
    const { initialDelay, maxDelay, multiplier } = this.#onErrorBackoff
    const cappedDelay = Math.min(
      maxDelay,
      initialDelay * Math.pow(multiplier, retryAttempt - 1) // 1-indexed: first retry uses multiplier^0
    )
    const delayMs = Math.floor(Math.random() * cappedDelay)

    const signal = this.options.signal
    if (delayMs <= 0 || signal?.aborted) return

    await new Promise<void>((resolve) => {
      let settled = false
      const done = () => {
        if (settled) return
        settled = true
        clearTimeout(timer)
        signal?.removeEventListener(`abort`, done)
        resolve()
      }
      const timer = setTimeout(done, delayMs)
      signal?.addEventListener(`abort`, done, { once: true })
      if (signal?.aborted) done()
    })
  }

  async #requestShape(requestShapeCacheBuster?: string): Promise<void> {
    // Poll iteratively: a tail-recursive self-call pins ~106 B of promise chain per cycle,
    // reclaimed only when the stream aborts.
    let pendingCacheBuster = requestShapeCacheBuster

    while (true) {
      const activeCacheBuster =
        pendingCacheBuster ?? this.#pendingRequestShapeCacheBuster
      pendingCacheBuster = undefined

      if (this.#stopped || this.options.signal?.aborted) {
        this.#teardown()
        return
      }

      // ErrorState should never reach the request loop — re-throw so
      // #start's catch block can route it through onError properly.
      if (this.#syncState instanceof ErrorState) {
        throw this.#syncState.error
      }

      if (this.#pauseLock.isPaused) {
        if (activeCacheBuster) {
          this.#pendingRequestShapeCacheBuster = activeCacheBuster
        }
        return
      }

      if (
        !this.options.subscribe &&
        (this.options.signal?.aborted || this.#syncState.isUpToDate)
      ) {
        return
      }

      // Only check for fast loops on non-live requests; live polling is expected to be rapid
      if (!this.#syncState.isUpToDate) {
        await this.#checkFastLoop()
      } else {
        this.#fastLoopConsecutiveCount = 0
        this.#recentRequestEntries = []
      }

      let resumingFromPause = false
      if (this.#syncState instanceof PausedState) {
        resumingFromPause = true
        this.#syncState = this.#syncState.resume()
      }

      const { url, signal } = this.options
      const { fetchUrl, requestHeaders } = await this.#constructUrl(
        url,
        resumingFromPause
      )

      if (activeCacheBuster) {
        fetchUrl.searchParams.set(CACHE_BUSTER_QUERY_PARAM, activeCacheBuster)
        fetchUrl.searchParams.sort()
      }
      const abortListener = await this.#createAbortListener(signal)
      const requestAbortController = this.#requestAbortController! // we know that it is not undefined because it is set by `this.#createAbortListener`

      // Re-check after async setup — the lock may have been acquired
      // during URL construction or abort controller creation (e.g., by
      // requestSnapshot), when the abort controller didn't exist yet.
      if (this.#pauseLock.isPaused) {
        if (abortListener && signal) {
          signal.removeEventListener(`abort`, abortListener)
        }
        if (activeCacheBuster) {
          this.#pendingRequestShapeCacheBuster = activeCacheBuster
        }
        this.#releaseRequestAbortController(requestAbortController)
        return
      }

      this.#pendingRequestShapeCacheBuster = undefined

      const requestOffset = this.#syncState.offset

      try {
        await this.#fetchShape({
          fetchUrl,
          requestAbortController,
          headers: requestHeaders,
          resumingFromPause,
        })
        this.#recordFastLoopResponse(requestOffset)
      } catch (e) {
        const abortReason = requestAbortController.signal.reason
        const isMarkedRestartAbort = this.#restartAbortControllers.delete(
          requestAbortController
        )
        const isRestartAbort =
          requestAbortController.signal.aborted &&
          (isMarkedRestartAbort ||
            abortReason === FORCE_DISCONNECT_AND_REFRESH ||
            abortReason === SYSTEM_WAKE ||
            abortReason === LIVE_REQUEST_TIMEOUT)

        if (
          (e instanceof FetchError || e instanceof FetchBackoffAbortError) &&
          isRestartAbort
        ) {
          continue
        }

        if (e instanceof FetchBackoffAbortError) {
          return // interrupted
        }

        if (e instanceof StaleCacheError) {
          // Two paths throw StaleCacheError:
          // 1. Normal stale-retry: response handle matched expired handle,
          //    #staleCacheBuster set to bypass CDN cache on next request.
          // 2. Self-healing: stale retries exhausted, expired entry cleared,
          //    stream reset — retry without expired_handle param.
          this.#recordFastLoopResponse(requestOffset)
          continue
        }

        if (!(e instanceof FetchError)) throw e // should never happen

        if (e.status == 409) {
          this.#recordFastLoopResponse(requestOffset)

          // Upon receiving a 409, start from scratch with the newly
          // provided shape handle (if present). An unconditional cache
          // buster ensures the retry URL is always unique regardless of
          // whether the server returns a new, same, or missing handle.

          // Store the current shape URL as expired to avoid future 409s
          if (this.#syncState.handle) {
            const shapeKey = canonicalShapeKey(fetchUrl)
            expiredShapesCache.markExpired(shapeKey, this.#syncState.handle)
          }

          const newShapeHandle = e.headers[SHAPE_HANDLE_HEADER]
          if (!newShapeHandle) {
            console.warn(
              `[Electric] Received 409 response without a shape handle header. ` +
                `This likely indicates a proxy or CDN stripping required headers.`
            )
          }
          const nextRequestShapeCacheBuster = createCacheBuster()
          this.#reset(newShapeHandle)

          // Notify subscribers that data must be re-fetched so they can
          // clear accumulated state (e.g., Shape clears its row map).
          // We publish a synthetic control message rather than the raw 409
          // body to avoid delivering stale data rows to subscribers.
          await this.#publish([{ headers: { control: `must-refetch` } }])

          pendingCacheBuster = nextRequestShapeCacheBuster
          continue
        } else {
          // errors that have reached this point are not actionable without
          // additional user input, such as 400s, or have exhausted their
          // retries (network errors, bodies cut off mid-read), so we exit the
          // loop and let #start handle it
          // Note: We don't notify subscribers here because onError might recover
          throw e
        }
      } finally {
        if (abortListener && signal) {
          signal.removeEventListener(`abort`, abortListener)
        }
        this.#releaseRequestAbortController(requestAbortController)
      }

      this.#tickPromiseResolver?.()
    }
  }

  /**
   * Records a request that got a response, for fast-loop detection. Requests
   * aborted before a response (e.g. by a snapshot pausing the stream) are not
   * recorded; they say nothing about a retry loop.
   */
  #recordFastLoopResponse(requestOffset: Offset): void {
    this.#recentRequestEntries.push({
      timestamp: Date.now(),
      offset: requestOffset,
    })
  }

  /**
   * Detects tight retry loops (e.g., from stale client-side caches or
   * proxy/CDN misconfiguration) and attempts recovery. On first detection,
   * clears client-side caches (in-memory and localStorage) and resets the
   * stream to fetch from scratch.
   * If the loop persists, applies exponential backoff and eventually throws.
   */
  async #checkFastLoop(): Promise<void> {
    const now = Date.now()
    const currentOffset = this.#syncState.offset

    this.#recentRequestEntries = this.#recentRequestEntries.filter(
      (e) => now - e.timestamp < this.#fastLoopWindowMs
    )

    // Only flag as a fast loop if requests are stuck at the same offset.
    // Normal rapid syncing advances the offset with each response.
    const sameOffsetCount = this.#recentRequestEntries.filter(
      (e) => e.offset === currentOffset
    ).length

    if (sameOffsetCount < this.#fastLoopThreshold) return

    this.#fastLoopConsecutiveCount++

    if (this.#fastLoopConsecutiveCount >= this.#fastLoopMaxCount) {
      throw new FetchError(
        502,
        undefined,
        undefined,
        {},
        this.options.url,
        `Client is stuck in a fast retry loop ` +
          `(${this.#fastLoopThreshold} requests in ${this.#fastLoopWindowMs}ms at the same offset, ` +
          `repeated ${this.#fastLoopMaxCount} times). ` +
          `Client-side caches were cleared automatically on first detection, but the loop persists. ` +
          `This usually indicates a proxy or CDN misconfiguration. ` +
          `Common causes:\n` +
          `  - Proxy is not including query parameters (handle, offset) in its cache key\n` +
          `  - CDN is serving stale 409 responses\n` +
          `  - Proxy is stripping required Electric headers from responses\n` +
          `For more information visit the troubleshooting guide: ${TROUBLESHOOTING_URL}`
      )
    }

    if (this.#fastLoopConsecutiveCount === 1) {
      console.warn(
        `[Electric]`,
        `Detected fast retry loop ` +
          `(${this.#fastLoopThreshold} requests in ${this.#fastLoopWindowMs}ms at the same offset). ` +
          `Clearing client-side caches and resetting stream to recover. ` +
          `If this persists, check that your proxy includes all query parameters ` +
          `(especially 'handle' and 'offset') in its cache key, ` +
          `and that required Electric headers are forwarded to the client. ` +
          `For more information visit the troubleshooting guide: ${TROUBLESHOOTING_URL}`,
        new Error(`stack trace`)
      )

      if (this.#currentFetchUrl) {
        const shapeKey = canonicalShapeKey(this.#currentFetchUrl)
        expiredShapesCache.delete(shapeKey)
        upToDateTracker.delete(shapeKey)
      } else {
        expiredShapesCache.clear()
        upToDateTracker.clear()
      }
      this.#reset()
      this.#recentRequestEntries = []
      return
    }

    // Exponential backoff with full jitter
    const maxDelay = Math.min(
      this.#fastLoopBackoffMaxMs,
      this.#fastLoopBackoffBaseMs * Math.pow(2, this.#fastLoopConsecutiveCount)
    )
    const delayMs = Math.floor(Math.random() * maxDelay)

    await new Promise((resolve) => setTimeout(resolve, delayMs))

    this.#recentRequestEntries = []
  }

  async #constructUrl(
    url: string,
    resumingFromPause: boolean,
    subsetParams?: SubsetParams
  ) {
    // Resolve headers and params in parallel
    const [requestHeaders, params] = await Promise.all([
      resolveHeaders(this.options.headers),
      this.options.params
        ? toInternalParams(convertWhereParamsToObj(this.options.params))
        : undefined,
    ])

    // Validate params after resolution
    if (params) validateParams(params)

    const fetchUrl = new URL(url)

    // Add PostgreSQL-specific parameters
    if (params) {
      if (params.table) setQueryParam(fetchUrl, TABLE_QUERY_PARAM, params.table)
      if (params.where && typeof params.where === `string`) {
        const encodedWhere = encodeWhereClause(
          params.where,
          this.options.columnMapper?.encode
        )
        setQueryParam(fetchUrl, WHERE_QUERY_PARAM, encodedWhere)
      }
      if (params.columns) {
        // Get original columns array from options (before toInternalParams converted to string)
        const originalColumns = await resolveValue(this.options.params?.columns)
        if (Array.isArray(originalColumns)) {
          // Apply columnMapper encoding if present
          let encodedColumns = originalColumns.map(String)
          if (this.options.columnMapper) {
            encodedColumns = encodedColumns.map(
              this.options.columnMapper.encode
            )
          }
          // Quote each column name to handle special characters (commas, etc.)
          const serializedColumns = encodedColumns
            .map(quoteIdentifier)
            .join(`,`)
          setQueryParam(fetchUrl, COLUMNS_QUERY_PARAM, serializedColumns)
        } else {
          // Fallback: columns was already a string
          setQueryParam(fetchUrl, COLUMNS_QUERY_PARAM, params.columns)
        }
      }
      if (params.replica) setQueryParam(fetchUrl, REPLICA_PARAM, params.replica)
      if (params.params)
        setQueryParam(fetchUrl, WHERE_PARAMS_PARAM, params.params)

      // Add any remaining custom parameters
      const customParams = { ...params }
      delete customParams.table
      delete customParams.where
      delete customParams.columns
      delete customParams.replica
      delete customParams.params

      for (const [key, value] of Object.entries(customParams)) {
        setQueryParam(fetchUrl, key, value)
      }
    }

    if (subsetParams) {
      // Prefer structured expressions when available (allows proper columnMapper application)
      // Fall back to legacy string format for backwards compatibility
      if (subsetParams.whereExpr) {
        // Compile structured expression with columnMapper applied
        const compiledWhere = compileExpression(
          subsetParams.whereExpr,
          this.options.columnMapper?.encode
        )
        setQueryParam(fetchUrl, SUBSET_PARAM_WHERE, compiledWhere)
        // Also send the structured expression for servers that support it
        fetchUrl.searchParams.set(
          SUBSET_PARAM_WHERE_EXPR,
          JSON.stringify(subsetParams.whereExpr)
        )
      } else if (subsetParams.where && typeof subsetParams.where === `string`) {
        // Legacy string format (no columnMapper applied to already-compiled SQL)
        const encodedWhere = encodeWhereClause(
          subsetParams.where,
          this.options.columnMapper?.encode
        )
        setQueryParam(fetchUrl, SUBSET_PARAM_WHERE, encodedWhere)
      }

      if (subsetParams.params)
        // Serialize params as JSON to keep the parameter name constant for proxy configs
        fetchUrl.searchParams.set(
          SUBSET_PARAM_WHERE_PARAMS,
          bigintSafeStringify(subsetParams.params)
        )
      if (subsetParams.limit !== undefined)
        setQueryParam(fetchUrl, SUBSET_PARAM_LIMIT, subsetParams.limit)
      if (subsetParams.offset !== undefined)
        setQueryParam(fetchUrl, SUBSET_PARAM_OFFSET, subsetParams.offset)

      // Prefer structured ORDER BY expressions when available
      if (subsetParams.orderByExpr) {
        // Compile structured ORDER BY with columnMapper applied
        const compiledOrderBy = compileOrderBy(
          subsetParams.orderByExpr,
          this.options.columnMapper?.encode
        )
        setQueryParam(fetchUrl, SUBSET_PARAM_ORDER_BY, compiledOrderBy)
        // Also send the structured expression for servers that support it
        fetchUrl.searchParams.set(
          SUBSET_PARAM_ORDER_BY_EXPR,
          JSON.stringify(subsetParams.orderByExpr)
        )
      } else if (
        subsetParams.orderBy &&
        typeof subsetParams.orderBy === `string`
      ) {
        // Legacy string format
        const encodedOrderBy = encodeWhereClause(
          subsetParams.orderBy,
          this.options.columnMapper?.encode
        )
        setQueryParam(fetchUrl, SUBSET_PARAM_ORDER_BY, encodedOrderBy)
      }
    }

    // Add state-specific parameters (offset, handle, live cache busters, etc.)
    this.#syncState.applyUrlParams(fetchUrl, {
      isSnapshotRequest: subsetParams !== undefined,
      // Don't long-poll when resuming from pause or refreshing — avoids
      // a 20s hold during which `isConnected` would be false
      canLongPoll: !this.#isRefreshing && !resumingFromPause,
    })
    fetchUrl.searchParams.set(LOG_MODE_QUERY_PARAM, this.#mode)

    // Add cache buster for shapes known to be expired to prevent 409s
    const shapeKey = canonicalShapeKey(fetchUrl)
    const expiredHandle = expiredShapesCache.getExpiredHandle(shapeKey)
    if (expiredHandle) {
      fetchUrl.searchParams.set(EXPIRED_HANDLE_QUERY_PARAM, expiredHandle)
    }

    // sort query params in-place for stable URLs and improved cache hits
    fetchUrl.searchParams.sort()

    return {
      fetchUrl,
      requestHeaders,
    }
  }

  // Clears the current request's controller, but only if it still belongs to the request that is
  // finishing. A request loop that a pause aborted can settle after the resumed loop has started
  // its own request; clearing unconditionally would drop the resumed loop's controller, and
  // `#stop`, the wake check and the online event would then find nothing to abort.
  #releaseRequestAbortController(controller: AbortController) {
    if (this.#requestAbortController === controller) {
      this.#requestAbortController = undefined
    }
  }

  async #createAbortListener(signal?: AbortSignal) {
    // Create a new AbortController for this request
    this.#requestAbortController = new AbortController()

    // If user provided a signal, listen to it and pass on the reason for the abort
    if (signal) {
      const abortListener = () => {
        this.#requestAbortController?.abort(signal.reason)
      }

      signal.addEventListener(`abort`, abortListener, { once: true })

      if (signal.aborted) {
        // If the signal is already aborted, abort the request immediately
        this.#requestAbortController?.abort(signal.reason)
      }

      return abortListener
    }
  }

  /**
   * Processes response metadata (headers, status) and updates sync state.
   * Returns `true` if the response body should be processed by the caller,
   * or `false` if the response was ignored (stale) and the body should be skipped.
   * Throws on stale-retry (to trigger a retry with cache buster).
   */
  async #onInitialResponse(
    response: Response,
    isSse = false
  ): Promise<boolean> {
    const { headers, status } = response
    const shapeHandle = headers.get(SHAPE_HANDLE_HEADER)
    const shapeKey = this.#currentFetchUrl
      ? canonicalShapeKey(this.#currentFetchUrl)
      : null
    const expiredHandle = shapeKey
      ? expiredShapesCache.getExpiredHandle(shapeKey)
      : null

    // If this response is the first one after a self-healing retry, check
    // whether the proxy/CDN returned the exact handle we just marked expired.
    // If so, the client is about to accept stale data silently — loudly warn
    // so operators can detect and fix the proxy misconfiguration.
    if (this.#pendingSelfHealCheck) {
      const { shapeKey: healedKey, staleHandle } = this.#pendingSelfHealCheck
      this.#pendingSelfHealCheck = null
      if (shapeKey === healedKey && shapeHandle === staleHandle) {
        console.warn(
          `[Electric]`,
          `Self-healing retry received the same handle "${staleHandle}" that was just marked expired. ` +
            `This means your proxy/CDN is serving a stale cached response and ignoring cache-buster query params. ` +
            `The client will proceed with this stale data to avoid a permanent failure, but it may be out of date until the cache refreshes. ` +
            `Fix: configure your proxy/CDN to include all query parameters (especially 'handle' and 'offset') in its cache key. ` +
            `For more information visit the troubleshooting guide: ${TROUBLESHOOTING_URL}`,
          new Error(`stack trace`)
        )
      }
    }

    const transition = this.#syncState.handleResponseMetadata({
      status,
      responseHandle: shapeHandle,
      responseOffset: headers.get(CHUNK_LAST_OFFSET_HEADER) as Offset | null,
      responseCursor: headers.get(LIVE_CACHE_BUSTER_HEADER),
      responseSchema: getSchemaFromHeaders(headers),
      expiredHandle,
      isSse,
      now: Date.now(),
      maxStaleCacheRetries: this.#maxStaleCacheRetries,
      createCacheBuster,
    })

    this.#syncState = transition.state

    // Clear recovery guard on 204 (no-content), since the empty body means
    // #onMessages won't run to clear it via the up-to-date path.
    if (status === 204) {
      this.#expiredShapeRecoveryKey = null
    }

    if (transition.action === `accepted` && status === 204) {
      this.#consecutiveErrorRetries = 0
    }

    if (transition.action === `stale-retry`) {
      // Cancel the response body to release the connection before retrying.
      await response.body?.cancel()
      if (transition.exceededMaxRetries) {
        if (shapeKey) {
          // Clear the expired entry — keeping it only poisons future sessions.
          expiredShapesCache.delete(shapeKey)

          // Try one self-healing retry per shape: reset the stream and
          // retry without the expired_handle param. Since handles are never
          // reused (see SPEC.md S0), the fresh response will have a new
          // handle and won't trigger stale detection.
          if (this.#expiredShapeRecoveryKey !== shapeKey) {
            console.warn(
              `[Electric]`,
              `Stale cache retries exhausted (${this.#maxStaleCacheRetries} attempts). ` +
                `Clearing expired handle entry and attempting self-healing retry without the expired_handle parameter. ` +
                `For more information visit the troubleshooting guide: ${TROUBLESHOOTING_URL}`,
              new Error(`stack trace`)
            )
            this.#expiredShapeRecoveryKey = shapeKey
            // Arm a post-self-heal check: if the next response comes back
            // with the same handle we just marked expired, the proxy/CDN is
            // still serving stale data and we'll warn loudly instead of
            // accepting it silently.
            if (shapeHandle) {
              this.#pendingSelfHealCheck = {
                shapeKey,
                staleHandle: shapeHandle,
              }
            }
            this.#reset()
            throw new StaleCacheError(
              `Expired handle entry evicted for self-healing retry`
            )
          }
        }
        throw new FetchError(
          502,
          undefined,
          undefined,
          {},
          this.#currentFetchUrl?.toString() ?? ``,
          `CDN continues serving stale cached responses after ${this.#maxStaleCacheRetries} retry attempts. ` +
            `This indicates a severe proxy/CDN misconfiguration. ` +
            `Check that your proxy includes all query parameters (especially 'handle' and 'offset') in its cache key. ` +
            `For more information visit the troubleshooting guide: ${TROUBLESHOOTING_URL}`
        )
      }
      console.warn(
        `[Electric]`,
        `Received stale cached response with expired shape handle. ` +
          `This should not happen and indicates a proxy/CDN caching misconfiguration. ` +
          `The response contained handle "${shapeHandle}" which was previously marked as expired. ` +
          `Check that your proxy includes all query parameters (especially 'handle' and 'offset') in its cache key. ` +
          `For more information visit the troubleshooting guide: ${TROUBLESHOOTING_URL} ` +
          `Retrying with a random cache buster to bypass the stale cache (attempt ${this.#syncState.staleCacheRetryCount}/${this.#maxStaleCacheRetries}).`,
        new Error(`stack trace`)
      )
      throw new StaleCacheError(
        `Received stale cached response with expired handle "${shapeHandle}". ` +
          `This indicates a proxy/CDN caching misconfiguration. ` +
          `Check that your proxy includes all query parameters (especially 'handle' and 'offset') in its cache key.`
      )
    }

    if (transition.action === `ignored`) {
      console.warn(
        `[Electric]`,
        `Response was ignored by state "${this.#syncState.kind}". ` +
          `The response body will be skipped. ` +
          `This may indicate a proxy/CDN caching issue or a client state machine bug.`,
        new Error(`stack trace`)
      )
      return false
    }

    return true
  }

  /**
   * Throws synchronously, with nothing moved, when the batch cannot be
   * processed: the caller's error path then asks for the batch again.
   */
  #onMessages(
    batch: Array<Message<T>>,
    isSseMessage = false,
    opts: { allowReentrantPublishBypass?: boolean } = {}
  ): Promise<void[]> {
    if (!Array.isArray(batch)) {
      console.warn(
        `[Electric]`,
        `#onMessages called with non-array argument (${typeof batch}). ` +
          `This is a client bug — please report it.`,
        new Error(`stack trace`)
      )
      return Promise.resolve([])
    }
    if (batch.length === 0) return Promise.resolve([])

    const lastMessage = batch[batch.length - 1]
    const hasUpToDateMessage = isUpToDateMessage(lastMessage)
    const upToDateOffset = hasUpToDateMessage
      ? getOffset(lastMessage)
      : undefined

    const transition = this.#syncState.handleMessageBatch({
      hasMessages: true,
      hasUpToDateMessage,
      isSse: isSseMessage,
      upToDateOffset,
      now: Date.now(),
      currentCursor: this.#syncState.liveCacheBuster,
    })
    // Filter messages using snapshot tracker
    const snapshotTracker = this.#snapshotTracker.clone()
    const messagesToProcess = batch.filter((message) => {
      if (isChangeMessage(message)) {
        const changeLsn = message.headers.lsn
        if (typeof changeLsn === `string` && changeLsn) {
          // A quiet SSE stream may deliver its first later change before the
          // next up-to-date boundary. Retire snapshots against that change's
          // WAL position before resolving its wrapped transaction ID.
          snapshotTracker.lastSeenUpdate(BigInt(changeLsn))
        }
        return !snapshotTracker.shouldRejectMessage(message)
      }

      if (isUpToDateMessage(message)) {
        const lastSeenLsn = message.headers.global_last_seen_lsn
        if (typeof lastSeenLsn === `string` && lastSeenLsn) {
          // Process this in message order: changes before the up-to-date
          // boundary still need snapshot deduplication, while later changes do
          // not once the database has passed the snapshot's LSN.
          snapshotTracker.lastSeenUpdate(BigInt(lastSeenLsn))
        }

        // A replayed up-to-date for the previous session's cursor still
        // carries real WAL progress, which is applied above; only its
        // delivery to subscribers is skipped.
        if (transition.suppressUpToDate) return false
      }

      return true // Always process control messages
    })

    this.#snapshotTracker = snapshotTracker
    this.#consecutiveErrorRetries = 0
    this.#syncState = transition.state

    if (hasUpToDateMessage) {
      this.#refreshCatchUpWatchdogActive = false
      if (!transition.suppressUpToDate && this.#currentFetchUrl) {
        const shapeKey = canonicalShapeKey(this.#currentFetchUrl)
        upToDateTracker.recordUpToDate(
          shapeKey,
          this.#syncState.liveCacheBuster
        )
        this.#expiredShapeRecoveryKey = null
      }
    }

    // A replay batch that carried nothing but the suppressed up-to-date has
    // nothing to deliver. Skip the callback rather than publishing an empty
    // batch: subscribers are promised one or more messages per notification,
    // and the pre-suppression code never notified for a suppressed replay.
    // Batches emptied by the snapshot tracker alone are still published as
    // before.
    if (messagesToProcess.length === 0 && transition.suppressUpToDate) {
      return Promise.resolve([])
    }

    return this.#publish(messagesToProcess, {
      allowReentrantBypass: opts.allowReentrantPublishBypass,
    })
  }

  /**
   * Fetches the shape from the server using either long polling or SSE.
   * Upon receiving a successful response, the #onInitialResponse method is called.
   * Afterwards, the #onMessages method is called for all the incoming updates.
   * @param opts - The options for the request.
   * @returns A promise that resolves when the request is complete (i.e. the long poll receives a response or the SSE connection is closed).
   */
  async #fetchShape(opts: {
    fetchUrl: URL
    requestAbortController: AbortController
    headers: Record<string, string>
    resumingFromPause?: boolean
  }): Promise<void> {
    // Store current fetch URL for shape key computation
    this.#currentFetchUrl = opts.fetchUrl

    // Check if we should enter replay mode (replaying cached responses)
    // This happens when we're starting fresh (offset=-1 or before first up-to-date)
    // and there's a recent up-to-date in localStorage (< 60s)
    if (!this.#syncState.isUpToDate && this.#syncState.canEnterReplayMode()) {
      const shapeKey = canonicalShapeKey(opts.fetchUrl)
      const lastSeenCursor = upToDateTracker.shouldEnterReplayMode(shapeKey)
      if (lastSeenCursor) {
        // Enter replay mode and store the last seen cursor
        this.#syncState = this.#syncState.enterReplayMode(lastSeenCursor)
      }
    }

    const useSse = this.options.liveSse ?? this.options.experimentalLiveSse
    if (
      this.#syncState.shouldUseSse({
        liveSseEnabled: !!useSse,
        isRefreshing: this.#isRefreshing,
        resumingFromPause: !!opts.resumingFromPause,
      })
    ) {
      opts.fetchUrl.searchParams.set(EXPERIMENTAL_LIVE_SSE_QUERY_PARAM, `true`)
      opts.fetchUrl.searchParams.set(LIVE_SSE_QUERY_PARAM, `true`)
      return this.#requestShapeSSE(opts)
    }

    return this.#requestShapeLongPoll(opts)
  }

  // `start` runs the request and gets `progress`, which it calls when the request moves on, to
  // give the watchdog a whole window again.
  async #withRequestTimeout<T>(
    start: (progress: () => void) => Promise<T>,
    requestAbortController: AbortController,
    fetchUrl: URL
  ): Promise<T> {
    const timeoutMs = this.#liveRequestTimeoutMs
    const isLiveRequest = fetchUrl.searchParams.get(LIVE_QUERY_PARAM) === `true`
    const isRefreshCatchUpRequest =
      this.#isRefreshing || this.#refreshCatchUpWatchdogActive

    if (timeoutMs === false || (!isLiveRequest && !isRefreshCatchUpRequest)) {
      return start(() => {})
    }

    const watchdog = this.#watchRequest(requestAbortController)
    // Retry waits happen on this signal, and the timeout must not run through them.
    const unwatch = watchRetryWaits(requestAbortController.signal, {
      pause: watchdog.pause,
      resume: watchdog.rearm,
    })

    try {
      return await Promise.race([start(watchdog.rearm), watchdog.interrupted])
    } finally {
      unwatch()
      watchdog.dispose()
    }
  }

  /**
   * Restarts a request that receives nothing for `liveRequestTimeoutMs`. The
   * timer runs from the call. A request calls `rearm` when the headers
   * arrive and on every body chunk, which includes an SSE server's keep-alive
   * comments, so a large body that keeps arriving is never cut off. A request that waits to retry calls `pause` for the wait and
   * `rearm` after it, so the next attempt gets a whole window. Call `dispose`
   * when the request is over.
   *
   * `interrupted` rejects with `FetchBackoffAbortError` when the watchdog
   * fires and also when the request is aborted by anything else, so racing a
   * request against it settles even if its fetch or body ignores the signal.
   * An abort also stops the timer, which would otherwise outlive the stream.
   */
  #watchRequest(requestAbortController: AbortController) {
    const timeoutMs = this.#liveRequestTimeoutMs
    const signal = requestAbortController.signal
    let timeout: ReturnType<typeof setTimeout> | undefined
    let rejectInterrupted!: (error: Error) => void
    const interrupted = new Promise<never>((_, reject) => {
      rejectInterrupted = reject
    })
    // Nobody may be racing it yet when it rejects.
    interrupted.catch(() => {})

    const pause = () => clearTimeout(timeout)
    const rearm = () => {
      pause()
      if (timeoutMs === false || signal.aborted) return
      timeout = setTimeout(() => {
        if (!signal.aborted) {
          this.#restartAbortControllers.add(requestAbortController)
          requestAbortController.abort(LIVE_REQUEST_TIMEOUT)
        }
        rejectInterrupted(new FetchBackoffAbortError())
      }, timeoutMs)
    }
    const onAbort = () => {
      pause()
      rejectInterrupted(new FetchBackoffAbortError())
    }
    const dispose = () => {
      pause()
      signal.removeEventListener(`abort`, onAbort)
    }

    if (signal.aborted) {
      onAbort()
    } else {
      signal.addEventListener(`abort`, onAbort, { once: true })
      rearm()
    }
    return { rearm, pause, interrupted, dispose }
  }

  async #requestShapeLongPoll(opts: {
    fetchUrl: URL
    requestAbortController: AbortController
    headers: Record<string, string>
  }): Promise<void> {
    const { fetchUrl, requestAbortController, headers } = opts
    const fetchUrlString = fetchUrl.toString()
    const isLiveRequest = fetchUrl.searchParams.get(LIVE_QUERY_PARAM) === `true`
    const consumeBody = (raw: Response, onChunk?: () => void) =>
      consumeResponseBody(
        raw,
        fetchUrlString,
        requestAbortController.signal,
        this.#maxResponseBytes,
        onChunk
      )

    // A body cut off mid-read is as transient as a connection dropped before
    // the headers, so retry it with the fetch backoff instead of ending the
    // stream. Nothing is committed to the sync state until this returns.
    const response = await retryOnBodyReadFailure(
      async () => {
        const fetchResponse = this.#sseFetchClient(fetchUrlString, {
          signal: requestAbortController.signal,
          headers,
        })

        // A live long-poll streams its body over a held-open connection, so a
        // body that stalls past the timeout must trip the same watchdog as
        // stalled headers. The watchdog counts the time since the last sign of
        // life, not the time of the whole request, so a large body that keeps
        // arriving finishes. A non-live catch-up is bounded data that may
        // legitimately be slow, so only its headers are watched and the body
        // is read afterwards.
        return isLiveRequest
          ? this.#withRequestTimeout(
              async (progress) => {
                const res = await fetchResponse
                progress()
                return consumeBody(res, progress)
              },
              requestAbortController,
              fetchUrl
            )
          : this.#withRequestTimeout(
              () => fetchResponse,
              requestAbortController,
              fetchUrl
            ).then((res) => consumeBody(res))
      },
      this.#backoffOptions,
      requestAbortController.signal
    )

    this.#connected = true
    // #onInitialResponse commits the offset and live cursor from the response
    // headers before the body below is parsed. Snapshot the pre-response sync
    // state so a body that fails to parse (or is not an array) rolls the offset
    // back instead of advancing past a chunk that was never applied — otherwise
    // an onError retry resumes past it and skips those rows silently.
    const preResponseSyncState = this.#syncState
    const shouldProcessBody = await this.#onInitialResponse(response)
    if (!shouldProcessBody) return

    const schema = this.#syncState.schema! // we know that it is not undefined because it is set by `this.#onInitialResponse`
    const res = await response.text()
    const messages = res || `[]`
    let batch: Array<Message<T>>
    try {
      batch = this.#messageParser.parse<Array<Message<T>>>(messages, schema)

      if (!Array.isArray(batch)) {
        const preview = bigintSafeStringify(batch)?.slice(0, 200)
        throw new FetchError(
          response.status,
          `Received non-array response body from shape endpoint. ` +
            `This may indicate a proxy or CDN is returning an unexpected response. ` +
            `Expected a JSON array, got ${typeof batch}: ${preview}`,
          undefined,
          Object.fromEntries(response.headers.entries()),
          fetchUrl.toString()
        )
      }

      await this.#onMessages(batch)
    } catch (bodyError) {
      this.#syncState = preResponseSyncState
      throw bodyError
    }
  }

  async #requestShapeSSE(opts: {
    fetchUrl: URL
    requestAbortController: AbortController
    headers: Record<string, string>
  }): Promise<void> {
    const { fetchUrl, requestAbortController, headers } = opts
    const watchdog = this.#watchRequest(requestAbortController)

    // Track when the SSE connection starts
    this.#lastSseConnectionStartTime = Date.now()

    // Add Accept header for SSE requests
    const sseHeaders = {
      ...headers,
      Accept: `text/event-stream`,
    }

    let ignoredStaleResponse = false
    let buffer: Array<Message<T>> = []
    let receivedMessages = false
    let unflushedBytes = 0
    let exceededByteLimit = false
    let closedForSlowSubscribers = false
    try {
      const sse = fetchEventSource(fetchUrl.toString(), {
        headers: sseHeaders,
        // The SSE parser drops keep-alive comments before `onmessage`, so the
        // watchdog watches the raw body chunks instead.
        fetch: async (input, init) => {
          // The retry waits of this fetch are found by the signal it was given,
          // which is fetchEventSource's own, not `requestAbortController`'s.
          const unwatch = watchRetryWaits(init?.signal, {
            pause: watchdog.pause,
            resume: watchdog.rearm,
          })
          try {
            return observeResponseBodyChunks(
              await this.#sseFetchClient(input, init),
              (chunk) => {
                watchdog.rearm()
                unflushedBytes += chunk.byteLength
                if (unflushedBytes > this.#maxResponseBytes) {
                  exceededByteLimit = true
                  throw new Error(`SSE messages exceeded maxResponseBytes`)
                }
              },
              // Aborted with the request, and by fetchEventSource for each
              // attempt, which `requestAbortController` is not.
              init?.signal ?? undefined
            )
          } finally {
            unwatch()
          }
        },
        // A fetch that ignored the watchdog's abort can still resolve after
        // the request restarted; its response must not reach the stream.
        onopen: async (response: Response) => {
          if (requestAbortController.signal.aborted) {
            throw new FetchBackoffAbortError()
          }
          watchdog.rearm()
          this.#connected = true
          const shouldProcessBody = await this.#onInitialResponse(
            response,
            true
          )
          if (!shouldProcessBody) {
            ignoredStaleResponse = true
            throw new Error(`stale response ignored`)
          }
        },
        onmessage: (event: EventSourceMessage) => {
          if (requestAbortController.signal.aborted) return
          if (event.data) {
            receivedMessages = true
            // event.data is a single JSON object
            const schema = this.#syncState.schema! // we know that it is not undefined because it is set in onopen when we call this.#onInitialResponse
            const message = this.#messageParser.parse<Message<T>>(
              event.data,
              schema
            )
            buffer.push(message)

            if (isUpToDateMessage(message)) {
              // Flush the buffer on up-to-date message.
              // Ensures that we only process complete batches of operations.
              // A batch that cannot be processed throws out of `onmessage`,
              // so the connection closes and the batch stays unflushed.
              const delivered = this.#onMessages(buffer, true)
              const batchBytes = unflushedBytes
              unflushedBytes = 0
              this.#undeliveredSseBytes += batchBytes
              delivered.finally(() => {
                this.#undeliveredSseBytes -= batchBytes
              })
              buffer = []

              // `onmessage` cannot wait for subscribers. Closing the
              // connection is the only way to stop the server sending.
              if (this.#undeliveredSseBytes > this.#maxResponseBytes) {
                closedForSlowSubscribers = true
                throw new Error(`Undelivered SSE messages exceeded the limit`)
              }
            }
          }
        },
        onerror: (error: Error) => {
          // rethrow to close the SSE connection
          throw error
        },
        signal: requestAbortController.signal,
      })
      await Promise.race([sse, watchdog.interrupted])
    } catch (error) {
      if (ignoredStaleResponse) {
        // Stale response was ignored in onopen — let the fetch loop retry
        return
      }
      if (requestAbortController.signal.aborted) {
        // An abort during SSE stream parsing produces a raw AbortError
        // instead of going through createFetchWithBackoff -- wrap it so
        // #start handles it correctly.
        throw new FetchBackoffAbortError()
      }
      // Re-throw known Electric errors so the caller can handle them
      // (e.g., 409 shape rotation, stale cache retry, missing headers).
      // Other errors (body parsing, SSE protocol failures, null body)
      // are SSE connection failures handled by the fallback mechanism
      // in the finally block below.
      if (
        error instanceof FetchError ||
        error instanceof StaleCacheError ||
        error instanceof MissingHeadersError
      ) {
        throw error
      }
    } finally {
      watchdog.dispose()

      // Check if the SSE connection closed too quickly
      // This can happen when responses are cached or when the proxy/server
      // is misconfigured for SSE and closes the connection immediately
      const connectionDuration = Date.now() - this.#lastSseConnectionStartTime!
      const wasAborted =
        requestAbortController.signal.aborted || closedForSlowSubscribers

      const transition = this.#syncState.handleSseConnectionClosed({
        connectionDuration,
        wasAborted,
        hadUnflushedMessages: buffer.length > 0 || exceededByteLimit,
        // The header alone can move the offset; it counts only with messages.
        advancedOffset:
          receivedMessages &&
          this.#syncState.offset !==
            fetchUrl.searchParams.get(OFFSET_QUERY_PARAM),
        minConnectionDuration: this.#minSseConnectionDuration,
        maxShortConnections: this.#maxShortSseConnections,
      })
      this.#syncState = transition.state

      if (transition.fellBackToLongPolling) {
        console.warn(
          `[Electric] SSE connections are closing immediately (possibly due to proxy buffering or misconfiguration). ` +
            `Falling back to long polling. ` +
            `Your proxy must support streaming SSE responses (not buffer the complete response). ` +
            `Configuration: Nginx add 'X-Accel-Buffering: no', Caddy add 'flush_interval -1' to reverse_proxy. ` +
            `Note: Do NOT disable caching entirely - Electric uses cache headers to enable request collapsing for efficiency.`,
          new Error(`stack trace`)
        )
      } else if (transition.wasShortConnection) {
        // Exponential backoff with full jitter: random(0, min(cap, base * 2^attempt))
        const maxDelay = Math.min(
          this.#sseBackoffMaxDelay,
          this.#sseBackoffBaseDelay *
            Math.pow(2, this.#syncState.consecutiveShortSseConnections)
        )
        const delayMs = Math.floor(Math.random() * maxDelay)
        await new Promise((resolve) => setTimeout(resolve, delayMs))
      }

      if (this.#undeliveredSseBytes > this.#maxResponseBytes) {
        await this.#messageChain
      }
    }
  }

  subscribe(
    callback: (messages: Message<T>[]) => MaybePromise<void>,
    onError: (error: Error) => void = () => {}
  ) {
    const subscriptionId = {}

    this.#subscribers.set(subscriptionId, [callback, onError])
    if (!this.#started) {
      this.#start().catch(() => {
        // Errors from #start are handled internally via onError.
        // This catch prevents unhandled promise rejection in Node/Bun.
      })
    }

    return () => {
      this.#subscribers.delete(subscriptionId)
    }
  }

  unsubscribeAll(): void {
    this.#subscribers.clear()
    this.#stop()
  }

  // Tears the stream down for good: the long-poll loop cannot recurse past the `#stopped` guard,
  // the in-flight request is aborted, `#teardown` drops the wake listener, and the constructor-
  // registered visibility listener is dropped here — only on this terminal path. Without it,
  // `unsubscribeAll` left an orphan polling forever and leaking a hidden-tab subscription.
  #stop(): void {
    this.#stopped = true
    this.#requestAbortController?.abort(STREAM_STOPPED)
    this.#teardownForGood()
  }

  /** Unix time at which we last synced. Undefined until first successful up-to-date. */
  lastSyncedAt(): number | undefined {
    return this.#syncState.lastSyncedAt
  }

  /** Time elapsed since last sync (in ms). Infinity if we did not yet sync. */
  lastSynced(): number {
    if (this.#syncState.lastSyncedAt === undefined) return Infinity
    return Date.now() - this.#syncState.lastSyncedAt
  }

  /** Indicates if we are connected to the Electric sync service. */
  isConnected(): boolean {
    return this.#connected
  }

  /** True during initial fetch. False afterwards.  */
  isLoading(): boolean {
    return !this.#syncState.isUpToDate
  }

  hasStarted(): boolean {
    return this.#started
  }

  isPaused(): boolean {
    return this.#pauseLock.isPaused
  }

  /** Await the next tick of the request loop */
  async #nextTick() {
    if (this.#pauseLock.isPaused) {
      throw new Error(
        `Cannot wait for next tick while PauseLock is held — this would deadlock because the request loop is paused`
      )
    }
    if (this.#tickPromise) {
      return this.#tickPromise
    }
    this.#tickPromise = new Promise((resolve, reject) => {
      this.#tickPromiseResolver = resolve
      this.#tickPromiseRejecter = reject
    })
    this.#tickPromise
      .finally(() => {
        this.#tickPromise = undefined
        this.#tickPromiseResolver = undefined
        this.#tickPromiseRejecter = undefined
      })
      .catch(() => {
        // The original tick promise is returned to callers; this chained promise
        // is only for cleanup, so consume rejections to avoid unhandled errors.
      })
    return this.#tickPromise
  }

  /**
   * Refreshes the shape stream.
   * This preemptively aborts any ongoing long poll and reconnects without
   * long polling, ensuring that the stream receives an up to date message with the
   * latest LSN from Postgres at that point in time.
   */
  async forceDisconnectAndRefresh(): Promise<void> {
    this.#refreshCount++
    this.#refreshCatchUpWatchdogActive = true
    try {
      const requestAbortController = this.#requestAbortController
      if (
        this.#syncState.isUpToDate &&
        requestAbortController &&
        !requestAbortController.signal.aborted
      ) {
        // If we are "up to date", any current request will be a "live" request
        // and needs to be aborted. Track restart intent ourselves instead of
        // relying only on AbortSignal.reason, which is missing in some React
        // Native runtimes.
        this.#restartAbortControllers.add(requestAbortController)
        requestAbortController.abort(FORCE_DISCONNECT_AND_REFRESH)
      }
      await this.#nextTick()
    } finally {
      this.#refreshCount--
    }
  }

  async #publish(
    messages: Message<T>[],
    opts: { allowReentrantBypass?: boolean } = {}
  ): Promise<void[]> {
    const deliver = () =>
      Promise.all(
        Array.from(this.#subscribers.values()).map(async ([callback, __]) => {
          try {
            await callback(messages)
          } catch (err) {
            queueMicrotask(() => {
              throw err
            })
          }
        })
      )

    // We process messages asynchronously but SSE's `onmessage` handler is
    // synchronous. Use a promise chain to ensure handlers execute sequentially
    // in the order messages were received. Only requestSnapshot's injected
    // snapshot batch is allowed to bypass the queue reentrantly; ordinary
    // stream batches (including SSE batches) must remain serialized.
    if (this.#isPublishing && opts.allowReentrantBypass) {
      return deliver()
    }

    this.#messageChain = this.#messageChain.then(async () => {
      this.#isPublishing = true
      try {
        return await deliver()
      } finally {
        this.#isPublishing = false
      }
    })

    return this.#messageChain
  }

  #sendErrorToSubscribers(error: Error) {
    this.#subscribers.forEach(([_, errorFn]) => {
      errorFn?.(error)
    })
  }

  #hasBrowserVisibilityAPI(): boolean {
    return (
      typeof document === `object` &&
      typeof document.hidden === `boolean` &&
      typeof document.addEventListener === `function`
    )
  }

  #setVisibilityPaused(isHidden: boolean) {
    if (isHidden) {
      this.#pauseLock.acquire(`visibility`)
    } else if (this.#pauseLock.isHeldBy(`visibility`)) {
      this.#pauseLock.release(`visibility`)
    }
  }

  #subscribeToVisibilityChanges() {
    const runtimeVisibility =
      this.options.runtimeVisibility ?? getDefaultRuntimeVisibilityAdapter()

    if (runtimeVisibility) {
      this.#setVisibilityPaused(
        runtimeVisibility.getCurrentState?.() === `hidden`
      )

      const unsubscribe = runtimeVisibility.subscribe((state) => {
        this.#setVisibilityPaused(state === `hidden`)
      })

      this.#unsubscribeFromVisibilityChanges = () => {
        unsubscribe()
        this.#unsubscribeFromVisibilityChanges = undefined
      }
      return
    }

    if (this.#hasBrowserVisibilityAPI()) {
      const visibilityHandler = () => {
        this.#setVisibilityPaused(document.hidden)
      }

      visibilityHandler()
      document.addEventListener(`visibilitychange`, visibilityHandler)

      // Store cleanup function to remove the event listener
      this.#unsubscribeFromVisibilityChanges = () => {
        document.removeEventListener(`visibilitychange`, visibilityHandler)
        this.#unsubscribeFromVisibilityChanges = undefined
      }
    }
  }

  /**
   * Detects system wake from sleep using timer gap detection.
   * When the system sleeps, setInterval timers are paused. On wake,
   * the elapsed wall-clock time since the last tick will be much larger
   * than the interval period, indicating the system was asleep.
   *
   * Only active in non-browser environments (Bun, Node.js) where
   * `document.visibilitychange` is not available. In browsers,
   * `#subscribeToVisibilityChanges` handles this instead. Without wake
   * detection, in-flight HTTP requests (long-poll or SSE) may hang until
   * the OS TCP timeout.
   */
  #forceDisconnectAndRefreshFromWake() {
    const requestAbortController = this.#requestAbortController
    if (
      this.#pauseLock.isPaused ||
      !requestAbortController ||
      requestAbortController.signal.aborted ||
      this.options.signal?.aborted
    ) {
      return
    }

    this.#refreshCount++
    this.#refreshCatchUpWatchdogActive = true
    // Track restart intent ourselves instead of relying only on
    // AbortSignal.reason, which is missing in some React Native runtimes.
    this.#restartAbortControllers.add(requestAbortController)
    requestAbortController.abort(SYSTEM_WAKE)

    this.#nextTick()
      .catch(() => {
        // Teardown or user abort can reject the tick promise. Wake recovery runs
        // from a timer callback, so avoid surfacing that as an unhandled rejection.
      })
      .finally(() => {
        this.#refreshCount--
      })
  }

  #subscribeToWakeDetection() {
    if (this.#hasBrowserVisibilityAPI()) return
    if (this.#unsubscribeFromWakeDetection) return

    const INTERVAL_MS = 2_000
    const WAKE_THRESHOLD_MS = 4_000

    let lastTickTime = Date.now()

    const timer = setInterval(() => {
      const now = Date.now()
      const elapsed = now - lastTickTime
      lastTickTime = now

      if (elapsed > INTERVAL_MS + WAKE_THRESHOLD_MS) {
        this.#forceDisconnectAndRefreshFromWake()
      }
    }, INTERVAL_MS)

    // Ensure the timer doesn't prevent the process from exiting
    if (typeof timer === `object` && `unref` in timer) {
      timer.unref()
    }

    this.#unsubscribeFromWakeDetection = () => {
      clearInterval(timer)
      this.#unsubscribeFromWakeDetection = undefined
    }
  }

  /**
   * Cuts a retry backoff short when the browser reports it is back online.
   * Nothing else would: a stream that lost its connection waits out its own
   * backoff timer, which can run for tens of seconds. A stream that is not
   * connected restarts its request the way a system wake does, after a random
   * delay so that many streams do not hit the server at the same instant.
   * The restart never comes before a `Retry-After` deadline: the event only
   * says the network is back, not that the server is ready for the retry. The
   * deadline includes the spread the stream's own retry drew on top of the
   * server's time, so streams that were given the same `Retry-After` do not
   * restart together at the moment it ends.
   *
   * Needs a `window` that can listen for events, which Node, Bun and web
   * workers do not have and React Native may not implement.
   */
  #subscribeToOnlineEvent() {
    if (this.#unsubscribeFromOnlineEvent) return
    if (
      typeof window === `undefined` ||
      typeof window.addEventListener !== `function`
    ) {
      return
    }

    const ONLINE_RETRY_JITTER_MS = 500
    let timer: ReturnType<typeof setTimeout> | undefined
    let attemptsAtEvent = 0

    const restart = () => {
      timer = undefined
      if (this.#connected) return
      // The stream's own retry can come first, at the same moment as a restart
      // that waited for the deadline. That request was sent after the event, so
      // restarting it would only send it twice.
      if (this.#fetchAttempts !== attemptsAtEvent) return
      const untilServerDeadline = this.#retryNotBefore - Date.now()
      if (untilServerDeadline > 0) {
        timer = setTimeout(restart, untilServerDeadline)
        return
      }
      this.#forceDisconnectAndRefreshFromWake()
    }

    const onOnline = () => {
      // A connected stream has a healthy request in flight.
      if (this.#connected || timer) return
      attemptsAtEvent = this.#fetchAttempts
      timer = setTimeout(restart, Math.random() * ONLINE_RETRY_JITTER_MS)
    }

    const target = window
    target.addEventListener(`online`, onOnline)

    this.#unsubscribeFromOnlineEvent = () => {
      target.removeEventListener(`online`, onOnline)
      clearTimeout(timer)
      this.#unsubscribeFromOnlineEvent = undefined
    }
  }

  /**
   * Resets the state of the stream, optionally with a provided
   * shape handle
   */
  #reset(handle?: string) {
    this.#syncState = this.#syncState.markMustRefetch(handle)
    this.#connected = false
    // The row map is cleared on rotation, so the per-key snapshot freshness
    // tracking must reset with it.
    this.#snapshotAppliedLsnByKey.clear()
    // releaseAllMatching intentionally doesn't fire onReleased — every caller
    // (#requestShape's 409 handler, #checkFastLoop, and stale-retry
    // self-healing in #onInitialResponse) runs inside the active stream loop,
    // so the stream is already active and doesn't need a resume signal.
    this.#pauseLock.releaseAllMatching(`snapshot`)
  }

  /**
   * Request a snapshot for subset of data and inject it into the subscribed data stream.
   *
   * Only available when mode is `changes_only`.
   * Returns the insertion point & the data, but more importantly injects the data
   * into the subscribed data stream. Returned value is unlikely to be useful for the caller,
   * unless the caller has complicated additional logic.
   *
   * Data will be injected in a way that's also tracking further incoming changes, and it'll
   * skip the ones that are already in the snapshot.
   *
   * @param opts - The options for the snapshot request.
   * @returns The metadata and the data for the snapshot.
   */
  async requestSnapshot(opts: SubsetParams): Promise<{
    metadata: SnapshotMetadata
    data: Array<ChangeMessage<T>>
  }> {
    if (this.#mode === `full`) {
      throw new Error(
        `Snapshot requests are not supported in ${this.#mode} mode, as the consumer is guaranteed to observe all data`
      )
    }
    // Start the stream if not started — fire-and-forget like subscribe() does.
    // We must NOT await #start() because it runs the full request loop. The
    // PauseLock acquire below will abort the in-flight request, and the
    // re-check guard in #requestShape handles the race.
    if (!this.#started) {
      this.#start().catch(() => {})
    }

    const snapshotReason = `snapshot-${++this.#snapshotCounter}`

    this.#pauseLock.acquire(snapshotReason)

    // Warn if the snapshot holds the pause lock for too long — this likely
    // indicates a hung fetch or leaked lock. Visibility pauses are
    // intentionally long-lived so the warning lives here, not in PauseLock.
    const snapshotWarnTimer = setTimeout(() => {
      console.warn(
        `[Electric]`,
        `Snapshot "${snapshotReason}" has held the pause lock for 30s — ` +
          `possible hung request or leaked lock. ` +
          `Current holders: ${[...new Set([snapshotReason])].join(`, `)}`,
        new Error(`stack trace`)
      )
    }, 30_000)

    try {
      const { metadata, data, responseOffset, responseHandle } =
        await this.fetchSnapshot(opts)

      // Drop rows a fresher snapshot has already applied for the same key so an
      // out-of-order older snapshot cannot overwrite a newer row. The freshness
      // map is updated synchronously, before the injection await, so concurrent
      // overlapping snapshots arbitrate against a consistent view regardless of
      // completion order.
      const snapshotLsn = BigInt(metadata.database_lsn)
      const freshData = data.filter((message) => {
        const applied = this.#snapshotAppliedLsnByKey.get(message.key)
        return applied === undefined || snapshotLsn >= applied
      })
      for (const message of freshData) {
        const applied = this.#snapshotAppliedLsnByKey.get(message.key)
        if (applied === undefined || snapshotLsn > applied) {
          this.#snapshotAppliedLsnByKey.set(message.key, snapshotLsn)
        }
      }

      const dataWithEndBoundary = (freshData as Array<Message<T>>).concat([
        { headers: { control: `snapshot-end`, ...metadata } },
        { headers: { control: `subset-end`, ...opts } },
      ])

      this.#snapshotTracker.addSnapshot(
        metadata,
        new Set(data.map((message) => message.key))
      )
      await this.#onMessages(dataWithEndBoundary, false, {
        allowReentrantPublishBypass: true,
      })

      // A snapshot may only move the stream to an earlier offset. It holds
      // just its own subset, so the log between the stream's offset and a
      // later snapshot offset has changes to other rows that it cannot supply,
      // and adopting that offset would drop them silently. A stream with no
      // position yet (`-1`, `now`) takes the snapshot's, and of concurrent
      // cold-start snapshots the earliest wins whichever returns first. The
      // handle is adopted either way.
      const streamOffset = this.#syncState.offset
      const streamHasNoPosition =
        streamOffset === `-1` || streamOffset === `now`
      const snapshotIsEarlier =
        responseOffset !== null &&
        (compareLogOffsets(responseOffset, streamOffset) ?? 0) < 0
      const snapshotOffset =
        streamHasNoPosition || snapshotIsEarlier ? responseOffset : null

      if (snapshotOffset !== null || responseHandle !== null) {
        const transition = this.#syncState.handleResponseMetadata({
          status: 200,
          responseHandle,
          responseOffset: snapshotOffset,
          responseCursor: null,
          expiredHandle: null,
          isSse: false,
          now: Date.now(),
          maxStaleCacheRetries: this.#maxStaleCacheRetries,
          createCacheBuster,
        })
        if (transition.action === `accepted`) {
          this.#syncState = transition.state
        } else {
          console.warn(
            `[Electric]`,
            `Snapshot response metadata was not accepted ` +
              `by state "${this.#syncState.kind}" (action: ${transition.action}). ` +
              `Stream offset was not advanced from snapshot.`,
            new Error(`stack trace`)
          )
        }
      }

      return {
        metadata,
        data,
      }
    } finally {
      clearTimeout(snapshotWarnTimer)
      this.#pauseLock.release(snapshotReason)
    }
  }

  /**
   * Fetch a snapshot for subset of data.
   * Returns the metadata and the data, but does not inject it into the subscribed data stream.
   *
   * By default, uses GET to send subset parameters as query parameters. This may hit URL length
   * limits (HTTP 414) with large WHERE clauses or many parameters. Set `method: 'POST'` or use
   * `subsetMethod: 'POST'` on the stream to send parameters in the request body instead.
   *
   * @param opts - The options for the snapshot request.
   * @returns The metadata, data, and the response's offset/handle for state advancement.
   */
  async fetchSnapshot(opts: SubsetParams): Promise<{
    metadata: SnapshotMetadata
    data: Array<ChangeMessage<T>>
    responseOffset: Offset | null
    responseHandle: string | null
  }> {
    return this.#fetchSnapshotWithRetry(opts, 0)
  }

  async #fetchSnapshotWithRetry(
    opts: SubsetParams,
    retryCount: number,
    cacheBuster?: string
  ): Promise<{
    metadata: SnapshotMetadata
    data: Array<ChangeMessage<T>>
    responseOffset: Offset | null
    responseHandle: string | null
  }> {
    const method = opts.method ?? this.options.subsetMethod ?? `GET`
    const usePost = method === `POST`

    let fetchUrl: URL
    let fetchOptions: RequestInit

    // The caller's stream signal, never #requestAbortController: the pause lock
    // this snapshot holds has already aborted that controller, so using it would
    // abort the snapshot's own fetch. Without a signal the backoff wrapper's
    // default maxRetries: Infinity keeps retrying a disposed stream's snapshot
    // and holds its pause lock until the request settles.
    const signal = this.options.signal

    if (usePost) {
      const result = await this.#constructUrl(this.options.url, true)
      fetchUrl = result.fetchUrl
      fetchOptions = {
        method: `POST`,
        headers: {
          ...result.requestHeaders,
          'Content-Type': `application/json`,
        },
        body: bigintSafeStringify(this.#buildSubsetBody(opts)),
        signal,
      }
    } else {
      const result = await this.#constructUrl(this.options.url, true, opts)
      fetchUrl = result.fetchUrl
      fetchOptions = { headers: result.requestHeaders, signal }
    }

    // Apply cache buster from same-handle 409 retry
    if (cacheBuster) {
      fetchUrl.searchParams.set(CACHE_BUSTER_QUERY_PARAM, cacheBuster)
      fetchUrl.searchParams.sort()
    }

    // Capture handle before fetch to avoid race conditions if it changes during the request
    const usedHandle = this.#syncState.handle

    let response: Response
    try {
      response = await this.#fetchClient(fetchUrl.toString(), fetchOptions)
    } catch (e) {
      // Handle 409 "must-refetch" - shape handle changed/expired.
      // The fetch wrapper throws FetchError for non-OK responses, so we catch here.
      // Unlike #requestShape, we don't call #reset() here as that would
      // clear the pause lock and break requestSnapshot's pause/resume logic.
      if (e instanceof FetchError && e.status === 409) {
        const nextRetryCount = retryCount + 1
        if (nextRetryCount > this.#maxSnapshotRetries) {
          throw new FetchError(
            502,
            undefined,
            undefined,
            {},
            fetchUrl.toString(),
            `Snapshot request stuck in 409 retry loop after ${this.#maxSnapshotRetries} attempts. ` +
              `This indicates a proxy/CDN misconfiguration. ` +
              `For more information visit the troubleshooting guide: ${TROUBLESHOOTING_URL}`
          )
        }

        if (usedHandle) {
          const shapeKey = canonicalShapeKey(fetchUrl)
          expiredShapesCache.markExpired(shapeKey, usedHandle)
        }

        // For snapshot 409s, only update the handle — don't reset offset/schema/etc.
        // The main stream is paused and should not be disturbed.
        const nextHandle = e.headers[SHAPE_HANDLE_HEADER]
        if (nextHandle) {
          this.#syncState = this.#syncState.withHandle(nextHandle)
        } else {
          console.warn(
            `[Electric] Received 409 response without a shape handle header. ` +
              `This likely indicates a proxy or CDN stripping required headers.`
          )
        }
        const nextCacheBuster = createCacheBuster()

        return this.#fetchSnapshotWithRetry(
          opts,
          nextRetryCount,
          nextCacheBuster
        )
      }
      throw e
    }

    // Handle non-OK responses from custom fetch clients that bypass the wrapper chain
    if (!response.ok) {
      throw await FetchError.fromResponse(response, fetchUrl.toString())
    }

    const schema: Schema =
      this.#syncState.schema ??
      getSchemaFromHeaders(response.headers, {
        required: true,
        url: fetchUrl.toString(),
      })

    const { metadata, data: rawData } = await response.json()
    const data = this.#messageParser.parseSnapshotData<ChangeMessage<T>>(
      rawData,
      schema
    )

    const responseOffset =
      (response.headers.get(CHUNK_LAST_OFFSET_HEADER) as Offset) || null
    const responseHandle = response.headers.get(SHAPE_HANDLE_HEADER)

    return { metadata, data, responseOffset, responseHandle }
  }

  #buildSubsetBody(opts: SubsetParams): Record<string, unknown> {
    const body: Record<string, unknown> = {}

    if (opts.whereExpr) {
      body.where = compileExpression(
        opts.whereExpr,
        this.options.columnMapper?.encode
      )
      body.where_expr = opts.whereExpr
    } else if (opts.where && typeof opts.where === `string`) {
      body.where = encodeWhereClause(
        opts.where,
        this.options.columnMapper?.encode
      )
    }

    if (opts.params) {
      body.params = opts.params
    }

    if (opts.limit !== undefined) {
      body.limit = opts.limit
    }

    if (opts.offset !== undefined) {
      body.offset = opts.offset
    }

    if (opts.orderByExpr) {
      body.order_by = compileOrderBy(
        opts.orderByExpr,
        this.options.columnMapper?.encode
      )
      body.order_by_expr = opts.orderByExpr
    } else if (opts.orderBy && typeof opts.orderBy === `string`) {
      body.order_by = encodeWhereClause(
        opts.orderBy,
        this.options.columnMapper?.encode
      )
    }

    return body
  }
}

/**
 * Extracts the schema from response headers.
 * @param headers - The response headers
 * @param options - Options for schema extraction
 * @param options.required - If true, throws MissingHeadersError when header is missing. Defaults to false.
 * @param options.url - The URL to include in the error message if required is true
 * @returns The parsed schema, or an empty object if not required and header is missing
 * @throws {MissingHeadersError} if required is true and the header is missing
 */
function getSchemaFromHeaders(
  headers: Headers,
  options?: { required?: boolean; url?: string }
): Schema {
  const schemaHeader = headers.get(SHAPE_SCHEMA_HEADER)
  if (!schemaHeader) {
    if (options?.required && options?.url) {
      throw new MissingHeadersError(options.url, [SHAPE_SCHEMA_HEADER])
    }
    return {}
  }
  return JSON.parse(schemaHeader)
}

/**
 * Validates that no reserved parameter names are used in the provided params object
 * @throws {ReservedParamError} if any reserved parameter names are found
 */
function validateParams(params: Record<string, unknown> | undefined): void {
  if (!params) return

  const reservedParams = Object.keys(params).filter((key) =>
    RESERVED_PARAMS.has(key as ReservedParamKeys)
  )
  if (reservedParams.length > 0) {
    throw new ReservedParamError(reservedParams)
  }
}

// Module-level flag to ensure HTTP warning is only shown once per process/module
let didWarnOnHttp = false

/**
 * Safely gets NODE_ENV without throwing ReferenceError in browser builds
 * where `process` may not be defined.
 */
function getNodeEnvSafely(): string | undefined {
  return typeof process !== `undefined` ? process.env?.NODE_ENV : undefined
}

/**
 * Attempts to resolve a URL string, optionally with a base URL.
 * Returns undefined if the URL cannot be parsed.
 */
function resolveUrlMaybe(url: string, base?: string): URL | undefined {
  try {
    return new URL(url, base)
  } catch {
    return undefined
  }
}

/**
 * Checks if we're in a browser environment (has window object).
 */
function isBrowserEnvironment(): boolean {
  return typeof window !== `undefined`
}

/**
 * Gets the current page URL in browser environments.
 * Returns undefined in non-browser environments.
 */
function getWindowLocationHref(): string | undefined {
  if (isBrowserEnvironment() && typeof window.location !== `undefined`) {
    return window.location.href
  }
  return undefined
}

function validateOptions<T>(options: Partial<ShapeStreamOptions<T>>): void {
  if (!options.url) {
    throw new MissingShapeUrlError()
  }
  if (options.signal && !(options.signal instanceof AbortSignal)) {
    throw new InvalidSignalError()
  }

  if (
    options.liveRequestTimeoutMs !== undefined &&
    options.liveRequestTimeoutMs !== false &&
    (!Number.isFinite(options.liveRequestTimeoutMs) ||
      options.liveRequestTimeoutMs <= 0)
  ) {
    throw new InvalidShapeOptionsError(
      `Invalid shape options: liveRequestTimeoutMs must be a positive finite number or false`
    )
  }

  if (
    options.maxResponseBytes !== undefined &&
    (!Number.isFinite(options.maxResponseBytes) ||
      options.maxResponseBytes <= 0)
  ) {
    throw new InvalidShapeOptionsError(
      `Invalid shape options: maxResponseBytes must be a positive finite number`
    )
  }

  if (
    options.offset !== undefined &&
    options.offset !== `-1` &&
    options.offset !== `now` &&
    !options.handle
  ) {
    throw new MissingShapeHandleError()
  }

  validateParams(options.params)

  // Warn about HTTP URLs in browser environments (unless disabled or in tests)
  // HTTP forces HTTP/1.1 which typically limits browsers to ~6 concurrent connections per origin
  const nodeEnv = getNodeEnvSafely()
  const warnOnHttp = options.warnOnHttp ?? nodeEnv !== `test`

  if (warnOnHttp && !didWarnOnHttp && isBrowserEnvironment()) {
    if (typeof console !== `undefined`) {
      // Try to resolve the URL, using window.location.href as base for relative URLs
      const baseUrl = getWindowLocationHref()
      const resolvedUrl = resolveUrlMaybe(options.url, baseUrl)

      // Check if the resolved URL (or the page itself for relative URLs) uses HTTP
      const isHttp = resolvedUrl?.protocol === `http:`

      if (isHttp) {
        didWarnOnHttp = true
        console.warn(
          `[Electric] Using HTTP (not HTTPS) typically limits browsers to ~6 concurrent connections per origin under HTTP/1.1. ` +
            `This can cause slow shapes and app freezes with multiple shapes. ` +
            `Use HTTPS for HTTP/2 support. See: https://electric-sql.com/r/electric-http2`
        )
      }
    }
  }

  return
}

/**
 * Resets the HTTP warning flag. Only for testing purposes.
 * @internal
 */
export function _resetHttpWarningForTesting(): void {
  didWarnOnHttp = false
}

// `unknown` being in the value is a bit of defensive programming if user doesn't use TS
function setQueryParam(
  url: URL,
  key: string,
  value: Record<string, string> | string | unknown
): void {
  if (value === undefined || value == null) {
    return
  } else if (typeof value === `string`) {
    url.searchParams.set(key, value)
  } else if (typeof value === `object`) {
    for (const [k, v] of Object.entries(value)) {
      url.searchParams.set(`${key}[${k}]`, v)
    }
  } else {
    url.searchParams.set(key, value.toString())
  }
}

function convertWhereParamsToObj(
  allPgParams: ExternalParamsRecord<Row>
): ExternalParamsRecord<Row> {
  if (Array.isArray(allPgParams.params)) {
    return {
      ...allPgParams,
      params: Object.fromEntries(allPgParams.params.map((v, i) => [i + 1, v])),
    }
  }
  return allPgParams
}
