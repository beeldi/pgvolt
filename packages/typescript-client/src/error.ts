import {
  CACHE_BUSTER_QUERY_PARAM,
  EXPERIMENTAL_LIVE_SSE_QUERY_PARAM,
  EXPIRED_HANDLE_QUERY_PARAM,
  LIVE_CACHE_BUSTER_QUERY_PARAM,
  LIVE_QUERY_PARAM,
  LIVE_SSE_QUERY_PARAM,
  LOG_MODE_QUERY_PARAM,
  OFFSET_QUERY_PARAM,
  SHAPE_HANDLE_QUERY_PARAM,
} from './constants'
import { readBodyUpTo } from './response-body'

const MAX_ERROR_BODY_BYTES = 1024 * 1024

const REPORTED_QUERY_PARAMS = new Set([
  CACHE_BUSTER_QUERY_PARAM,
  EXPERIMENTAL_LIVE_SSE_QUERY_PARAM,
  EXPIRED_HANDLE_QUERY_PARAM,
  LIVE_CACHE_BUSTER_QUERY_PARAM,
  LIVE_QUERY_PARAM,
  LIVE_SSE_QUERY_PARAM,
  LOG_MODE_QUERY_PARAM,
  OFFSET_QUERY_PARAM,
  SHAPE_HANDLE_QUERY_PARAM,
])

/**
 * The URL an error may carry into logs and telemetry: the Electric protocol
 * parameters only, without user info. Application parameters, the shape
 * definition and its predicates stay out of it.
 */
function urlForReporting(url: string): string {
  if (!/[?#@]/.test(url)) return url

  try {
    const parsed = new URL(url)
    parsed.search = new URLSearchParams(
      [...parsed.searchParams].filter(([name]) =>
        REPORTED_QUERY_PARAMS.has(name)
      )
    ).toString()
    parsed.username = ``
    parsed.password = ``
    parsed.hash = ``
    return parsed.toString()
  } catch {
    return url.split(/[?#]/, 1)[0].replace(/\/\/[^/]*@/, `//`)
  }
}

export class FetchError extends Error {
  status: number
  text?: string
  json?: object
  headers: Record<string, string>
  url: string

  constructor(
    status: number,
    text: string | undefined,
    json: object | undefined,
    headers: Record<string, string>,
    url: string,
    message?: string
  ) {
    const reportedUrl = urlForReporting(url)
    super(
      message ||
        `HTTP Error ${status} at ${reportedUrl}: ${text ?? JSON.stringify(json)}`
    )
    this.name = `FetchError`
    this.status = status
    this.text = text
    this.json = json
    this.headers = headers
    this.url = reportedUrl
  }

  static async fromResponse(
    response: Response,
    url: string
  ): Promise<FetchError> {
    const status = response.status
    const headers = Object.fromEntries([...response.headers.entries()])
    let text: string | undefined = undefined
    let json: object | undefined = undefined

    const contentType = response.headers.get(`content-type`)
    if (!response.bodyUsed) {
      const body = await readBodyUpTo(response, MAX_ERROR_BODY_BYTES)
      if (
        body.complete &&
        contentType &&
        contentType.includes(`application/json`)
      ) {
        json = JSON.parse(body.text) as object
      } else {
        text = body.text
      }
    }

    return new FetchError(status, text, json, headers, url)
  }
}

/**
 * A response body is longer than `maxResponseBytes`. Sending the same request
 * again would get the same body, so this ends the stream unless `onError`
 * changes something.
 */
export class ResponseTooLargeError extends Error {
  url: string
  maxBytes: number

  constructor(url: string, maxBytes: number) {
    const reportedUrl = urlForReporting(url)
    super(
      `The response at ${reportedUrl} is longer than the limit of ${maxBytes} bytes. ` +
        `Raise maxResponseBytes if responses of this size are expected.`
    )
    this.name = `ResponseTooLargeError`
    this.url = reportedUrl
    this.maxBytes = maxBytes
  }
}

/**
 * The response headers arrived but the body could not be read in full, e.g.
 * the connection dropped mid-body. Unlike a 4xx it says nothing about the
 * request, so the same request is worth sending again.
 */
export class FetchBodyReadError extends FetchError {}

export class FetchBackoffAbortError extends Error {
  constructor() {
    super(`Fetch with backoff aborted`)
    this.name = `FetchBackoffAbortError`
  }
}

export class InvalidShapeOptionsError extends Error {
  constructor(message: string) {
    super(message)
    this.name = `InvalidShapeOptionsError`
  }
}

export class MissingShapeUrlError extends Error {
  constructor() {
    super(`Invalid shape options: missing required url parameter`)
    this.name = `MissingShapeUrlError`
  }
}

export class InvalidSignalError extends Error {
  constructor() {
    super(`Invalid signal option. It must be an instance of AbortSignal.`)
    this.name = `InvalidSignalError`
  }
}

export class MissingShapeHandleError extends Error {
  constructor() {
    super(
      `shapeHandle is required if this isn't an initial fetch (i.e. offset > -1)`
    )
    this.name = `MissingShapeHandleError`
  }
}

export class ReservedParamError extends Error {
  constructor(reservedParams: string[]) {
    super(
      `Cannot use reserved Electric parameter names in custom params: ${reservedParams.join(`, `)}`
    )
    this.name = `ReservedParamError`
  }
}

export class ParserNullValueError extends Error {
  constructor(columnName: string) {
    super(`Column "${columnName ?? `unknown`}" does not allow NULL values`)
    this.name = `ParserNullValueError`
  }
}

export class ParserArrayShapeError extends Error {
  constructor(columnName: string, type: string) {
    super(
      `Column "${columnName ?? `unknown`}" is typed as scalar "${type}" but received an array literal`
    )
    this.name = `ParserArrayShapeError`
  }
}

export class ParserArrayBoundsError extends Error {
  constructor(literal: string) {
    super(
      `Postgres array literal "${literal}" has an explicit lower bound, which cannot be represented as a zero-indexed array`
    )
    this.name = `ParserArrayBoundsError`
  }
}

export class ParserArrayDepthError extends Error {
  constructor(maxDimensions: number) {
    super(
      `Postgres array literal is nested deeper than the ${maxDimensions} dimensions Postgres allows`
    )
    this.name = `ParserArrayDepthError`
  }
}

export class ShapeStreamAlreadyRunningError extends Error {
  constructor() {
    super(`ShapeStream is already running`)
    this.name = `ShapeStreamAlreadyRunningError`
  }
}

export class MissingHeadersError extends Error {
  constructor(url: string, missingHeaders: Array<string>) {
    let msg = `The response for the shape request to ${urlForReporting(url)} didn't include the following required headers:\n`
    missingHeaders.forEach((h) => {
      msg += `- ${h}\n`
    })
    msg += `\nThis is often due to a proxy not setting CORS correctly so that all Electric headers can be read by the client.`
    msg += `\nFor more information visit the troubleshooting guide: /docs/guides/troubleshooting/missing-headers`
    super(msg)
  }
}

export class StaleCacheError extends Error {
  constructor(message: string) {
    super(message)
    this.name = `StaleCacheError`
  }
}
