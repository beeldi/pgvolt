import { describe, it, expect, vi } from 'vitest'
import {
  FetchError,
  MissingHeadersError,
  ResponseTooLargeError,
} from '../src/error'

describe(`FetchError`, () => {
  it(`should create a FetchError with the correct properties`, () => {
    const status = 404
    const text = `Not Found`
    const json = undefined
    const headers = { 'content-type': `text/plain` }
    const url = `https://example.com/notfound`

    const error = new FetchError(status, text, json, headers, url)

    expect(error).toBeInstanceOf(Error)
    expect(error.name).toBe(`FetchError`)
    expect(error.status).toBe(status)
    expect(error.text).toBe(text)
    expect(error.json).toBe(json)
    expect(error.headers).toEqual(headers)
    expect(error.url).toBe(url)
    expect(error.message).toBe(
      `HTTP Error 404 at https://example.com/notfound: Not Found`
    )
  })

  it(`should create a FetchError with a JSON response and use the JSON in the message`, () => {
    const status = 500
    const text = undefined
    const json = { error: `Internal Server Error` }
    const headers = { 'content-type': `application/json` }
    const url = `https://example.com/servererror`

    const error = new FetchError(status, text, json, headers, url)

    expect(error.status).toBe(status)
    expect(error.text).toBeUndefined()
    expect(error.json).toEqual(json)
    expect(error.headers).toEqual(headers)
    expect(error.message).toBe(
      `HTTP Error 500 at https://example.com/servererror: {"error":"Internal Server Error"}`
    )
  })

  it(`should create a FetchError with a custom message if provided`, () => {
    const status = 403
    const text = `Forbidden`
    const json = undefined
    const headers = { 'content-type': `text/plain` }
    const url = `https://example.com/forbidden`
    const customMessage = `Custom Error Message`

    const error = new FetchError(
      status,
      text,
      json,
      headers,
      url,
      customMessage
    )

    expect(error.message).toBe(customMessage)
  })

  describe(`fromResponse`, () => {
    it(`should create a FetchError from a text-based response`, async () => {
      const mockResponse = {
        status: 404,
        headers: new Headers({ 'content-type': `text/plain` }),
        text: vi.fn().mockResolvedValue(`Not Found`),
      } as unknown as Response

      const url = `https://example.com/notfound`
      const error = await FetchError.fromResponse(mockResponse, url)

      expect(mockResponse.text).toHaveBeenCalled()
      expect(error).toBeInstanceOf(FetchError)
      expect(error.status).toBe(404)
      expect(error.text).toBe(`Not Found`)
      expect(error.json).toBeUndefined()
      expect(error.headers).toEqual({ 'content-type': `text/plain` })
      expect(error.message).toBe(
        `HTTP Error 404 at https://example.com/notfound: Not Found`
      )
    })

    it(`should create a FetchError from a JSON-based response`, async () => {
      const mockResponse = new Response(
        JSON.stringify({ error: `Internal Server Error` }),
        {
          status: 500,
          headers: { 'content-type': `application/json` },
        }
      )

      const url = `https://example.com/servererror`
      const error = await FetchError.fromResponse(mockResponse, url)

      expect(error).toBeInstanceOf(FetchError)
      expect(error.status).toBe(500)
      expect(error.text).toBeUndefined()
      expect(error.json).toEqual({ error: `Internal Server Error` })
      expect(error.headers).toEqual({ 'content-type': `application/json` })
      expect(error.message).toBe(
        `HTTP Error 500 at https://example.com/servererror: {"error":"Internal Server Error"}`
      )
    })

    it(`should handle content-type not set in response headers`, async () => {
      const mockResponse = {
        status: 500,
        headers: new Headers(),
        text: vi.fn().mockResolvedValue(`Server error with no content-type`),
      } as unknown as Response

      const url = `https://example.com/no-content-type`
      const error = await FetchError.fromResponse(mockResponse, url)

      expect(mockResponse.text).toHaveBeenCalled()
      expect(error).toBeInstanceOf(FetchError)
      expect(error.status).toBe(500)
      expect(error.text).toBe(`Server error with no content-type`)
      expect(error.json).toBeUndefined()
      expect(error.headers).toEqual({})
      expect(error.message).toBe(
        `HTTP Error 500 at https://example.com/no-content-type: Server error with no content-type`
      )
    })
  })

  describe(`URLs carried by errors`, () => {
    const url =
      `https://user:pw@example.com/v1/shape?table=private_table&token=secret-value` +
      `&where=tenant%3D1&params%5B1%5D=private-param&offset=-1&handle=h-1&live=true#fragment`

    const reported = `https://example.com/v1/shape?offset=-1&handle=h-1&live=true`

    it(`keeps only the protocol parameters in the url and the message`, () => {
      const error = new FetchError(400, `Bad`, undefined, {}, url)

      expect(error.url).toBe(reported)
      expect(error.message).toBe(`HTTP Error 400 at ${reported}: Bad`)
    })

    it(`keeps only the protocol parameters when reading from a response`, async () => {
      const error = await FetchError.fromResponse(
        new Response(`Bad`, { status: 400 }),
        url
      )

      expect(error.url).toBe(reported)
      expect(error.message).not.toContain(`secret-value`)
    })

    it(`cuts a URL it cannot parse at its query`, () => {
      const error = new FetchError(
        400,
        `Bad`,
        undefined,
        {},
        `/v1/shape?token=secret-value`
      )

      expect(error.url).toBe(`/v1/shape`)
    })

    it(`leaves them out of the missing headers message`, () => {
      const error = new MissingHeadersError(url, [`electric-offset`])

      expect(error.message).toContain(reported)
      expect(error.message).not.toContain(`secret-value`)
      expect(error.message).not.toContain(`private_table`)
    })

    it(`leaves them out of the too large response error`, () => {
      const error = new ResponseTooLargeError(url, 10)

      expect(error.url).toBe(reported)
      expect(error.message).not.toContain(`private-param`)
    })
  })

  describe(`size of an error body`, () => {
    const tooLongForAnErrorBody = 2 * 1024 * 1024

    const longBody = (cancel: () => void) => {
      let sent = 0
      return new ReadableStream<Uint8Array>({
        pull(controller) {
          if (sent >= tooLongForAnErrorBody) return controller.close()
          sent += 64 * 1024
          controller.enqueue(new Uint8Array(64 * 1024).fill(97))
        },
        cancel,
      })
    }

    it(`stops reading a text body past the limit and keeps what it read`, async () => {
      const cancel = vi.fn()

      const error = await FetchError.fromResponse(
        new Response(longBody(cancel), { status: 500 }),
        `https://example.com/v1/shape`
      )

      expect(cancel).toHaveBeenCalledOnce()
      expect(error.text?.length).toBeLessThanOrEqual(1024 * 1024)
      expect(error.text?.length).toBeGreaterThan(0)
    })

    it(`keeps a JSON body past the limit as text`, async () => {
      const error = await FetchError.fromResponse(
        new Response(
          longBody(() => {}),
          {
            status: 500,
            headers: { 'content-type': `application/json` },
          }
        ),
        `https://example.com/v1/shape`
      )

      expect(error.json).toBeUndefined()
      expect(error.text?.length).toBeGreaterThan(0)
    })
  })
})
