// @vitest-environment node
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { ShapeStream } from '../src'
import type { RuntimeVisibilityState } from '../src'
import {
  MockFetchHarness,
  errorResponse,
  upToDateResponse,
} from './support/mock-fetch-harness'

function visibilityAdapter() {
  let notify: ((state: RuntimeVisibilityState) => void) | undefined
  const unsubscribe = vi.fn()
  return {
    unsubscribe,
    change: (state: RuntimeVisibilityState) => notify?.(state),
    adapter: {
      getCurrentState: () => `visible` as const,
      subscribe: (callback: (state: RuntimeVisibilityState) => void) => {
        notify = callback
        return unsubscribe
      },
    },
  }
}

describe(`visibility listener of a ShapeStream`, () => {
  let aborter: AbortController

  beforeEach(() => {
    aborter = new AbortController()
  })

  afterEach(() => {
    aborter.abort()
  })

  function streamWith(
    harness: MockFetchHarness,
    options: { subscribe?: boolean; signal?: AbortSignal },
    visibility: ReturnType<typeof visibilityAdapter>
  ) {
    return new ShapeStream({
      url: `http://localhost:3000/v1/shape`,
      params: { table: `test_table` },
      fetchClient: harness.fetchClient,
      runtimeVisibility: visibility.adapter,
      ...options,
    })
  }

  it(`is removed when the caller's signal aborts the stream`, async () => {
    const visibility = visibilityAdapter()
    const harness = new MockFetchHarness().onFallback(() => upToDateResponse())
    const own = new AbortController()
    const stream = streamWith(harness, { signal: own.signal }, visibility)
    stream.subscribe(() => {})
    await vi.waitFor(() => expect(harness.requests.length).toBeGreaterThan(1))
    expect(visibility.unsubscribe).not.toHaveBeenCalled()

    own.abort()

    await vi.waitFor(() => expect(visibility.unsubscribe).toHaveBeenCalled())
  })

  it(`is removed when a stream that does not subscribe completes`, async () => {
    const visibility = visibilityAdapter()
    const harness = new MockFetchHarness().enqueue(upToDateResponse())
    const stream = streamWith(
      harness,
      { subscribe: false, signal: aborter.signal },
      visibility
    )
    stream.subscribe(() => {})

    await vi.waitFor(() => expect(visibility.unsubscribe).toHaveBeenCalled())
  })

  it(`is removed when the stream ends on an unrecoverable error`, async () => {
    const visibility = visibilityAdapter()
    const harness = new MockFetchHarness().enqueue(errorResponse(400))
    const stream = streamWith(harness, { signal: aborter.signal }, visibility)
    stream.subscribe(
      () => {},
      () => {}
    )

    await vi.waitFor(() => expect(visibility.unsubscribe).toHaveBeenCalled())
  })

  it(`is kept while a live stream is paused by a hidden page, and resumes it`, async () => {
    const visibility = visibilityAdapter()
    const harness = new MockFetchHarness().onFallback(() => upToDateResponse())
    const stream = streamWith(harness, { signal: aborter.signal }, visibility)
    stream.subscribe(() => {})
    await vi.waitFor(() => expect(harness.requests.length).toBeGreaterThan(1))

    visibility.change(`hidden`)
    await vi.waitFor(() => expect(stream.isPaused()).toBe(true))
    await new Promise((resolve) => setTimeout(resolve, 50))
    expect(visibility.unsubscribe).not.toHaveBeenCalled()

    const requestsWhileHidden = harness.requests.length
    visibility.change(`visible`)
    await vi.waitFor(() =>
      expect(harness.requests.length).toBeGreaterThan(requestsWhileHidden)
    )
    expect(visibility.unsubscribe).not.toHaveBeenCalled()
  })
})
