import { afterEach, describe, expect, it, vi } from 'vitest'
import { ShapeStream } from '../src/client'
import type {
  RuntimeVisibilityAdapter,
  RuntimeVisibilityState,
} from '../src/client'
import {
  MockFetchHarness,
  upToDateResponse,
} from './support/mock-fetch-harness'

// A visibility adapter whose unsubscribe genuinely detaches the callback, so a
// resume delivered after teardown reaches nothing. The browser `document` mock
// cannot express this: its removeEventListener is a no-op, so a removed handler
// still fires and hides the regression.
function controllableVisibility() {
  let callback: ((state: RuntimeVisibilityState) => void) | undefined
  let state: RuntimeVisibilityState = `visible`

  const adapter: RuntimeVisibilityAdapter = {
    getCurrentState: () => state,
    subscribe: (cb) => {
      callback = cb
      return () => {
        callback = undefined
      }
    },
  }

  return {
    adapter,
    setState(next: RuntimeVisibilityState) {
      state = next
      callback?.(next)
    },
    get subscribed() {
      return callback !== undefined
    },
  }
}

describe(`ShapeStream visibility pause/resume`, () => {
  let cleanup: () => void = () => {}
  afterEach(() => cleanup())

  // A visibility pause runs #teardown (via #start's per-request-return teardown), which used to
  // drop the visibility listener. That listener is registered once in the constructor, not per
  // #start, so dropping it left nothing to observe the tab becoming visible again and the stream
  // never resumed. Teardown must keep the listener; only the terminal #stop path drops it.
  it(`resumes long-polling after a hidden -> visible cycle`, async () => {
    const harness = new MockFetchHarness()
    harness.onFallback(() => upToDateResponse())
    const aborter = new AbortController()
    const visibility = controllableVisibility()

    const stream = new ShapeStream({
      url: `http://localhost:3000/v1/shape`,
      params: { table: `test_table` },
      fetchClient: harness.fetchClient,
      signal: aborter.signal,
      runtimeVisibility: visibility.adapter,
    })
    cleanup = () => aborter.abort()

    stream.subscribe(() => {})

    await vi.waitFor(() => expect(stream.isUpToDate).toBe(true))
    await vi.waitFor(() =>
      expect(harness.consumedCount).toBeGreaterThanOrEqual(3)
    )

    visibility.setState(`hidden`)
    await vi.waitFor(() => expect(stream.isPaused()).toBe(true))
    const countWhilePaused = harness.consumedCount
    await new Promise((resolve) => setTimeout(resolve, 50))

    expect(harness.consumedCount).toBe(countWhilePaused)
    expect(visibility.subscribed).toBe(true)

    visibility.setState(`visible`)

    await vi.waitFor(() =>
      expect(harness.consumedCount).toBeGreaterThan(countWhilePaused)
    )
  })
})
