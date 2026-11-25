import { afterEach, describe, expect, it, vi } from 'vitest'
import {
  createMockShapeStream,
  upToDateResponse,
} from './support/mock-fetch-harness'

describe(`ShapeStream teardown`, () => {
  let cleanup: () => void = () => {}
  afterEach(() => cleanup())

  // unsubscribeAll used to clear subscribers and the visibility adapter but leave the #requestShape
  // long-poll recursion running, so an orphaned stream polled forever and — having dropped its
  // visibility subscription — no longer paused when the tab was hidden. Teardown must stop the loop.
  it(`stops the long-poll loop after unsubscribeAll`, async () => {
    const { harness, stream, aborter } = createMockShapeStream({
      fallback: () => upToDateResponse(),
    })
    cleanup = () => aborter.abort()

    stream.subscribe(() => {})

    await vi.waitFor(() => expect(stream.isUpToDate).toBe(true))
    await vi.waitFor(() =>
      expect(harness.consumedCount).toBeGreaterThanOrEqual(3)
    )

    stream.unsubscribeAll()
    const countAtTeardown = harness.consumedCount

    // Give the loop several macrotasks to keep polling if it was orphaned.
    await new Promise((resolve) => setTimeout(resolve, 50))

    expect(harness.consumedCount).toBe(countAtTeardown)
  })
})
