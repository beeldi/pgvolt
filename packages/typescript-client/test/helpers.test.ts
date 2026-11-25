import { describe, expect, it } from 'vitest'
import { isChangeMessage, isControlMessage, Message } from '../src'
import {
  isUpToDateMessage,
  bigintSafeStringify,
  compareLogOffsets,
  isOffsetAhead,
  toError,
} from '../src/helpers'

describe(`helpers`, () => {
  const changeMsg = {
    headers: {
      operation: `insert`,
    },
    offset: `-1`,
    key: `key`,
    value: { key: `value` },
  } as Message

  const upToDateMsg = {
    headers: {
      control: `up-to-date`,
    },
  } as Message

  const mustRefetchMsg = {
    headers: {
      control: `must-refetch`,
    },
  } as Message

  const eventMsg = {
    headers: {
      event: `move-out`,
      patterns: [{ pos: 0, value: `test` }],
    },
  } as Message

  it(`should correctly detect ChangeMessages`, () => {
    expect(isChangeMessage(changeMsg)).toBe(true)
    expect(isControlMessage(changeMsg)).toBe(false)
  })

  it(`should correctly detect ControlMessages`, () => {
    expect(isControlMessage(upToDateMsg)).toBe(true)
    expect(isControlMessage(mustRefetchMsg)).toBe(true)
    expect(isChangeMessage(upToDateMsg)).toBe(false)
    expect(isChangeMessage(mustRefetchMsg)).toBe(false)
  })

  it(`should correctly detect up-to-date message`, () => {
    expect(isUpToDateMessage(upToDateMsg)).toBe(true)
    expect(isUpToDateMessage(mustRefetchMsg)).toBe(false)
    expect(isUpToDateMessage(changeMsg)).toBe(false)
  })

  it(`should not classify EventMessages as ControlMessages`, () => {
    expect(isControlMessage(eventMsg)).toBe(false)
    expect(isChangeMessage(eventMsg)).toBe(false)
    expect(isUpToDateMessage(eventMsg)).toBe(false)
  })

  it(`should not classify messages without headers as ControlMessages`, () => {
    // Messages without headers can arrive from proxy/CDN interference
    // or unexpected server responses (e.g. 409 during initialization).
    const noHeadersMsg = {} as unknown as Message
    expect(isControlMessage(noHeadersMsg)).toBe(false)
    expect(isChangeMessage(noHeadersMsg)).toBe(false)
    expect(isUpToDateMessage(noHeadersMsg)).toBe(false)
  })

  describe(`bigintSafeStringify`, () => {
    it(`should serialize objects with BigInt values`, () => {
      const obj = { id: BigInt(`9223372036854775807`), name: `test` }
      expect(bigintSafeStringify(obj)).toBe(
        `{"id":"9223372036854775807","name":"test"}`
      )
    })

    it(`should handle nested BigInt values`, () => {
      const obj = { params: { '1': BigInt(42), '2': `hello` } }
      expect(bigintSafeStringify(obj)).toBe(`{"params":{"1":"42","2":"hello"}}`)
    })

    it(`should behave like JSON.stringify for non-BigInt values`, () => {
      const obj = { a: 1, b: `two`, c: true, d: null }
      expect(bigintSafeStringify(obj)).toBe(JSON.stringify(obj))
    })

    it(`should not throw for BigInt values where JSON.stringify would`, () => {
      const obj = { id: BigInt(123) }
      expect(() => JSON.stringify(obj)).toThrow(
        `Do not know how to serialize a BigInt`
      )
      expect(() => bigintSafeStringify(obj)).not.toThrow()
    })
  })

  describe(`compareLogOffsets`, () => {
    it(`orders by LSN, then by op`, () => {
      expect(compareLogOffsets(`1_5`, `2_0`)).toBe(-1)
      expect(compareLogOffsets(`2_0`, `1_5`)).toBe(1)
      expect(compareLogOffsets(`1_5`, `1_6`)).toBe(-1)
      expect(compareLogOffsets(`1_5`, `1_5`)).toBe(0)
    })

    it(`compares LSNs and ops beyond the safe integer range`, () => {
      expect(
        compareLogOffsets(`9007199254740992_0`, `9007199254740993_0`)
      ).toBe(-1)
      expect(
        compareLogOffsets(`1_9007199254740993`, `1_9007199254740992`)
      ).toBe(1)
    })

    it(`orders inf after finite ops at the same LSN only`, () => {
      expect(compareLogOffsets(`0_5`, `0_inf`)).toBe(-1)
      expect(compareLogOffsets(`0_inf`, `0_5`)).toBe(1)
      expect(compareLogOffsets(`0_inf`, `0_inf`)).toBe(0)
      expect(compareLogOffsets(`0_inf`, `1_0`)).toBe(-1)
    })

    it(`returns null when either offset is not a log position`, () => {
      expect(compareLogOffsets(`-1`, `1_0`)).toBeNull()
      expect(compareLogOffsets(`1_0`, `now`)).toBeNull()
      expect(compareLogOffsets(`bogus` as never, `1_0`)).toBeNull()
      expect(compareLogOffsets(`1_x` as never, `1_0`)).toBeNull()
    })
  })

  describe(`isOffsetAhead`, () => {
    it(`returns false when the candidate regresses within the same LSN`, () => {
      expect(isOffsetAhead(`1_0`, `1_285`)).toBe(false)
    })

    it(`returns true when the candidate advances the op at the same LSN`, () => {
      expect(isOffsetAhead(`1_290`, `1_285`)).toBe(true)
    })

    it(`returns true when the candidate advances the LSN`, () => {
      expect(isOffsetAhead(`2_0`, `1_285`)).toBe(true)
    })

    it(`returns false when the candidate lags on LSN despite higher op`, () => {
      expect(isOffsetAhead(`1_999`, `2_0`)).toBe(false)
    })

    it(`returns false for equal offsets`, () => {
      expect(isOffsetAhead(`1_285`, `1_285`)).toBe(false)
    })

    it(`compares LSNs beyond the safe integer range`, () => {
      expect(isOffsetAhead(`9007199254740993_0`, `9007199254740992_0`)).toBe(
        true
      )
      expect(isOffsetAhead(`9007199254740992_0`, `9007199254740993_0`)).toBe(
        false
      )
    })

    it(`treats -1 and now as superseded by any concrete offset`, () => {
      expect(isOffsetAhead(`1_0`, `-1`)).toBe(true)
      expect(isOffsetAhead(`1_0`, `now`)).toBe(true)
      expect(isOffsetAhead(`-1`, `1_285`)).toBe(false)
      expect(isOffsetAhead(`now`, `1_285`)).toBe(false)
    })

    it(`orders the inf operation boundary after every finite operation`, () => {
      expect(isOffsetAhead(`0_inf`, `0_5`)).toBe(true)
      expect(isOffsetAhead(`0_5`, `0_inf`)).toBe(false)
      expect(isOffsetAhead(`0_inf`, `0_inf`)).toBe(false)
      expect(isOffsetAhead(`5_0`, `0_inf`)).toBe(true)
      expect(isOffsetAhead(`0_inf`, `5_0`)).toBe(false)
      expect(isOffsetAhead(`5_inf`, `5_999`)).toBe(true)
    })

    it(`never moves on malformed offsets`, () => {
      expect(isOffsetAhead(`bogus` as never, `1_285`)).toBe(false)
      expect(isOffsetAhead(`1_0`, `bogus` as never)).toBe(false)
    })
  })

  describe(`toError`, () => {
    it(`returns an Error unchanged`, () => {
      const error = new TypeError(`boom`)
      expect(toError(error)).toBe(error)
    })

    it(`wraps a string and keeps it as the cause`, () => {
      const error = toError(`boom`)
      expect(error).toBeInstanceOf(Error)
      expect(error.message).toBe(`boom`)
      expect(error.cause).toBe(`boom`)
    })

    it(`wraps an object and keeps the same object as the cause`, () => {
      const reason = { code: `E_RESET` }
      expect(toError(reason).cause).toBe(reason)
    })

    it(`wraps undefined and null`, () => {
      expect(toError(undefined).cause).toBeUndefined()
      expect(toError(undefined).message).toBe(`undefined`)
      expect(toError(null).message).toBe(`null`)
    })

    it(`does not throw for a value that cannot be turned into a string`, () => {
      const reason = Object.create(null)
      const error = toError(reason)
      expect(error).toBeInstanceOf(Error)
      expect(error.cause).toBe(reason)
    })
  })

  it(`should handle null and undefined messages without throwing`, () => {
    // Null/undefined messages should not occur in normal operation, but
    // these guards protect against unexpected runtime values from
    // proxy/CDN interference or future code changes.
    expect(isChangeMessage(undefined as unknown as Message)).toBe(false)
    expect(isChangeMessage(null as unknown as Message)).toBe(false)
    expect(isControlMessage(undefined as unknown as Message)).toBe(false)
    expect(isControlMessage(null as unknown as Message)).toBe(false)
    expect(isUpToDateMessage(undefined as unknown as Message)).toBe(false)
    expect(isUpToDateMessage(null as unknown as Message)).toBe(false)
  })
})
