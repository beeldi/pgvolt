import { describe, expect, it } from 'vitest'
import { MessageParser, defaultParser, pgArrayParser } from '../src/parser'

describe(`Default parser`, () => {
  it(`should parse integers`, () => {
    expect(defaultParser.int2(`0`)).toBe(0)
    expect(defaultParser.int2(`-32768`)).toBe(-32768)
    expect(defaultParser.int2(`32767`)).toBe(32767)

    expect(defaultParser.int4(`0`)).toBe(0)
    expect(defaultParser.int4(`2147483647`)).toBe(2147483647)
    expect(defaultParser.int4(`-2147483648`)).toBe(-2147483648)
  })

  it(`should parse bigints`, () => {
    expect(defaultParser.int8(`-9223372036854775808`)).toBe(
      BigInt(`-9223372036854775808`)
    )
    expect(defaultParser.int8(`9223372036854775807`)).toBe(
      BigInt(`9223372036854775807`)
    )
    expect(defaultParser.int8(`0`)).toBe(BigInt(`0`))
  })

  it(`should parse booleans`, () => {
    expect(defaultParser.bool(`t`)).toBe(true)
    expect(defaultParser.bool(`true`)).toBe(true)
    expect(defaultParser.bool(`false`)).toBe(false)
  })

  it(`should parse float4`, () => {
    expect(defaultParser.float4(`1.1754944e-38`)).toBe(1.1754944e-38)
    expect(defaultParser.float4(`3.4028235e38`)).toBe(3.4028235e38)
    expect(defaultParser.float4(`-3.4028235e38`)).toBe(-3.4028235e38)
    expect(defaultParser.float4(`-1.1754944e-38`)).toBe(-1.1754944e-38)
    expect(defaultParser.float4(`0`)).toBe(0)
    expect(defaultParser.float4(`Infinity`)).toBe(Infinity)
    expect(defaultParser.float4(`-Infinity`)).toBe(-Infinity)
    expect(defaultParser.float4(`NaN`)).toBe(NaN)
  })

  it(`should parse float8`, () => {
    expect(defaultParser.float8(`1.797e308`)).toBe(1.797e308)
    expect(defaultParser.float8(`-1.797e+308`)).toBe(-1.797e308)
    expect(defaultParser.float8(`0`)).toBe(0)
    expect(defaultParser.float8(`Infinity`)).toBe(Infinity)
    expect(defaultParser.float8(`-Infinity`)).toBe(-Infinity)
    expect(defaultParser.float8(`NaN`)).toBe(NaN)
  })

  it(`should parse json`, () => {
    expect(defaultParser.json(`true`)).toEqual(true)
    expect(defaultParser.json(`5`)).toEqual(5)
    expect(defaultParser.json(`"foo"`)).toEqual(`foo`)
    expect(defaultParser.json(`{}`)).toEqual({})
    expect(defaultParser.json(`null`)).toEqual(null)
    expect(defaultParser.json(`{"a":null}`)).toEqual({ a: null })
    expect(defaultParser.json(`[]`)).toEqual([])
    expect(defaultParser.json(`{"a":1}`)).toEqual({ a: 1 })
    expect(defaultParser.json(`{"a":1,"b":2}`)).toEqual({ a: 1, b: 2 })
    expect(defaultParser.json(`[{"a":1,"b":2},{"c": [{"d": 5}]}]`)).toEqual([
      { a: 1, b: 2 },
      { c: [{ d: 5 }] },
    ])

    expect(defaultParser.jsonb(`true`)).toEqual(true)
    expect(defaultParser.jsonb(`5`)).toEqual(5)
    expect(defaultParser.jsonb(`"foo"`)).toEqual(`foo`)
    expect(defaultParser.jsonb(`{}`)).toEqual({})
    expect(defaultParser.jsonb(`null`)).toEqual(null)
    expect(defaultParser.json(`{"a":null}`)).toEqual({ a: null })
    expect(defaultParser.jsonb(`[]`)).toEqual([])
    expect(defaultParser.jsonb(`{"a":1}`)).toEqual({ a: 1 })
    expect(defaultParser.jsonb(`{"a":1,"b":2}`)).toEqual({ a: 1, b: 2 })
    expect(defaultParser.jsonb(`[{"a":1,"b":2},{"c": [{"d": 5}]}]`)).toEqual([
      { a: 1, b: 2 },
      { c: [{ d: 5 }] },
    ])
  })

  it(`should preserve json/jsonb integers beyond 2^53 as exact bigints`, () => {
    expect(defaultParser.json(`9007199254740993`)).toEqual(
      BigInt(`9007199254740993`)
    )
    expect(defaultParser.jsonb(`9007199254740993`)).toEqual(
      BigInt(`9007199254740993`)
    )
    expect(defaultParser.jsonb(`-9007199254740993`)).toEqual(
      BigInt(`-9007199254740993`)
    )
    expect(defaultParser.jsonb(`{"n":9007199254740993}`)).toEqual({
      n: BigInt(`9007199254740993`),
    })
    expect(defaultParser.jsonb(`[9007199254740995]`)).toEqual([
      BigInt(`9007199254740995`),
    ])
    expect(defaultParser.jsonb(`{"a":{"b":9007199254740993}}`)).toEqual({
      a: { b: BigInt(`9007199254740993`) },
    })
  })

  it(`should parse a json document nested deeper than the call stack allows a reviver`, () => {
    const depth = 10_000
    const nested = (inner: string) =>
      `[`.repeat(depth) + inner + `]`.repeat(depth)
    const innermost = (value: unknown) => {
      let node = value
      for (let level = 0; level < depth; level++) node = (node as unknown[])[0]
      return node
    }

    expect(innermost(defaultParser.jsonb(nested(`1`)))).toEqual(1)
    // Too deep for the reviver, so the integer is a double, as JSON.parse reads it.
    expect(innermost(defaultParser.jsonb(nested(`9007199254740993`)))).toEqual(
      Number(`9007199254740993`)
    )
  })

  it(`should leave safe integers and floats as numbers`, () => {
    expect(typeof defaultParser.jsonb(`9007199254740991`)).toBe(`number`)
    expect(defaultParser.jsonb(`9007199254740991`)).toEqual(9007199254740991)
    expect(typeof defaultParser.jsonb(`1e308`)).toBe(`number`)
    expect(defaultParser.jsonb(`1e308`)).toEqual(1e308)
    expect(defaultParser.jsonb(`3.14`)).toEqual(3.14)
    expect(defaultParser.jsonb(`{"a":1,"b":2}`)).toEqual({ a: 1, b: 2 })
  })
})

describe(`Postgres array parser`, () => {
  const nullableDefaultParser = Object.fromEntries(
    Object.entries(defaultParser).map(([key, parser]) => [
      key,
      (val: string | null) => (val === null ? null : parser(val)),
    ])
  )

  it(`should parse arrays and their values`, () => {
    expect(
      pgArrayParser(`{1,2,3,4,5,NULL}`, nullableDefaultParser.int2)
    ).toEqual([1, 2, 3, 4, 5, null])
    expect(
      pgArrayParser(`{1,2,3,4,5,NULL}`, nullableDefaultParser.int8)
    ).toEqual([BigInt(1), BigInt(2), BigInt(3), BigInt(4), BigInt(5), null])
    expect(pgArrayParser(`{"foo","bar","NULL",NULL}`, (v) => v)).toEqual([
      `foo`,
      `bar`,
      `NULL`,
      null,
    ])
    expect(pgArrayParser(`{foo,"}"}`, (v) => v)).toEqual([`foo`, `}`])
    expect(pgArrayParser(`{t,f,f,NULL}`, nullableDefaultParser.bool)).toEqual([
      true,
      false,
      false,
      null,
    ])

    expect(pgArrayParser(`{}`, nullableDefaultParser.json)).toEqual([])
    expect(pgArrayParser(`{"{}"}`, nullableDefaultParser.json)).toEqual([{}])
    expect(pgArrayParser(`{null}`, nullableDefaultParser.json)).toEqual([null])

    expect(
      pgArrayParser(`{"{\\"a\\":null}"}`, nullableDefaultParser.json)
    ).toEqual([{ a: null }])

    expect(
      pgArrayParser(
        `{Infinity,-Infinity,NaN,NULL}`,
        nullableDefaultParser.float8
      )
    ).toEqual([Infinity, -Infinity, NaN, null])
  })

  it(`should parse an array of the 6 dimensions Postgres allows`, () => {
    expect(pgArrayParser(`{{{{{{1}}}}}}`, nullableDefaultParser.int2)).toEqual([
      [[[[[1]]]]],
    ])
  })

  it(`should refuse an array nested deeper than Postgres allows`, () => {
    expect(() =>
      pgArrayParser(`{{{{{{{1}}}}}}}`, nullableDefaultParser.int2)
    ).toThrow(/nested deeper than the 6 dimensions/)
  })

  it(`should refuse a very deep array with an error, not a stack overflow`, () => {
    const depth = 1_000_000
    expect(() =>
      pgArrayParser(`{`.repeat(depth) + `}`.repeat(depth), (v) => v)
    ).toThrow(/nested deeper than the 6 dimensions/)
  })

  it(`should not count braces inside quoted elements as nesting`, () => {
    expect(pgArrayParser(`{"{{{{{{{{x}}}}}}}}"}`, (v) => v)).toEqual([
      `{{{{{{{{x}}}}}}}}`,
    ])
  })

  it(`should parse nested arrays`, () => {
    expect(pgArrayParser(`{{1,2},{3,4}}`, nullableDefaultParser.int2)).toEqual([
      [1, 2],
      [3, 4],
    ])
    expect(pgArrayParser(`{{"foo"},{"bar"}}`, (v) => v)).toEqual([
      [`foo`],
      [`bar`],
    ])
    expect(pgArrayParser(`{{t,f}, {f,t}}`, nullableDefaultParser.bool)).toEqual(
      [
        [true, false],
        [false, true],
      ]
    )
    expect(pgArrayParser(`{{1,2},{3,4}}`, nullableDefaultParser.int8)).toEqual([
      [BigInt(1), BigInt(2)],
      [BigInt(3), BigInt(4)],
    ])

    expect(pgArrayParser(`{{},{}}`, nullableDefaultParser.json)).toEqual([
      [],
      [],
    ])
    expect(pgArrayParser(`{"{}","{}"}`, nullableDefaultParser.json)).toEqual([
      {},
      {},
    ])
    expect(pgArrayParser(`{null,null}`, nullableDefaultParser.json)).toEqual([
      null,
      null,
    ])
    expect(
      pgArrayParser(
        `{"{\\\"a\\\":null}", "{\\\"b\\\":null}"}`,
        nullableDefaultParser.json
      )
    ).toEqual([{ a: null }, { b: null }])

    expect(
      pgArrayParser(
        `{{Infinity,-Infinity,NaN},{NaN,Infinity,-Infinity}}`,
        nullableDefaultParser.float8
      )
    ).toEqual([
      [Infinity, -Infinity, NaN],
      [NaN, Infinity, -Infinity],
    ])
  })

  it(`should not append the array length for unterminated array text`, () => {
    // A dimension with no closing brace leaves `last < i` true at the end of
    // the loop. The tail must push the trailing value once, not push the return
    // value of an inner push (its new length) as a bogus extra element.
    expect(pgArrayParser(`{1,2`, (v) => v)).toEqual([`1`, `2`])
    expect(pgArrayParser(`{1,2,3`, (v) => v)).toEqual([`1`, `2`, `3`])
    expect(pgArrayParser(`{a`, (v) => v)).toEqual([`a`])
  })

  it(`should reject an explicit non-default lower bound instead of silently reindexing`, () => {
    expect(() =>
      pgArrayParser(`[-2:0]={1,2,3}`, nullableDefaultParser.int2)
    ).toThrowError(
      `Postgres array literal "[-2:0]={1,2,3}" has an explicit lower bound, which cannot be represented as a zero-indexed array`
    )

    expect(pgArrayParser(`{1,2,3}`, nullableDefaultParser.int2)).toEqual([
      1, 2, 3,
    ])
  })
})

describe(`Message parser`, () => {
  const parser = new MessageParser()

  it(`should preserve jsonb integers beyond 2^53 through the message pipeline`, () => {
    const messages = `[ { "value": { "meta": "{\\"id\\":9007199254740993}" } } ]`
    expect(parser.parse(messages, { meta: { type: `jsonb` } })).toEqual([
      { value: { meta: { id: BigInt(`9007199254740993`) } } },
    ])
  })

  it(`should parse null values`, () => {
    const messages = `[ { "value": { "a": null } }, { "value": { "value": null } } ]`
    const expectedParsedMessages = [
      { value: { a: null } },
      { value: { value: null } },
    ]

    const schema = (tpe: string, dims: number | undefined) => ({
      a: { type: tpe, dims },
      value: { type: tpe, dims },
    })

    const sampleDims = [undefined, 1, 2]

    for (const dims of sampleDims) {
      // If it's not nullable it should throw
      expect(() =>
        parser.parse(messages, { a: { type: `int2`, dims, not_null: true } })
      ).toThrowError(`Column "a" does not allow NULL values`)

      expect(() =>
        parser.parse(messages, {
          value: { type: `int2`, dims, not_null: true },
        })
      ).toThrowError(`Column "value" does not allow NULL value`)

      // Otherwise, it should parse as null
      expect(parser.parse(messages, schema(`int2`, dims))).toEqual(
        expectedParsedMessages
      )
      expect(parser.parse(messages, schema(`int4`, dims))).toEqual(
        expectedParsedMessages
      )
      expect(parser.parse(messages, schema(`int8`, dims))).toEqual(
        expectedParsedMessages
      )
      expect(parser.parse(messages, schema(`bool`, dims))).toEqual(
        expectedParsedMessages
      )
      expect(parser.parse(messages, schema(`float4`, dims))).toEqual(
        expectedParsedMessages
      )
      expect(parser.parse(messages, schema(`float8`, dims))).toEqual(
        expectedParsedMessages
      )
      expect(parser.parse(messages, schema(`json`, dims))).toEqual(
        expectedParsedMessages
      )
      expect(parser.parse(messages, schema(`jsonb`, dims))).toEqual(
        expectedParsedMessages
      )
      expect(parser.parse(messages, schema(`text`, dims))).toEqual(
        expectedParsedMessages
      )
    }
  })

  it(`should parse text values like "NULL" correctly`, () => {
    expect(
      parser.parse(`[ { "value": { "a": "NULL" } } ]`, {
        a: { type: `text`, not_null: true },
      })
    ).toEqual([{ value: { a: `NULL` } }])

    const messagesNullStringAndReal = `[ { "value": { "a": "{\\"a\\",\\"NULL\\",NULL,\\"b\\"}" } } ]`

    expect(
      parser.parse(messagesNullStringAndReal, {
        a: { type: `text`, dims: 1 },
      })
    ).toEqual([{ value: { a: [`a`, `NULL`, null, `b`] } }])

    // A NOT NULL array column constrains the array value, not its elements: a
    // NULL element is valid and must not throw.
    expect(
      parser.parse(messagesNullStringAndReal, {
        a: { type: `text`, dims: 1, not_null: true },
      })
    ).toEqual([{ value: { a: [`a`, `NULL`, null, `b`] } }])
  })

  it(`should keep NOT NULL array constraints on the array value, not its elements`, () => {
    // A NULL element under a NOT NULL array parses to null.
    expect(
      parser.parse(`[ { "value": { "a": "{1,NULL,3}" } } ]`, {
        a: { type: `int2`, dims: 1, not_null: true },
      })
    ).toEqual([{ value: { a: [1, null, 3] } }])

    // The array value itself being NULL under NOT NULL still throws.
    expect(() =>
      parser.parse(`[ { "value": { "a": null } } ]`, {
        a: { type: `int2`, dims: 1, not_null: true },
      })
    ).toThrowError(`Column "a" does not allow NULL values`)
  })

  it(`should reject an array literal arriving under a scalar schema entry`, () => {
    for (const type of [`int2`, `int4`, `int8`, `float4`, `float8`, `bool`]) {
      expect(() =>
        parser.parse(`[ { "value": { "a": "{1,2,3}" } } ]`, {
          a: { type },
        })
      ).toThrowError(
        `Column "a" is typed as scalar "${type}" but received an array literal`
      )
    }

    // The explicit-lower-bound serialization is also an array literal.
    expect(() =>
      parser.parse(`[ { "value": { "a": "[0:2]={1,2,3}" } } ]`, {
        a: { type: `int4` },
      })
    ).toThrowError(
      `Column "a" is typed as scalar "int4" but received an array literal`
    )

    // A text scalar legitimately holds a value that starts with a brace.
    expect(
      parser.parse(`[ { "value": { "a": "{not an array}" } } ]`, {
        a: { type: `text` },
      })
    ).toEqual([{ value: { a: `{not an array}` } }])
  })

  it(`should parse arrays including null values`, () => {
    const schema = {
      a: { type: `int2`, dims: 1 },
    }

    expect(
      parser.parse(`[ { "value": { "a": "{1,2,NULL,4,5}" } } ]`, schema)
    ).toEqual([{ value: { a: [1, 2, null, 4, 5] } }])
  })

  it(`should reject an array value carrying an explicit lower bound`, () => {
    expect(() =>
      parser.parse(`[ { "value": { "a": "[-2:0]={1,2,3}" } } ]`, {
        a: { type: `int2`, dims: 1 },
      })
    ).toThrowError(
      `Postgres array literal "[-2:0]={1,2,3}" has an explicit lower bound, which cannot be represented as a zero-indexed array`
    )

    expect(
      parser.parse(`[ { "value": { "a": "{1,2,3}" } } ]`, {
        a: { type: `int2`, dims: 1 },
      })
    ).toEqual([{ value: { a: [1, 2, 3] } }])
  })
})
