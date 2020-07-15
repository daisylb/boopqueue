import * as t from "io-ts"
import { left, right } from "fp-ts/es6/Either"

export const emptyTuple = new t.Type<[]>(
  "emptyTuple",
  (x): x is [] => Array.isArray(x) && x.length === 0,
  (value, context) =>
    Array.isArray(value) && value.length === 0
      ? right([])
      : left([{ value, context }]),
  x => x,
)
