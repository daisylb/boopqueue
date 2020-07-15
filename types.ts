import { Observable } from "rxjs"
import * as t from "io-ts"
import { IsNever, IsExact } from "conditional-type-checks"
import { AssertFalse, AssertTrue } from "conditional-type-checks"

export type Send<T> = (data: T) => void
export type Recv<T> = (callback: (data: T) => void) => void

export type Persistence = {
  set: (key: string, data: JsonValue) => void
  get: (key: string) => Promise<JsonValue | undefined>
  delete: (key: string) => Promise<void>
}

export type JobInfo = { name: string; id: string }
export type InProgressJobInfo = JobInfo & { progress: number | null }
export type ErroredJobInfo = JobInfo & { error: string | null }

export type State = {
  readonly waiting: readonly JobInfo[]
  readonly inProgress: InProgressJobInfo[]
  readonly errored: readonly ErroredJobInfo[]
}

export type ClientMsg<TTaskMap extends TaskMap> =
  | {
      [K in keyof TTaskMap & string]: {
        type: "send"
        name: K & string
        args: ArgsOf<TTaskMap[K & string]>
        id: string
      }
    }[keyof TTaskMap & string]
  | { type: "retry"; id: string }
  | { type: "cancel"; id: string }
  | { type: "retryAll" }

export type ServerMsg<TTaskMap extends TaskMap> = {
  type: "status"
  status: State
}

export type Meta<TPartialState = unknown> = {
  progress(progress: number): void
  partialState: TPartialState | undefined
  savePartial(state: TPartialState): Promise<void>
}

type AnyArrayC = t.ArrayC<t.Mixed>

export type JsonValue =
  | { readonly [k: string]: JsonValue }
  | readonly JsonValue[]
  | string
  | number
  | boolean
  | null

type Optional<TObject extends object> = { [K in keyof TObject]?: TObject[K] }

type ConditionallyOptional<
  Condition extends boolean,
  TObject extends object
> = Condition extends false ? TObject : Optional<TObject>

type _TaskAll<TArgs extends readonly unknown[], TPartialState> = {
  readonly name: string
  readonly describeError?: (error: unknown) => string | undefined
  readonly action: (meta: Meta<TPartialState>, ...args: TArgs) => Promise<void>
}

type _TaskArgs<TArgs extends readonly unknown[], TPartialState> = {
  readonly args: t.Type<TArgs, JsonValue[], unknown>
}

type _TaskPartialState<TArgs extends readonly unknown[], TPartialState> = {
  readonly partialState: t.Type<TPartialState, JsonValue, unknown>
}

export type Task<
  TArgs extends readonly unknown[] = [],
  TPartialState = undefined
> = _TaskAll<TArgs, TPartialState> &
  ConditionallyOptional<IsExact<TArgs, []>, _TaskArgs<TArgs, TPartialState>> &
  ConditionallyOptional<
    IsExact<TPartialState, undefined>,
    _TaskPartialState<TArgs, TPartialState>
  >

export type AnyTask = _TaskAll<any[], any> &
  Optional<_TaskArgs<any[], any> & _TaskPartialState<any[], any>>

type _AnyTaskTest<T extends AnyTask> = true
type _ATT1 = _AnyTaskTest<Task<[string], undefined>>
type _ATT2 = _AnyTaskTest<Task<[], undefined>>
type _ATT3 = _AnyTaskTest<Task<[string], string>>
type _ATT4 = _AnyTaskTest<Task<[], string>>

export type ArgsOf<T extends AnyTask> = T extends {
  action: (meta: any, ...args: infer U) => any
}
  ? U
  : any[]

export type PartialStateOf<T extends AnyTask> = T extends
  | Task<[], infer U>
  | Task<any, infer U>
  ? U
  : unknown

export type TaskMap = {
  readonly [id: string]: AnyTask
}

export type Client<TTaskMap extends TaskMap> = {
  send<K extends keyof TTaskMap & string>(
    name: K,
    ...args: ArgsOf<TTaskMap[K]>
  ): Observable<"inProgress" | "waiting" | "errored" | "done">
  retry(id: string): void
  cancel(id: string): void
  retryAll(): void
  status: Observable<State>
}
