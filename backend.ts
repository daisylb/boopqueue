import {
  Send,
  Recv,
  Persistence,
  ServerMsg,
  ClientMsg,
  State,
  TaskMap,
  ArgsOf,
  JobInfo,
  Meta,
  ErroredJobInfo,
  Task,
  AnyTask,
  PartialStateOf,
  JsonValue,
} from "./types"
import * as t from "io-ts"
import { getOrElse, fold, Either } from "fp-ts/es6/Either"
import { emptyTuple } from "./codecs"

function hasattr<T, A extends string>(v: T, a: A): v is T & Record<A, unknown> {
  return a in v
}

const stateEntry = t.type({ id: t.string })
type StateEntry = t.TypeOf<typeof stateEntry>
const errorStateEntry = t.type({
  id: t.string,
  error: t.union([t.string, t.null]),
})
type ErrorStateEntry = t.TypeOf<typeof errorStateEntry>

const internalState = t.type(
  {
    waiting: t.array(stateEntry),
    inProgress: t.array(stateEntry),
    errored: t.array(errorStateEntry),
  },
  "internalState",
)
type InternalState = t.TypeOf<typeof internalState>
type _ = typeof internalState.decode

const defaultState: InternalState = {
  waiting: [],
  inProgress: [],
  errored: [],
}

async function load<T, TDefault>(
  persist: Persistence,
  key: string,
  codec: t.Type<T, any, any>,
  defaultValue: TDefault,
): Promise<T | TDefault>
async function load<T>(
  persist: Persistence,
  key: string,
  codec: t.Type<T, any, any>,
): Promise<T | undefined>
async function load<T, TDefault = undefined>(
  persist: Persistence,
  key: string,
  codec: t.Type<T, any, any>,
  defaultValue: TDefault | undefined = undefined,
): Promise<T | TDefault | undefined> {
  const s = await persist.get(key)
  if (s === undefined) return defaultValue
  return getOrElse<any, T | TDefault | undefined>(e => {
    console.warn(e)
    return defaultValue
  })(codec.decode(s))
}

const MAX_RUNNERS = 4

function cached<K extends Object, V>(
  cb: (k: K) => Promise<V>,
): ((k: K) => Promise<V>) & { remove: (k: K) => void } {
  const cache = new WeakMap<K, V>()
  return Object.assign(
    async function(k: K): Promise<V> {
      if (cache.has(k)) return cache.get(k) as V
      const v = await cb(k)
      cache.set(k, v)
      return v
    },
    {
      remove: (k: K) => {
        cache.delete(k)
      },
    },
  )
}

type TaskDetails<TTaskMap extends TaskMap> = {
  [K in keyof TTaskMap & string]: { type: K; args: ArgsOf<TTaskMap[K]> }
}[keyof TTaskMap & string]

function taskDetailC<TTask extends AnyTask>(name: string, task: TTask) {
  return t.type({
    type: t.literal(name),
    args: task.args ?? emptyTuple,
  })
}
type TaskDetailC = ReturnType<typeof taskDetailC>

function taskDetailsC(
  taskMap: TaskMap,
): t.Type<
  {
    type: string
    args: unknown[]
  },
  { type: string; args: JsonValue[] },
  unknown
> {
  const entries = Array.from(Object.entries(taskMap))
  if (entries.length === 0) {
    throw Error("There must be at least one task specified")
  }
  if (entries.length === 1) {
    return taskDetailC(...entries[0])
  }
  return t.union(
    entries.map(x => taskDetailC(...x)) as [
      TaskDetailC,
      TaskDetailC,
      ...TaskDetailC[],
    ],
  )
}

const STATE_KEY = "state.tv6"

export default async function runBackend<TTaskMap extends TaskMap>(p: {
  tasks: TTaskMap
  send: Send<ServerMsg<TTaskMap>>
  recv: Recv<ClientMsg<TTaskMap>>
  defer: (callback: () => void) => void
  persist: Persistence
}) {
  const taskDetails = taskDetailsC(p.tasks)

  // Load state from persistence backend
  var state: InternalState = await load(
    p.persist,
    STATE_KEY,
    internalState,
    defaultState,
  )
  var runnerCount = 0

  // If there was an in-progress job in the state we loaded, add it to the front
  // of the queue
  if (state.inProgress.length) {
    console.info("Restoring partially completed jobs", state.inProgress)
    state = {
      ...state,
      inProgress: [],
      waiting: [...state.inProgress, ...state.waiting],
    }
  }

  async function setState(newState: InternalState) {
    state = newState
    await p.persist.set(STATE_KEY, state)
    reportState()
  }

  const getJobInfo = cached(
    async (entry: StateEntry): Promise<JobInfo> => {
      const details = await load(p.persist, `details/${entry.id}`, taskDetails)
      if (details === undefined) return { id: entry.id, name: "<unknown>" }
      return {
        id: entry.id,
        name: p.tasks[details.type].name,
      }
    },
  )
  const getErroredJobInfo = cached(
    async (entry: ErrorStateEntry): Promise<ErroredJobInfo> => {
      return { ...(await getJobInfo(entry)), error: entry.error }
    },
  )
  const progressMap = new WeakMap<StateEntry, number>()

  async function reportState() {
    await p.send({
      type: "status",
      status: {
        waiting: await Promise.all(state.waiting.map(getJobInfo)),
        inProgress: await Promise.all(
          state.inProgress.map(x =>
            getJobInfo(x).then(y => ({
              ...y,
              progress: progressMap.get(x) ?? null,
            })),
          ),
        ),
        errored: await Promise.all(state.errored.map(getErroredJobInfo)),
      },
    })
  }

  async function startRunner<TArgs extends unknown[], TPartialState>(
    runnerNum: number,
  ) {
    // Update state
    const [current, ...waiting] = state.waiting
    if (!current) {
      console.debug("No more tasks to run")
      return
    }
    console.debug(runnerNum, "Running task", current)
    setState({ ...state, inProgress: [...state.inProgress, current], waiting })
    const detailsKey = `details/${current.id}`

    // load args
    const details = (await load(p.persist, detailsKey, taskDetails)) as
      | { type: string; args: TArgs }
      | undefined

    if (details === undefined) {
      console.warn(
        `Unrecoverable error loading task ${current.id}: details not found`,
      )
      await setState({
        ...state,
        inProgress: state.inProgress.filter(x => x.id !== current.id),
      })
      return
    }

    const task = (p.tasks[details.type] as unknown) as Task<
      TArgs,
      TPartialState
    >

    // If the task is one we've restored, and it has partial state, load it
    const partialStateC = hasattr(task, "partialState")
      ? task.partialState
      : undefined
    const partialState: TPartialState | undefined = partialStateC
      ? await load(p.persist, `partial/${current.id}`, partialStateC)
      : undefined

    // Create the meta object that provides our API to the task
    const meta: Meta<TPartialState> = {
      progress: progress => {
        if (progress < 0 || progress > 1) {
          console.warn("progress should be between 0 and 1, got", progress)
          return
        }
        progressMap.set(current, progress)
        reportState()
      },
      partialState,
      savePartial: async x => {
        if (!partialStateC) return
        await p.persist.set(`partial/${current.id}`, partialStateC.encode(x))
      },
    }

    // Actually invoke the task
    console.info("Starting task", current, "with args", details.args)
    try {
      await p.tasks[details.type].action(meta, ...details.args)
    } catch (error) {
      // handle error
      console.warn(error)
      const errorDesc = p.tasks[details.type].describeError?.(error) ?? null
      await setState({
        ...state,
        inProgress: state.inProgress.filter(x => x.id !== current.id),
        errored: [...state.errored, { ...current, error: errorDesc }],
      })
      return
    } finally {
      progressMap.delete(current)
    }

    // handle success
    console.info("Task succeeded", current)
    await setState({
      ...state,
      inProgress: state.inProgress.filter(x => x.id !== current.id),
    })
    p.persist.delete(detailsKey)
  }

  function startRunners() {
    if (!state.waiting.length) return
    while (runnerCount < MAX_RUNNERS && state.waiting.length) {
      const thisCount = runnerCount++
      p.defer(() =>
        startRunner(thisCount).finally(() => {
          runnerCount--
          startRunners()
        }),
      )
    }
  }

  p.recv(async message => {
    switch (message.type) {
      case "send":
        await p.persist.set(
          `details/${message.id}`,
          taskDetails.encode({
            type: message.name,
            args: message.args,
          }),
        )
        await setState({
          ...state,
          waiting: [...state.waiting, { id: message.id }],
        })
        break
      case "retry":
        const retryable = state.errored.find(x => x.id === message.id)
        if (!retryable) return
        await setState({
          ...state,
          errored: state.errored.filter(x => x.id !== message.id),
          waiting: [...state.waiting, { id: retryable.id }],
        })
        break
      case "retryAll":
        await setState({
          ...state,
          errored: [],
          waiting: [
            ...state.waiting,
            ...state.errored.map(x => ({ id: x.id })),
          ],
        })
        break
      case "cancel":
        await setState({
          ...state,
          errored: state.errored.filter(x => x.id !== message.id),
          waiting: state.waiting.filter(x => x.id !== message.id),
        })
        await p.persist.delete(`details/${message.id}`)
        break
    }
    startRunners()
  })
  reportState()
  startRunners()
}
