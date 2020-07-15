import {
  Client,
  Send,
  Recv,
  State,
  ClientMsg,
  ServerMsg,
  TaskMap,
  ArgsOf,
} from "./types"
import { BehaviorSubject, Observable } from "rxjs"
import { filter } from "rxjs/operators"
import * as o from "rxjs/operators"
import { OperatorFunction } from "rxjs/internal/types"

function idGen() {
  return (
    Math.random()
      .toString(36)
      .substring(2) +
    Math.random()
      .toString(36)
      .substring(2)
  )
}

export function makeClient<TTaskMap extends TaskMap>(
  send: Send<ClientMsg<TTaskMap>>,
  recv: Recv<ServerMsg<TTaskMap>>,
): Client<TTaskMap> {
  const statusSubj = new BehaviorSubject<State | null>(null)
  recv(msg => {
    statusSubj.next(msg.status)
  })
  return {
    send: <K extends keyof TTaskMap & string>(
      name: K,
      ...args: ArgsOf<TTaskMap[K]>
    ) => {
      const id = idGen()
      send({ type: "send", name, args, id })
      return statusSubj.pipe(
        o.scan<
          State | null,
          "inProgress" | "waiting" | "errored" | "initial" | "done"
        >((lastState, state) => {
          if (state?.inProgress.findIndex(x => x.id === id) !== -1)
            return "inProgress"
          if (state?.waiting.findIndex(x => x.id === id) !== -1)
            return "waiting"
          if (state?.errored.findIndex(x => x.id === id) !== -1)
            return "errored"
          if (lastState !== "initial") return "done"
          return "initial"
        }, "initial"),
        o.takeWhile(x => x !== "done", true),
        o.map<
          "inProgress" | "waiting" | "initial" | "errored" | "done",
          "inProgress" | "waiting" | "errored" | "done"
        >(x => (x === "initial" ? "waiting" : x)),
      )
    },
    retry: id => send({ type: "retry", id }),
    cancel: id => send({ type: "cancel", id }),
    retryAll: () => send({ type: "retryAll" }),
    status: statusSubj.pipe(filter(x => x !== null)) as Observable<State>,
  }
}
