import { Send, Recv } from "./types"

export default function<T>(): [Send<T>, Recv<T>] {
  const q: T[] = []
  var handler_ = (v: T) => {
    q.push(v)
  }
  var handlerSet = false
  const send = (v: T) => handler_(v)
  const recv = (handler: (v: T) => void) => {
    if (handlerSet) throw new Error("Receive handler set more than once")
    handler_ = handler
    for (const v of q) handler(v)
    q.length = 0
    handlerSet = true
  }
  return [send, recv]
}
