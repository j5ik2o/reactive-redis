package com.github.j5ik2o.reactive

import cats.data.ReaderT
import monix.eval.Task

package object redis {
  type ReaderTTask[C, A]             = ReaderT[Task, C, A]
  type ReaderTTaskRedisConnection[A] = ReaderTTask[RedisConnection, A]
}
