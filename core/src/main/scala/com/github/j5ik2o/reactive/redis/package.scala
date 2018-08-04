package com.github.j5ik2o.reactive

import akka.stream.Supervision
import cats.data.ReaderT
import com.github.j5ik2o.reactive.redis.RedisConnection.EventHandler
import monix.eval.Task

package object redis {
  type NewRedisConnection             = (PeerConfig, Option[Supervision.Decider], Seq[EventHandler]) => RedisConnection
  type ReaderTTask[C, A]              = ReaderT[Task, C, A]
  type ReaderTTaskRedisConnection[A]  = ReaderTTask[RedisConnection, A]
  type ReaderRedisConnection[M[_], A] = ReaderT[M, RedisConnection, A]
}
