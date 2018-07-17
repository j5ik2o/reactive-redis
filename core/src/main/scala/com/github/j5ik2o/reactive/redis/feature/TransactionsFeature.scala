package com.github.j5ik2o.reactive.redis.feature

import java.util.UUID

import com.github.j5ik2o.reactive.redis.command.CommandResponse
import com.github.j5ik2o.reactive.redis.command.transactions._
import com.github.j5ik2o.reactive.redis.{ ReaderTTask, ReaderTTaskRedisConnection, RedisClient }

/**
  * https://redis.io/commands#transactions
  */
trait TransactionsFeature {
  this: RedisClient =>

  def discard(): ReaderTTaskRedisConnection[Unit] = send(DiscardRequest(UUID.randomUUID())).flatMap {
    case DiscardSucceeded(_, _)  => ReaderTTask.pure(())
    case DiscardFailed(_, _, ex) => ReaderTTask.raiseError(ex)
  }

  def exec(): ReaderTTaskRedisConnection[Seq[CommandResponse]] = send(ExecRequest(UUID.randomUUID())).flatMap {
    case ExecSucceeded(_, _, results) => ReaderTTask.pure(results)
    case ExecFailed(_, _, ex)         => ReaderTTask.raiseError(ex)
  }

  def multi(): ReaderTTaskRedisConnection[Unit] = send(MultiRequest(UUID.randomUUID())).flatMap {
    case MultiSucceeded(_, _)  => ReaderTTask.pure(())
    case MultiFailed(_, _, ex) => ReaderTTask.raiseError(ex)
  }

  def unwatch(): ReaderTTaskRedisConnection[Unit] = send(UnwatchRequest(UUID.randomUUID())).flatMap {
    case UnwatchSucceeded(_, _)  => ReaderTTask.pure(())
    case UnwatchFailed(_, _, ex) => ReaderTTask.raiseError(ex)
  }

  def watch(keys: Set[String]): ReaderTTaskRedisConnection[Unit] = send(WatchRequest(UUID.randomUUID(), keys)).flatMap {
    case WatchSucceeded(_, _)  => ReaderTTask.pure(())
    case WatchFailed(_, _, ex) => ReaderTTask.raiseError(ex)
  }

}
