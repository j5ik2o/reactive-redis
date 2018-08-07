package com.github.j5ik2o.reactive.redis.feature

import java.util.UUID

import com.github.j5ik2o.reactive.redis.command.CommandResponse
import com.github.j5ik2o.reactive.redis.command.transactions._
import com.github.j5ik2o.reactive.redis._

/**
  * https://redis.io/commands#transactions
  */
trait TransactionsAPI[M[_]] {
  def discard(): M[Unit]
  def exec(): M[Seq[CommandResponse]]
  def multi(): M[Unit]
  def unwatch(): M[Unit]
  def watch(keys: Set[String]): M[Result[Unit]]
}

trait TransactionsFeature extends TransactionsAPI[ReaderTTaskRedisConnection] {
  this: RedisClient =>

  override def discard(): ReaderTTaskRedisConnection[Unit] = send(DiscardRequest(UUID.randomUUID())).flatMap {
    case DiscardSucceeded(_, _)  => ReaderTTask.pure(())
    case DiscardFailed(_, _, ex) => ReaderTTask.raiseError(ex)
  }

  override def exec(): ReaderTTaskRedisConnection[Seq[CommandResponse]] = send(ExecRequest(UUID.randomUUID())).flatMap {
    case ExecSucceeded(_, _, results) => ReaderTTask.pure(results)
    case ExecFailed(_, _, ex)         => ReaderTTask.raiseError(ex)
  }

  override def multi(): ReaderTTaskRedisConnection[Unit] = send(MultiRequest(UUID.randomUUID())).flatMap {
    case MultiSucceeded(_, _)  => ReaderTTask.pure(())
    case MultiFailed(_, _, ex) => ReaderTTask.raiseError(ex)
  }

  override def unwatch(): ReaderTTaskRedisConnection[Unit] = send(UnwatchRequest(UUID.randomUUID())).flatMap {
    case UnwatchSucceeded(_, _)  => ReaderTTask.pure(())
    case UnwatchFailed(_, _, ex) => ReaderTTask.raiseError(ex)
  }

  override def watch(keys: Set[String]): ReaderTTaskRedisConnection[Result[Unit]] =
    send(WatchRequest(UUID.randomUUID(), keys)).flatMap {
      case WatchSuspended(_, _)  => ReaderTTask.pure(Suspended)
      case WatchSucceeded(_, _)  => ReaderTTask.pure(Provided(()))
      case WatchFailed(_, _, ex) => ReaderTTask.raiseError(ex)
    }

}
