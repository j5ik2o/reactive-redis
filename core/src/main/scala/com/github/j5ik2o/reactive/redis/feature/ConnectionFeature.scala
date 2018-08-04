package com.github.j5ik2o.reactive.redis.feature

import java.util.UUID

import com.github.j5ik2o.reactive.redis._
import com.github.j5ik2o.reactive.redis.command.connection._

/**
  * https://redis.io/commands#connection
  */
trait ConnectionFeature {
  this: RedisClient =>

  def auth(password: String): ReaderTTaskRedisConnection[Unit] =
    send(AuthRequest(UUID.randomUUID(), password)).flatMap {
      case AuthSucceeded(_, _)  => ReaderTTask.pure(())
      case AuthFailed(_, _, ex) => ReaderTTask.raiseError(ex)
    }

  def echo(message: String): ReaderTTaskRedisConnection[Result[String]] =
    send(EchoRequest(UUID.randomUUID(), message)).flatMap {
      case EchoSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case EchoSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case EchoFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  def ping(message: Option[String] = None): ReaderTTaskRedisConnection[Result[String]] =
    send(PingRequest(UUID.randomUUID(), message)).flatMap {
      case PingSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case PingSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case PingFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  def quit(): ReaderTTaskRedisConnection[Unit] = send(QuitRequest(UUID.randomUUID())).flatMap {
    case QuitSucceeded(_, _)  => ReaderTTask.pure(())
    case QuitFailed(_, _, ex) => ReaderTTask.raiseError(ex)
  }

  /**
  * SELECT
  * SWAPDB
  */

}
