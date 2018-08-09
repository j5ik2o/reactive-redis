package com.github.j5ik2o.reactive.redis.feature

import java.util.UUID

import com.github.j5ik2o.reactive.redis._
import com.github.j5ik2o.reactive.redis.command.connection._

/**
  * https://redis.io/commands#connection
  */
trait ConnectionAPI[M[_]] {
  def auth(password: String): M[Unit]
  def echo(message: String): M[Result[String]]
  def ping(message: Option[String] = None): M[Result[String]]
  def quit(): M[Unit]
  def select(index: Int): M[Result[Unit]]
  def swapDB(index0: Int, index1: Int): M[Result[Unit]]
}

trait ConnectionFeature extends ConnectionAPI[ReaderTTaskRedisConnection] {
  this: RedisClient =>

  override def auth(password: String): ReaderTTaskRedisConnection[Unit] =
    send(AuthRequest(UUID.randomUUID(), password)).flatMap {
      case AuthSucceeded(_, _)  => ReaderTTask.pure(())
      case AuthFailed(_, _, ex) => ReaderTTask.raiseError(ex)
    }

  override def echo(message: String): ReaderTTaskRedisConnection[Result[String]] =
    send(EchoRequest(UUID.randomUUID(), message)).flatMap {
      case EchoSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case EchoSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case EchoFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  override def ping(message: Option[String] = None): ReaderTTaskRedisConnection[Result[String]] =
    send(PingRequest(UUID.randomUUID(), message)).flatMap {
      case PingSuspended(_, _)         => ReaderTTask.pure(Suspended)
      case PingSucceeded(_, _, result) => ReaderTTask.pure(Provided(result))
      case PingFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  override def quit(): ReaderTTaskRedisConnection[Unit] = send(QuitRequest(UUID.randomUUID())).flatMap {
    case QuitSucceeded(_, _)  => ReaderTTask.pure(())
    case QuitFailed(_, _, ex) => ReaderTTask.raiseError(ex)
  }

  override def select(index: Int): ReaderTTaskRedisConnection[Result[Unit]] =
    send(SelectRequest(UUID.randomUUID(), index)).flatMap {
      case SelectSuspended(_, _)  => ReaderTTask.pure(Suspended)
      case SelectSucceeded(_, _)  => ReaderTTask.pure(Provided(()))
      case SelectFailed(_, _, ex) => ReaderTTask.raiseError(ex)
    }

  override def swapDB(index0: Int, index1: Int): ReaderTTaskRedisConnection[Result[Unit]] =
    send(SwapDBRequest(UUID.randomUUID(), index0, index1)).flatMap {
      case SwapDBSuspended(_, _)  => ReaderTTask.pure(Suspended)
      case SwapDBSucceeded(_, _)  => ReaderTTask.pure(Provided(()))
      case SwapDBFailed(_, _, ex) => ReaderTTask.raiseError(ex)
    }
}
