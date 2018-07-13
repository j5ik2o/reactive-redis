package com.github.j5ik2o.reactive.redis

import java.util.UUID

import akka.actor.ActorSystem
import cats.data.ReaderT
import com.github.j5ik2o.reactive.redis.command._
import monix.eval.Task

object RedisClient {

  def apply()(implicit system: ActorSystem): RedisClient = new RedisClient()

}

class RedisClient(implicit system: ActorSystem) {

  def send[C <: CommandRequest](cmd: C): ReaderTTaskRedisConnection[cmd.Response] = ReaderT(_.send(cmd))

  def append(key: String, value: String): ReaderT[Task, RedisConnection, Int] =
    send(AppendRequest(UUID.randomUUID(), key, value)).flatMap {
      case AppendSucceeded(_, _, result) => ReaderTTask.pure(result)
      case AppendFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

  def set(key: String, value: String): ReaderTTaskRedisConnection[Unit] =
    send(SetRequest(UUID.randomUUID(), key, value)).flatMap {
      case SetSucceeded(_, _)  => ReaderTTask.pure(())
      case SetFailed(_, _, ex) => ReaderTTask.raiseError(ex)
    }

  def get(key: String): ReaderTTaskRedisConnection[Option[String]] =
    send(GetRequest(UUID.randomUUID(), key)).flatMap {
      case GetSucceeded(_, _, value) => ReaderTTask.pure(value)
      case GetFailed(_, _, ex)       => ReaderTTask.raiseError(ex)
    }

  def getSet(key: String, value: String): ReaderTTaskRedisConnection[Option[String]] =
    send(GetSetRequest(UUID.randomUUID(), key, value)).flatMap {
      case GetSetSucceeded(_, _, result) => ReaderTTask.pure(result)
      case GetSetFailed(_, _, ex)        => ReaderTTask.raiseError(ex)
    }

}
