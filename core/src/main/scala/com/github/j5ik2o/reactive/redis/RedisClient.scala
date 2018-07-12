package com.github.j5ik2o.reactive.redis

import java.util.UUID

import akka.actor.ActorSystem
import cats.data.ReaderT
import com.github.j5ik2o.reactive.redis.command._

object RedisClient {

  def apply()(implicit system: ActorSystem): RedisClient = new RedisClient()

}

class RedisClient(implicit system: ActorSystem) {

  def send[C <: CommandRequest](cmd: C): ReaderTTaskRedisConnection[cmd.Response] = ReaderT(_.send(cmd))

  def set(key: String, value: String): ReaderTTaskRedisConnection[Unit] =
    send(SetCommandRequest(UUID.randomUUID(), key, value)).flatMap {
      case SetSucceeded(_, _)  => ReaderTTask.pure(())
      case SetFailed(_, _, ex) => ReaderTTask.raiseError(ex)
    }

  def get(key: String): ReaderTTaskRedisConnection[Option[String]] =
    send(GetCommandRequest(UUID.randomUUID(), key)).flatMap {
      case GetSucceeded(_, _, value) => ReaderTTask.pure(value)
      case GetFailed(_, _, ex)       => ReaderTTask.raiseError(ex)
    }

}
