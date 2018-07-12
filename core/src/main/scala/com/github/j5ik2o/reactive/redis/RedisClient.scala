package com.github.j5ik2o.reactive.redis

import java.util.UUID

import akka.actor.ActorSystem
import cats.data.ReaderT
import com.github.j5ik2o.reactive.redis.RedisClient.ReaderTTask
import com.github.j5ik2o.reactive.redis.command._
import monix.eval.Task

object RedisClient {

  type ReaderTTask[A] = ReaderT[Task, RedisConnection, A]

  def apply()(implicit system: ActorSystem): RedisClient = new RedisClient()

}

class RedisClient(
    implicit system: ActorSystem
) {

  def send[C <: CommandRequest](cmd: C): ReaderTTask[cmd.Response] = ReaderT(_.send(cmd))

  def set(key: String, value: String): ReaderTTask[Unit] =
    send(SetCommandRequest(UUID.randomUUID(), key, value)).flatMap {
      case SetSucceeded(_, _) =>
        ReaderT { _ =>
          Task.pure(value)
        }
      case SetFailed(_, _, ex) =>
        ReaderT { _ =>
          Task.raiseError(ex)
        }
    }

  def get(key: String): ReaderTTask[Option[String]] = send(GetCommandRequest(UUID.randomUUID(), key)).flatMap {
    case GetSucceeded(_, _, value) =>
      ReaderT { _ =>
        Task.pure(value)
      }
    case GetFailed(_, _, ex) =>
      ReaderT { _ =>
        Task.raiseError(ex)
      }
  }

}
