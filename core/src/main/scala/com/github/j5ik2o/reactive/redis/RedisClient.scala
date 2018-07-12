package com.github.j5ik2o.reactive.redis

import akka.actor.ActorSystem
import com.github.j5ik2o.reactive.redis.command.CommandRequest
import monix.eval.Task

class RedisClient(connectionPoolConfig: ConnectionPoolConfig, connectionConfig: ConnectionConfig)(
    implicit system: ActorSystem
) {

  private val connectionPool = new RedisConnectionPool[Task](connectionPoolConfig, connectionConfig)

  import connectionPool._

  def sendCommandRequest[C <: CommandRequest](cmd: C): Task[cmd.Response] =
    withConnection[cmd.Response](_.send(cmd))

}
