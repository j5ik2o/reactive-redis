package com.github.j5ik2o.reactive.redis

import java.util.UUID

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{ Actor, OneForOneStrategy, Props, SupervisorStrategy }

object RedisSupervisor {

  def props(redisActorProps: Props, id: UUID): Props =
    Props(new RedisSupervisor(redisActorProps, id))

}

class RedisSupervisor(redisActorProps: Props, val id: UUID) extends Actor {

  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
    case ex: Exception =>
      ex.printStackTrace()
      Restart
  }

  private val child = context.actorOf(redisActorProps, name = RedisActor.name(UUID.randomUUID()))

  override def receive: Receive = {
    case request =>
      child forward request
  }

}
