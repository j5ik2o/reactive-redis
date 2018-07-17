package com.github.j5ik2o.reactive.redis.pool

import java.util.concurrent.atomic.AtomicLong

import akka.actor.{ Actor, ActorLogging, ActorRef, Props }
import akka.routing._

object RedisConnectionPoolActor {

  def props(pool: Pool, connectionProps: Seq[Props]): Props =
    Props(new RedisConnectionPoolActor(pool, connectionProps))

}

class RedisConnectionPoolActor(pool: Pool, connectionProps: Seq[Props]) extends Actor with ActorLogging {

  private val index = new AtomicLong(0L)

  private val routers: Seq[ActorRef] = connectionProps.map(p => context.actorOf(pool.props(p)))

  override def receive: Receive = {
    case msg => routers(index.getAndIncrement().toInt % routers.size) forward msg
  }

}
