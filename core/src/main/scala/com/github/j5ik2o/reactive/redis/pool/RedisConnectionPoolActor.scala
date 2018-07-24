package com.github.j5ik2o.reactive.redis.pool

import java.util.concurrent.atomic.AtomicLong

import akka.actor.{ Actor, ActorLogging, ActorRef, Props }
import akka.routing._
import cats.data.NonEmptyList

object RedisConnectionPoolActor {

  def props(pool: Pool, connectionProps: NonEmptyList[Props]): Props =
    Props(new RedisConnectionPoolActor(pool, connectionProps))

}

class RedisConnectionPoolActor(pool: Pool, connectionProps: NonEmptyList[Props]) extends Actor with ActorLogging {

  private val index = new AtomicLong(0L)

  private val routers: NonEmptyList[ActorRef] = connectionProps.map(p => context.actorOf(pool.props(p)))

  override def receive: Receive = {
    case msg => routers.toList(index.getAndIncrement().toInt % routers.size) forward msg
  }

}
