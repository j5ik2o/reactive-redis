package com.github.j5ik2o.reactive.redis

import java.util.UUID

import akka.actor.{ ActorRef, ActorSystem, PoisonPill }
import akka.pattern._
import akka.util.Timeout

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ ExecutionContext, Future }

trait RedisFutureClient extends RedisStringFutureClient {

  val redisActor: ActorRef

  protected val timeout: Timeout

  private implicit val to = timeout

  def dispose(): Unit = {
    redisActor ! PoisonPill
  }

}

object RedisFutureClient {

  def apply(id: UUID = UUID.randomUUID(), host: String, port: Int = 6379, timeout: FiniteDuration)(
      implicit actorSystem: ActorSystem
  ): RedisFutureClient = {
    val redisActorRef = actorSystem.actorOf(RedisActor.props(id, host, port))
    apply(redisActorRef, Timeout(timeout))
  }

  def apply(redisActorRef: ActorRef, timeout: Timeout): RedisFutureClient =
    new Default(redisActorRef, timeout)

  class Default(val redisActor: ActorRef, protected val timeout: Timeout)
      extends RedisFutureClient
      with RedisStringFutureClient

}
