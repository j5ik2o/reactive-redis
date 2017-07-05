package com.github.j5ik2o.reactive.redis

import java.util.UUID

import akka.actor.{ ActorRef, ActorSystem, PoisonPill }
import akka.pattern._
import akka.util.Timeout

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ ExecutionContext, Future }

trait RedisFutureFutureClient extends RedisStringFutureClient {

  protected val redisActor: ActorRef

  protected val timeout: Timeout

  private implicit val to = timeout

  def dispose()(implicit ec: ExecutionContext): Future[Unit] = {
    (redisActor ? PoisonPill).map(_ => ())
  }

}

object RedisFutureFutureClient {

  def apply(id: UUID = UUID.randomUUID(), host: String, port: Int = 6379, timeout: FiniteDuration)(
      implicit actorSystem: ActorSystem
  ): RedisFutureFutureClient = {
    val redisActorRef = actorSystem.actorOf(RedisActor.props(id, host, port))
    apply(redisActorRef, Timeout(timeout))
  }

  def apply(redisActorRef: ActorRef, timeout: Timeout): RedisFutureFutureClient =
    new Default(redisActorRef, timeout)

  class Default(protected val redisActor: ActorRef, protected val timeout: Timeout)
      extends RedisFutureFutureClient
      with RedisStringFutureClient

}
