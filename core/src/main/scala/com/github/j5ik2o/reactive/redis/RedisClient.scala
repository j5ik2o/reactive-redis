package com.github.j5ik2o.reactive.redis

import java.util.UUID

import akka.actor.{ActorRef, ActorSystem, PoisonPill}
import akka.pattern._
import akka.util.Timeout
import com.github.j5ik2o.reactive.redis.Options.StartAndEnd
import com.github.j5ik2o.reactive.redis.StringOperations._

import scala.concurrent.{ExecutionContext, Future}

trait RedisStringClient { this: RedisClient =>

  private implicit val to = timeout

  def append(key: String, value: String)(implicit ec: ExecutionContext): Future[Unit] = {
    (redisActor ? AppendRequest(UUID.randomUUID(), key, value)).mapTo[AppendResponse].flatMap {
      case AppendFailed(_, _, ex) => Future.failed(ex)
      case _                      => Future.successful(())
    }
  }

  def bitCount(key: String, startAndEnd: Option[StartAndEnd] = None)(
      implicit ec: ExecutionContext): Future[Option[Int]] = {
    (redisActor ? BitCountRequest(UUID.randomUUID(), key, startAndEnd))
      .mapTo[BitCountResponse]
      .flatMap {
        case BitCountFailed(_, _, ex)        => Future.failed(ex)
        case BitCountSuspended(_, _)         => Future.successful(None)
        case BitCountSucceeded(_, _, result) => Future.successful(Some(result))
      }
  }

  def desc(key: String)(implicit ec: ExecutionContext): Future[Option[Int]] = {
    (redisActor ? DecrRequest(UUID.randomUUID(), key)).mapTo[DecrResponse].flatMap {
      case DecrFailed(_, _, ex)       => Future.failed(ex)
      case DecrSuspended(_, _)        => Future.successful(None)
      case DecrSucceeded(_, _, value) => Future.successful(Some(value))
    }
  }

  def get(key: String)(implicit ec: ExecutionContext): Future[Option[String]] = {
    (redisActor ? GetRequest(UUID.randomUUID(), key)).mapTo[GetResponse].flatMap {
      case GetFailed(_, _, ex)       => Future.failed(ex)
      case GetSuspended(_, _)        => Future.successful(None)
      case GetSucceeded(_, _, value) => Future.successful(value)
    }
  }

  def getSet(key: String, value: String)(implicit ec: ExecutionContext): Future[Option[String]] = {
    (redisActor ? GetSetRequest(UUID.randomUUID(), key, value)).mapTo[GetSetResponse].flatMap {
      case GetSetFailed(_, _, ex)        => Future.failed(ex)
      case GetSetSuspended(_, _)         => Future.successful(None)
      case GetSetSucceeded(_, _, result) => Future.successful(result)
    }
  }

  def set(key: String, value: String)(implicit ec: ExecutionContext): Future[Unit] = {
    (redisActor ? SetRequest(UUID.randomUUID(), key, value)).mapTo[SetResponse].flatMap {
      case SetFailed(_, _, ex) => Future.failed(ex)
      case _                   => Future.successful(())
    }
  }

  def incr(key: String)(implicit ec: ExecutionContext): Future[Option[Int]] = {
    (redisActor ? IncrRequest(UUID.randomUUID(), key)).mapTo[IncrResponse].flatMap {
      case IncrFailed(_, _, ex)        => Future.failed(ex)
      case IncrSuspended(_, _)         => Future.successful(None)
      case IncrSucceeded(_, _, result) => Future.successful(Some(result))
    }
  }

}

trait RedisClient extends RedisStringClient {

  protected val redisActor: ActorRef

  protected val timeout: Timeout

  private implicit val to = timeout

  def dispose()(implicit ec: ExecutionContext): Future[Unit] = {
    (redisActor ? PoisonPill).map(_ => ())
  }

}

object RedisClient {

  def apply(id: UUID, host: String, port: Int, timeout: Timeout)(
      implicit actorSystem: ActorSystem): RedisClient = {
    val redisActorRef = actorSystem.actorOf(RedisActor.props(id, host, port))
    apply(redisActorRef, timeout)
  }

  def apply(redisActorRef: ActorRef, timeout: Timeout): RedisClient =
    new Default(redisActorRef, timeout)

  class Default(protected val redisActor: ActorRef, protected val timeout: Timeout)
      extends RedisClient
      with RedisStringClient

}
