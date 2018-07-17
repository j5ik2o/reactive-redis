package com.github.j5ik2o.reactive.redis.pool

import akka.actor.{ Actor, ActorLogging, ActorSystem, Props }
import akka.pattern.pipe
import akka.util.Timeout
import com.github.j5ik2o.reactive.redis.command.{ CommandRequestBase, CommandResponse }
import com.github.j5ik2o.reactive.redis.pool.RedisConnectionActor.{ BorrowConnection, ConnectionGotten }
import com.github.j5ik2o.reactive.redis.{ PeerConfig, RedisConnection }
import monix.execution.Scheduler

import scala.concurrent.duration._

object RedisConnectionActor {

  def props(peerConfig: PeerConfig, passingTimeout: FiniteDuration, newConnection: PeerConfig => RedisConnection)(
      implicit scheduler: Scheduler
  ): Props =
    Props(new RedisConnectionActor(peerConfig, passingTimeout, newConnection))

  case object BorrowConnection
  case class ConnectionGotten(redisConnection: RedisConnection)

}

class RedisConnectionActor(peerConfig: PeerConfig,
                           passingTimeout: FiniteDuration,
                           newConnection: PeerConfig => RedisConnection)(
    implicit scheduler: Scheduler
) extends Actor
    with ActorLogging {
  private implicit val as: ActorSystem    = context.system
  private val connection: RedisConnection = newConnection(peerConfig)
  implicit val to: Timeout                = passingTimeout

  override def postStop(): Unit = {
    log.debug("connection_id = {}: connection#shutdown", connection.id)
    connection.shutdown()
  }

  override def receive: Receive = {
    case cmdReq: CommandRequestBase =>
      connection.send(cmdReq).runAsync.mapTo[CommandResponse].pipeTo(sender())
    case BorrowConnection =>
      sender() ! ConnectionGotten(connection)
  }

}
