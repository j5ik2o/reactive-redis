package com.github.j5ik2o.reactive.redis.pool

import akka.actor.{ Actor, ActorLogging, ActorSystem, PoisonPill, Props }
import akka.pattern.pipe
import akka.stream.Supervision
import akka.util.Timeout
import com.github.j5ik2o.reactive.redis.RedisConnection.{ Start, Stop }
import com.github.j5ik2o.reactive.redis.command.{ CommandRequestBase, CommandResponse }
import com.github.j5ik2o.reactive.redis.pool.RedisConnectionActor.{ BorrowConnection, ConnectionGotten }
import com.github.j5ik2o.reactive.redis.{ NewRedisConnection, PeerConfig, RedisConnection }
import monix.execution.Scheduler

import scala.concurrent.duration._

object RedisConnectionActor {

  def props(peerConfig: PeerConfig,
            newConnection: NewRedisConnection,
            supervisionDecider: Option[Supervision.Decider],
            passingTimeout: FiniteDuration)(
      implicit scheduler: Scheduler
  ): Props =
    Props(new RedisConnectionActor(peerConfig, newConnection, supervisionDecider, passingTimeout))

  case object BorrowConnection
  final case class ConnectionGotten(redisConnection: RedisConnection)

}
@SuppressWarnings(
  Array("org.wartremover.warts.Null",
        "org.wartremover.warts.Var",
        "org.wartremover.warts.Serializable",
        "org.wartremover.warts.MutableDataStructures")
)
final class RedisConnectionActor(
    peerConfig: PeerConfig,
    newConnection: NewRedisConnection,
    supervisionDecider: Option[Supervision.Decider],
    passingTimeout: FiniteDuration
)(
    implicit scheduler: Scheduler
) extends Actor
    with ActorLogging {
  private implicit val as: ActorSystem    = context.system
  private var connection: RedisConnection = _
  implicit val to: Timeout                = passingTimeout

  private def onDisconnect(): Unit = {
    if (connection != null)
      log.debug("connection_id = {}: onDisconnect", connection.id)
    self ! PoisonPill
  }

  override def preStart(): Unit = {
    log.debug("preStart: {}:{}", peerConfig.remoteAddress.getHostName, peerConfig.remoteAddress.getPort)
    connection = newConnection(peerConfig, supervisionDecider, Seq({
      case Stop =>
        onDisconnect()
      case Start =>
    }))
    log.debug("connection_id = {}: preStart", connection.id)
  }

  override def postStop(): Unit = {
    log.debug("postStop: {}:{}", peerConfig.remoteAddress.getHostName, peerConfig.remoteAddress.getPort)
    if (connection != null) {
      log.debug("connection_id = {}: postStop: {}:{}",
                connection.id,
                peerConfig.remoteAddress.getHostName,
                peerConfig.remoteAddress.getPort)
      connection.shutdown()
    }
  }

  override def receive: Receive = {
    case cmdReq: CommandRequestBase =>
      connection.send(cmdReq).runToFuture.mapTo[CommandResponse].pipeTo(sender())
    case BorrowConnection =>
      sender() ! ConnectionGotten(connection)
  }

}
