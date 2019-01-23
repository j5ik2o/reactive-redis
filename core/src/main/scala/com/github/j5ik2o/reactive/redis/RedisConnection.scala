package com.github.j5ik2o.reactive.redis

import java.util.UUID

import akka.NotUsed
import akka.actor.{ ActorSystem, SupervisorStrategy }
import akka.event.LogSource
import akka.stream._
import akka.stream.scaladsl._
import com.github.j5ik2o.reactive.redis.command.CommandRequestBase
import com.github.j5ik2o.reactive.redis.experimental.jedis.RedisConnectionOfJedis
import monix.eval.Task
import monix.execution.Scheduler

import scala.concurrent.duration._

object RedisConnection {

  implicit val logSource: LogSource[RedisConnection] = new LogSource[RedisConnection] {
    override def genString(o: RedisConnection): String = s"connection:${o.id}"

    override def getClazz(o: RedisConnection): Class[_] = o.getClass
  }

  final val DEFAULT_SUPERVISION_DECIDER: Supervision.Decider = {
    case _: StreamTcpException =>
      Supervision.Stop
    case _: Throwable =>
      Supervision.Stop
  }

  final val DEFAULT_SUPERVISOR_STRATEGY_DECIDER: SupervisorStrategy.Decider =
    ({
      case _: Throwable => SupervisorStrategy.Stop
    }: SupervisorStrategy.Decider).orElse(SupervisorStrategy.defaultDecider)

  def ofJedis(peerConfig: PeerConfig, supervisionDecider: Option[Supervision.Decider], listeners: Seq[EventHandler])(
      implicit system: ActorSystem
  ): RedisConnection =
    new RedisConnectionOfJedis(peerConfig, supervisionDecider, listeners)

  def ofDefault(peerConfig: PeerConfig, supervisionDecider: Option[Supervision.Decider], listeners: Seq[EventHandler])(
      implicit system: ActorSystem
  ): RedisConnection =
    apply(peerConfig, supervisionDecider, listeners)

  def apply(peerConfig: PeerConfig, supervisionDecider: Option[Supervision.Decider], listeners: Seq[EventHandler])(
      implicit system: ActorSystem
  ): RedisConnection =
    new RedisConnectionImpl(peerConfig, supervisionDecider, listeners)

  private[redis] case object ShutdownConnection

  val DEFAULT_REQUEST_TIMEOUT: FiniteDuration = 10 seconds

  val RETRY_MAX: Int = Int.MaxValue

  type EventHandler = Event => Unit

  sealed trait Event

  case object Stop extends Event

  case object Start extends Event

}

trait RedisConnection {
  def id: UUID

  def peerConfig: PeerConfig

  def shutdown(): Unit

  def send[C <: CommandRequestBase](cmd: C): Task[cmd.Response]

  def toFlow[C <: CommandRequestBase](
      parallelism: Int = 1
  )(implicit scheduler: Scheduler): Flow[C, C#Response, NotUsed] =
    Flow[C].mapAsync(parallelism) { cmd =>
      send(cmd).runToFuture
    }
}
