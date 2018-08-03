package com.github.j5ik2o.reactive.redis.experimental.jedis

import java.util.UUID

import akka.NotUsed
import akka.actor.{ ActorRef, ActorSystem }
import akka.event.Logging
import akka.stream._
import akka.stream.scaladsl.{ Flow, GraphDSL, Keep, RestartFlow, Sink, Unzip, Zip }
import akka.util.Timeout
import com.github.j5ik2o.reactive.redis.command.{ CommandRequestBase, CommandResponse }
import com.github.j5ik2o.reactive.redis.util.ActorSource
import com.github.j5ik2o.reactive.redis.{ PeerConfig, RedisConnection }
import monix.eval.Task

import scala.concurrent.duration._
import scala.concurrent.{ Future, Promise }

private[redis] class RedisConnectionJedis(val peerConfig: PeerConfig, supervisionDecider: Option[Supervision.Decider])(
    implicit system: ActorSystem
) extends RedisConnection {

  private implicit lazy val mat: ActorMaterializer = ActorMaterializer(
    ActorMaterializerSettings(system).withSupervisionStrategy(
      supervisionDecider.getOrElse(RedisConnection.DEFAULT_DECIDER)
    )
  )

  private lazy val log = Logging(system, this)

  override def id: UUID = UUID.randomUUID()

  override def shutdown(): Unit = { killSwitch.shutdown() }

  private lazy val jedisFlow: Flow[CommandRequestBase, CommandResponse, NotUsed] =
    peerConfig.backoffConfig match {
      case Some(_backoffConfig) =>
        RestartFlow.withBackoff(_backoffConfig.minBackoff,
                                _backoffConfig.maxBackoff,
                                _backoffConfig.randomFactor,
                                _backoffConfig.maxRestarts) { () =>
          JedisFlow(peerConfig.remoteAddress.getHostName,
                    peerConfig.remoteAddress.getPort,
                    Some(peerConfig.connectTimeout),
                    Some(peerConfig.connectTimeout))(
            system.dispatcher
          ).mapMaterializedValue(_ => NotUsed)
        }
      case None =>
        JedisFlow(peerConfig.remoteAddress.getHostName,
                  peerConfig.remoteAddress.getPort,
                  Some(peerConfig.connectTimeout),
                  Some(peerConfig.connectTimeout))(
          system.dispatcher
        ).mapMaterializedValue(_ => NotUsed)
    }

  private lazy val requestFlow
    : Flow[(CommandRequestBase, Promise[CommandResponse]), (CommandResponse, Promise[CommandResponse]), NotUsed] =
    Flow.fromGraph(GraphDSL.create(jedisFlow) { implicit b => jf =>
      import GraphDSL.Implicits._
      val unzip = b.add(Unzip[CommandRequestBase, Promise[CommandResponse]]())
      val zip   = b.add(Zip[CommandResponse, Promise[CommandResponse]]())

      unzip.out1 ~> zip.in1
      unzip.out0 ~> jf ~> zip.in0
      FlowShape(unzip.in, zip.out)
    })

  private lazy val (sourceActorRefFuture: Future[ActorRef], killSwitch: UniqueKillSwitch) =
    ActorSource[(CommandRequestBase, Promise[CommandResponse])](peerConfig.requestBufferSize)
      .via(requestFlow)
      .map {
        case (res, promise) =>
          promise.success(res)
      }
      .viaMat(KillSwitches.single)(Keep.both)
      .toMat(Sink.ignore)(Keep.left)
      .withAttributes(ActorAttributes.dispatcher("reactive-redis.dispatcher"))
      .run()

  override def send[C <: CommandRequestBase](cmd: C): Task[cmd.Response] = {
    val promise = Promise[CommandResponse]()
    Task.deferFutureAction { implicit ec =>
      sourceActorRefFuture.flatMap { ref =>
        implicit val to: Timeout = Timeout(10 seconds)
        ref ! (cmd, promise)
        promise.future.asInstanceOf[Future[cmd.Response]]
      }
    }
  }
}
