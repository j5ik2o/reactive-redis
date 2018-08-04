package com.github.j5ik2o.reactive.redis.experimental.jedis

import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.actor.{ ActorRef, ActorSystem }
import akka.event.Logging
import akka.stream._
import akka.stream.scaladsl.{ Flow, GraphDSL, Keep, RestartFlow, Sink, Source, SourceQueueWithComplete, Unzip, Zip }
import com.github.j5ik2o.reactive.redis._
import com.github.j5ik2o.reactive.redis.command.{ CommandRequestBase, CommandResponse }
import com.github.j5ik2o.reactive.redis.util.ActorSource
import monix.eval.Task

import scala.concurrent.duration._
import scala.concurrent.{ Future, Promise }

@SuppressWarnings(
  Array("org.wartremover.warts.Null",
        "org.wartremover.warts.Var",
        "org.wartremover.warts.Serializable",
        "org.wartremover.warts.MutableDataStructures")
)
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

  private def sendToActor[C <: CommandRequestBase](cmd: C): Task[cmd.Response] = {
    val promise = Promise[CommandResponse]()
    Task
      .deferFutureAction { implicit ec =>
        requestActor.flatMap { ref =>
          ref ! (cmd, promise)
          promise.future.asInstanceOf[Future[cmd.Response]]
        }
      }
      .timeout(
        if (peerConfig.requestTimeout.isFinite())
          Duration(peerConfig.requestTimeout.length, peerConfig.requestTimeout.unit)
        else Duration(Long.MaxValue, TimeUnit.NANOSECONDS)
      )
  }

  private def sendToQueue[C <: CommandRequestBase](cmd: C): Task[cmd.Response] =
    Task
      .deferFutureAction { implicit ec =>
        val promise = Promise[CommandResponse]()
        requestQueue
          .offer((cmd, promise))
          .flatMap {
            case QueueOfferResult.Enqueued =>
              promise.future.map(_.asInstanceOf[cmd.Response])
            case QueueOfferResult.Failure(t) =>
              Future.failed(RedisRequestException("Failed to send request", Some(t)))
            case QueueOfferResult.Dropped =>
              Future.failed(
                RedisRequestException(
                  s"Failed to send request, the queue buffer was full."
                )
              )
            case QueueOfferResult.QueueClosed =>
              Future.failed(RedisRequestException("Failed to send request, the queue was closed"))
          }
      }
      .timeout(
        if (peerConfig.requestTimeout.isFinite())
          Duration(peerConfig.requestTimeout.length, peerConfig.requestTimeout.unit)
        else Duration(Long.MaxValue, TimeUnit.NANOSECONDS)
      )

  private var requestQueue: SourceQueueWithComplete[(CommandRequestBase, Promise[CommandResponse])] = _
  private var requestActor: Future[ActorRef]                                                        = _
  private var killSwitch: UniqueKillSwitch                                                          = _

  private lazy val sourceQueueWithKillSwitchRunnableGraph =
    Source
      .queue[(CommandRequestBase, Promise[CommandResponse])](peerConfig.requestBufferSize,
                                                             peerConfig.overflowStrategyOnQueueMode)
      .via(requestFlow)
      .map {
        case (res, promise) =>
          promise.success(res)
      }
      .viaMat(KillSwitches.single)(Keep.both)
      .toMat(Sink.ignore)(Keep.left)
      .withAttributes(ActorAttributes.dispatcher("reactive-redis.dispatcher"))

  private lazy val sourceActorWithKillSwitchRunnableGraph =
    ActorSource[(CommandRequestBase, Promise[CommandResponse])](peerConfig.requestBufferSize)
      .via(requestFlow)
      .map {
        case (res, promise) =>
          promise.success(res)
      }
      .viaMat(KillSwitches.single)(Keep.both)
      .toMat(Sink.ignore)(Keep.left)
      .withAttributes(ActorAttributes.dispatcher("reactive-redis.dispatcher"))

  peerConfig.redisConnectionMode match {
    case RedisConnectionMode.QueueMode =>
      val result = sourceQueueWithKillSwitchRunnableGraph.run()
      requestQueue = result._1
      killSwitch = result._2
    case RedisConnectionMode.ActorMode =>
      val result = sourceActorWithKillSwitchRunnableGraph.run()
      requestActor = result._1
      killSwitch = result._2
  }

  override def send[C <: CommandRequestBase](cmd: C): Task[cmd.Response] = {
    peerConfig.redisConnectionMode match {
      case RedisConnectionMode.QueueMode =>
        sendToQueue(cmd)
      case RedisConnectionMode.ActorMode =>
        sendToActor(cmd)
    }
  }
}
