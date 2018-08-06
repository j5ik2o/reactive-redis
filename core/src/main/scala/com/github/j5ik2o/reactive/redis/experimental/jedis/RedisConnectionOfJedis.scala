package com.github.j5ik2o.reactive.redis.experimental.jedis

import java.util.UUID
import java.util.concurrent.TimeoutException

import akka.NotUsed
import akka.actor.{ Actor, ActorRef, ActorSystem, OneForOneStrategy, Props }
import akka.pattern.{ ask, AskTimeoutException }
import akka.stream._
import akka.stream.scaladsl.{ Flow, GraphDSL, Keep, RestartFlow, Sink, Source, SourceQueueWithComplete, Unzip, Zip }
import akka.util.Timeout
import com.github.j5ik2o.akka.backoff.enhancement.{ Backoff, BackoffSupervisor }
import com.github.j5ik2o.reactive.redis.RedisConnection.EventHandler
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
private[redis] class RedisConnectionOfJedis(val peerConfig: PeerConfig,
                                            supervisionDecider: Option[Supervision.Decider],
                                            listeners: Seq[EventHandler])(
    implicit val system: ActorSystem
) extends RedisConnection
    with RedisConnectionSupport {

  import RedisConnection._

  override lazy val id: UUID = UUID.randomUUID()

  private class InternalActor extends Actor {

    private implicit lazy val mat: ActorMaterializer = ActorMaterializer(
      ActorMaterializerSettings(system).withSupervisionStrategy(
        supervisionDecider.getOrElse(RedisConnection.DEFAULT_SUPERVISION_DECIDER)
      )
    )(context.system)

    private lazy val jedisFlow: Flow[CommandRequestBase, CommandResponse, NotUsed] =
      peerConfig.connectionBackoffConfig match {
        case Some(_backoffConfig) =>
          RestartFlow.withBackoff(_backoffConfig.minBackoff,
                                  _backoffConfig.maxBackoff,
                                  _backoffConfig.randomFactor,
                                  _backoffConfig.maxRestarts) { () =>
            log.debug("create connection with backoff")
            JedisFlow(peerConfig.remoteAddress.getHostName,
                      peerConfig.remoteAddress.getPort,
                      Some(peerConfig.connectTimeout),
                      Some(peerConfig.connectTimeout))(
              system.dispatcher
            ).mapMaterializedValue(_ => NotUsed)
          }
        case None =>
          log.debug("create connection without backoff")
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
          else DEFAULT_REQUEST_TIMEOUT
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
          else DEFAULT_REQUEST_TIMEOUT
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

    override def preStart(): Unit = {
      listeners.foreach(_(Start))
    }

    override def postStop(): Unit = {
      listeners.foreach(_(Stop))
    }

    override def receive: Receive = {
      case cmd: CommandRequestBase =>
        peerConfig.redisConnectionMode match {
          case RedisConnectionMode.QueueMode =>
            sender() ! sendToQueue(cmd)
          case RedisConnectionMode.ActorMode =>
            sender() ! sendToActor(cmd)
        }
      case ShutdownConnection =>
        killSwitch.shutdown()
        context.stop(self)
    }

  }

  private lazy val childProps = Props(new InternalActor)

  private lazy val connectionRef = peerConfig.connectionBackoffConfig match {
    case Some(bf) =>
      val stopBackOffOptions =
        Backoff
          .onStop(childProps, "backoff-connection", bf.minBackoff, bf.maxBackoff, bf.randomFactor)
          .withSupervisorStrategy(
            OneForOneStrategy(maxNrOfRetries = bf.maxRestarts)(DEFAULT_SUPERVISOR_STRATEGY_DECIDER)
          )
          .withOnStopChildHandler { v =>
            log.debug("child stop: {}", v)
          }
      system.actorOf(BackoffSupervisor.props(stopBackOffOptions))
    case None =>
      system.actorOf(childProps)
  }

  override def shutdown(): Unit = {
    connectionRef ! ShutdownConnection
  }

  override def send[C <: CommandRequestBase](cmd: C): Task[cmd.Response] = {
    implicit val to: Timeout =
      if (peerConfig.requestTimeout.isFinite())
        Duration(peerConfig.requestTimeout.length, peerConfig.requestTimeout.unit)
      else DEFAULT_REQUEST_TIMEOUT
    def send0(cmd: C): Task[cmd.Response] = {
      val result: Future[Task[cmd.Response]] = (connectionRef ? cmd).mapTo[Task[cmd.Response]]
      Task
        .fromFuture(result)
        .flatten
        .onErrorRecoverWith {
          case ex: AskTimeoutException =>
            Task.raiseError(RedisRequestException("ask timeout", Some(ex)))
          case ex: TimeoutException =>
            Task.raiseError(RedisRequestException("task timeout", Some(ex)))
        }
    }
    peerConfig.requestBackoffConfig match {
      case Some(bf) =>
        val maxRestarts = if (bf.maxRestarts == -1) RETRY_MAX else bf.maxRestarts
        val start       = 1
        retryBackoff(send0(cmd), maxRestarts, start, bf.minBackoff, bf.maxBackoff, bf.randomFactor)
      case None =>
        send0(cmd)
    }
  }
}
