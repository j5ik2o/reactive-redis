package com.github.j5ik2o.reactive.redis

import java.time.ZonedDateTime
import java.util.UUID
import java.util.concurrent.TimeoutException

import akka.NotUsed
import akka.actor.{ Actor, ActorRef, ActorSystem, OneForOneStrategy, Props }
import akka.pattern.AskTimeoutException
import akka.pattern.ask
import akka.stream._
import akka.stream.scaladsl.{
  Flow,
  GraphDSL,
  Keep,
  RestartFlow,
  RunnableGraph,
  Sink,
  Source,
  SourceQueueWithComplete,
  Tcp,
  Unzip,
  Zip
}
import akka.util.{ ByteString, Timeout }
import com.github.j5ik2o.akka.backoff.enhancement.{ Backoff, BackoffSupervisor }
import com.github.j5ik2o.reactive.redis.RedisConnection.Event
import com.github.j5ik2o.reactive.redis.command.{ CommandRequestBase, CommandResponse }
import com.github.j5ik2o.reactive.redis.util.{ ActorSource, InTxRequestsAggregationFlow }
import monix.eval.Task

import scala.concurrent.duration.Duration
import scala.concurrent.{ Future, Promise }

@SuppressWarnings(
  Array(
    "org.wartremover.warts.Null",
    "org.wartremover.warts.Var",
    "org.wartremover.warts.Serializable",
    "org.wartremover.warts.MutableDataStructures",
    "org.wartremover.warts.Recursion"
  )
)
private[redis] class RedisConnectionImpl(val peerConfig: PeerConfig,
                                         val supervisionDecider: Option[Supervision.Decider],
                                         val listeners: Seq[Event => Unit])(
    implicit val system: ActorSystem
) extends RedisConnection
    with RedisConnectionSupport {

  import RedisConnection._

  lazy val id: UUID = UUID.randomUUID()

  import peerConfig._

  private class InternalActor extends Actor {

    private implicit lazy val mat: ActorMaterializer = ActorMaterializer(
      ActorMaterializerSettings(system).withSupervisionStrategy(
        supervisionDecider.getOrElse(RedisConnection.DEFAULT_SUPERVISION_DECIDER)
      )
    )(context.system)

    protected lazy val tcpFlow: Flow[ByteString, ByteString, NotUsed] = {
      connectionBackoffConfig match {
        case Some(bc) =>
          RestartFlow.withBackoff(bc.minBackoff, bc.maxBackoff, bc.randomFactor, bc.maxRestarts) { () =>
            log.debug("create connection with backoff")
            Tcp()
              .outgoingConnection(remoteAddress, localAddress, options, halfClose, connectTimeout, idleTimeout)
          }
        case None =>
          log.debug("create connection")
          Tcp()
            .outgoingConnection(remoteAddress, localAddress, options, halfClose, connectTimeout, idleTimeout)
            .mapMaterializedValue(_ => NotUsed)
      }
    }

    protected lazy val connectionFlow: Flow[RequestContext, ResponseContext, NotUsed] =
      Flow.fromGraph(GraphDSL.create() { implicit b =>
        import GraphDSL.Implicits._
        val requestFlow = b.add(
          Flow[RequestContext]
            .map { rc =>
              if (log.isDebugEnabled)
                log.debug(s"request = [{}]", rc.commandRequestString)
              (ByteString.fromString(rc.commandRequest.asString + "\r\n"), rc)
            }
        )
        val responseFlow = b.add(Flow[(ByteString, RequestContext)].map {
          case (byteString, requestContext) =>
            if (log.isDebugEnabled)
              log.debug(s"response = [{}]", byteString.utf8String)
            ResponseContext(byteString, requestContext)
        })
        val unzip = b.add(Unzip[ByteString, RequestContext]())
        val zip   = b.add(Zip[ByteString, RequestContext]())
        requestFlow.out ~> unzip.in
        unzip.out0 ~> tcpFlow ~> zip.in0
        unzip.out1 ~> zip.in1
        zip.out ~> responseFlow.in
        FlowShape(requestFlow.in, responseFlow.out)
      })

    private var requestQueue: SourceQueueWithComplete[RequestContext] = _
    private var requestActorRef: Future[ActorRef]                     = _
    private var killSwitch: UniqueKillSwitch                          = _

    protected lazy val sourceQueueWithKillSwitchRunnableGraph
      : RunnableGraph[(SourceQueueWithComplete[RequestContext], UniqueKillSwitch)] =
      Source
        .queue[RequestContext](requestBufferSize, overflowStrategyOnQueueMode)
        .via(connectionFlow)
        .via(InTxRequestsAggregationFlow())
        .async
        .map { res =>
          val result = res.complete
          if (res.isQuit) self ! ShutdownConnection
          result
        }
        .viaMat(KillSwitches.single)(Keep.both)
        .toMat(Sink.ignore)(Keep.left)
        .withAttributes(ActorAttributes.dispatcher("reactive-redis.dispatcher"))

    protected lazy val sourceActorWithKillSwitchRunnableGraph: RunnableGraph[(Future[ActorRef], UniqueKillSwitch)] =
      ActorSource[RequestContext](requestBufferSize)
        .via(connectionFlow)
        .via(InTxRequestsAggregationFlow())
        .async
        .map { res =>
          val result = res.complete
          if (res.isQuit) self ! ShutdownConnection
          result
        }
        .viaMat(KillSwitches.single)(Keep.both)
        .toMat(Sink.ignore)(Keep.left)
        .withAttributes(ActorAttributes.dispatcher("reactive-redis.dispatcher"))

    redisConnectionMode match {
      case RedisConnectionMode.QueueMode =>
        val result = sourceQueueWithKillSwitchRunnableGraph.run()
        requestQueue = result._1
        killSwitch = result._2
      case RedisConnectionMode.ActorMode =>
        val result = sourceActorWithKillSwitchRunnableGraph.run()
        requestActorRef = result._1
        killSwitch = result._2
    }

    private def sendToActor[C <: CommandRequestBase](cmd: C): Task[cmd.Response] = {
      val promise = Promise[CommandResponse]()
      Task
        .deferFutureAction { implicit ec =>
          requestActorRef.flatMap { ref =>
            ref ! RequestContext(cmd, promise, ZonedDateTime.now())
            val result = promise.future.asInstanceOf[Future[cmd.Response]]
            result
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
            .offer(RequestContext(cmd, promise, ZonedDateTime.now()))
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

    override def preStart(): Unit = {
      listeners.foreach(_(Start))
    }

    override def postStop(): Unit = {
      listeners.foreach(_(Stop))
    }

    override def receive: Receive = {
      case cmd: CommandRequestBase =>
        redisConnectionMode match {
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

  private val childProps = Props(new InternalActor)

  private val connectionRef = peerConfig.connectionBackoffConfig match {
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

  def shutdown(): Unit = {
    connectionRef ! ShutdownConnection
  }

  def send[C <: CommandRequestBase](cmd: C): Task[cmd.Response] = {
    def send0(cmd: C): Task[cmd.Response] = {
      implicit val to: Timeout =
        if (peerConfig.requestTimeout.isFinite())
          Duration(peerConfig.requestTimeout.length, peerConfig.requestTimeout.unit)
        else DEFAULT_REQUEST_TIMEOUT
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
