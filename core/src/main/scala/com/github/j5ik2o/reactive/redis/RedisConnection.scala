package com.github.j5ik2o.reactive.redis

import java.time.ZonedDateTime
import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.actor.{ ActorRef, ActorSystem }
import akka.event.{ LogSource, Logging }
import akka.stream._
import akka.stream.scaladsl._
import akka.util.ByteString
import com.github.j5ik2o.reactive.redis.command.{ CommandRequestBase, CommandResponse }
import com.github.j5ik2o.reactive.redis.experimental.jedis.RedisConnectionJedis
import com.github.j5ik2o.reactive.redis.util.{ ActorSource, InTxRequestsAggregationFlow }
import enumeratum._
import monix.eval.Task
import monix.execution.Scheduler

import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{ Future, Promise }

object RedisConnection {

  implicit val logSource: LogSource[RedisConnection] = new LogSource[RedisConnection] {
    override def genString(o: RedisConnection): String = s"connection:${o.id}"

    override def getClazz(o: RedisConnection): Class[_] = o.getClass
  }

  final val DEFAULT_DECIDER: Supervision.Decider = {
    case _: StreamTcpException => Supervision.Restart
    case _                     => Supervision.Stop
  }

  def ofJedis(peerConfig: PeerConfig,
              supervisionDecider: Option[Supervision.Decider],
              redisConnectionMode: RedisConnectionMode)(implicit system: ActorSystem): RedisConnection =
    new RedisConnectionJedis(peerConfig, supervisionDecider)

  def apply(peerConfig: PeerConfig,
            supervisionDecider: Option[Supervision.Decider],
            redisConnectionMode: RedisConnectionMode)(
      implicit system: ActorSystem
  ): RedisConnection =
    new RedisConnectionImpl(peerConfig, supervisionDecider, redisConnectionMode)

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
      send(cmd).runAsync
    }
}

sealed trait RedisConnectionMode extends EnumEntry

object RedisConnectionMode extends Enum[RedisConnectionMode] {
  override def values: immutable.IndexedSeq[RedisConnectionMode] = findValues
  case object QueueMode extends RedisConnectionMode
  case object ActorMode extends RedisConnectionMode
}

@SuppressWarnings(
  Array("org.wartremover.warts.Null",
        "org.wartremover.warts.Var",
        "org.wartremover.warts.Serializable",
        "org.wartremover.warts.MutableDataStructures")
)
private[redis] class RedisConnectionImpl(val peerConfig: PeerConfig,
                                         val supervisionDecider: Option[Supervision.Decider],
                                         val redisConnectionMode: RedisConnectionMode)(
    implicit system: ActorSystem
) extends RedisConnection {

  lazy val id: UUID = UUID.randomUUID()

  import peerConfig._

  private lazy val log = Logging(system, this)

  private implicit lazy val mat: ActorMaterializer = ActorMaterializer(
    ActorMaterializerSettings(system).withSupervisionStrategy(
      supervisionDecider.getOrElse(RedisConnection.DEFAULT_DECIDER)
    )
  )

  protected lazy val tcpFlow: Flow[ByteString, ByteString, NotUsed] = {
    backoffConfig match {
      case Some(_backoffConfig) =>
        RestartFlow.withBackoff(_backoffConfig.minBackoff,
                                _backoffConfig.maxBackoff,
                                _backoffConfig.randomFactor,
                                _backoffConfig.maxRestarts) { () =>
          Tcp()
            .outgoingConnection(remoteAddress, localAddress, options, halfClose, connectTimeout, idleTimeout)
        }
      case None =>
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

  private def sendBySourceActorRef[C <: CommandRequestBase](cmd: C): Task[cmd.Response] = {
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
        else Duration(Long.MaxValue, TimeUnit.NANOSECONDS)
      )
  }

  private def sendByQueue[C <: CommandRequestBase](cmd: C): Task[cmd.Response] =
    Task
      .deferFutureAction { implicit ec =>
        val promise = Promise[CommandResponse]()
        requestQueue
          .offer(RequestContext(cmd, promise, ZonedDateTime.now()))
          .flatMap {
            case QueueOfferResult.Enqueued =>
              promise.future.map(_.asInstanceOf[cmd.Response])
            case QueueOfferResult.Failure(t) =>
              Future.failed(BufferOfferException("Failed to send request", Some(t)))
            case QueueOfferResult.Dropped =>
              Future.failed(
                BufferOfferException(
                  s"Failed to send request, the queue buffer was full."
                )
              )
            case QueueOfferResult.QueueClosed =>
              Future.failed(BufferOfferException("Failed to send request, the queue was closed"))
          }
      }
      .timeout(
        if (peerConfig.requestTimeout.isFinite())
          Duration(peerConfig.requestTimeout.length, peerConfig.requestTimeout.unit)
        else Duration(Long.MaxValue, TimeUnit.NANOSECONDS)
      )

  private var requestQueue: SourceQueueWithComplete[RequestContext] = _
  private var requestActorRef: Future[ActorRef]                     = _
  private var killSwitch: UniqueKillSwitch                          = _

  protected lazy val sourceQueueWithKillSwitchRunnableGraph
    : RunnableGraph[(SourceQueueWithComplete[RequestContext], UniqueKillSwitch)] =
    Source
      .queue[RequestContext](requestBufferSize, overflowStrategy)
      .via(connectionFlow)
      .via(InTxRequestsAggregationFlow())
      .async
      .map { res =>
        val result = res.complete
        if (res.isQuit) shutdown()
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
        if (res.isQuit) shutdown()
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

  def shutdown(): Unit = {
    log.debug("shutdown: start")
    killSwitch.shutdown()
    log.debug("shutdown: finished")
  }

  def send[C <: CommandRequestBase](cmd: C): Task[cmd.Response] = {
    redisConnectionMode match {
      case RedisConnectionMode.QueueMode =>
        sendByQueue(cmd)
      case RedisConnectionMode.ActorMode =>
        sendBySourceActorRef(cmd)
    }
  }

}
