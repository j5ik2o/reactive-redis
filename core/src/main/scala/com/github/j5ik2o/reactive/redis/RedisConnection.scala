package com.github.j5ik2o.reactive.redis

import java.time.ZonedDateTime
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference

import akka.NotUsed
import akka.actor.ActorSystem
import akka.event.{ LogSource, Logging }
import akka.stream._
import akka.stream.scaladsl._
import akka.util.ByteString
import cats.implicits._
import com.github.j5ik2o.reactive.redis.command.connection.{ PingRequest, PingSucceeded }
import com.github.j5ik2o.reactive.redis.command.strings._
import com.github.j5ik2o.reactive.redis.command.transactions.InTxRequestsAggregationFlow
import com.github.j5ik2o.reactive.redis.command.{ CommandRequestBase, CommandResponse }
import monix.eval.Task
import monix.execution.Scheduler
import org.apache.commons.lang3.time.StopWatch
import redis.clients.jedis.Jedis

import scala.concurrent.{ Future, Promise }

object RedisConnection {

  implicit val logSource: LogSource[RedisConnection] = new LogSource[RedisConnection] {
    override def genString(o: RedisConnection): String  = s"connection:${o.id}"
    override def getClazz(o: RedisConnection): Class[_] = o.getClass
  }

  final val DEFAULT_DECIDER: Supervision.Decider = {
    case _: StreamTcpException => Supervision.Restart
    case _                     => Supervision.Stop
  }

  def apply(peerConfig: PeerConfig,
            supervisionDecider: Option[Supervision.Decider])(implicit system: ActorSystem): RedisConnection =
//    new RedisConnectionJedis(peerConfig, supervisionDecider)
    new RedisConnectionImpl(peerConfig, supervisionDecider)

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

final case class ResettableRedisConnection(newRedisConnection: () => RedisConnection) extends RedisConnection {
  private val underlying: AtomicReference[RedisConnection] = new AtomicReference[RedisConnection](newRedisConnection())

  override def id: UUID = underlying.get.id

  override def peerConfig: PeerConfig = underlying.get.peerConfig

  def reset(): Unit = {
    underlying.set(newRedisConnection())
    // shutdown()
  }

  override def shutdown(): Unit = {
    underlying.get.shutdown()
  }

  override def toFlow[C <: CommandRequestBase](parallelism: Int)(
      implicit scheduler: Scheduler
  ): Flow[C, C#Response, NotUsed] = underlying.get.toFlow(parallelism)

  override def send[C <: CommandRequestBase](cmd: C): Task[cmd.Response] = underlying.get.send(cmd)

}

private[redis] class RedisConnectionJedis(val peerConfig: PeerConfig, supervisionDecider: Option[Supervision.Decider])
    extends RedisConnection {
  lazy val jedis = new Jedis(peerConfig.remoteAddress.getHostName, peerConfig.remoteAddress.getPort)

  override def id: UUID = UUID.randomUUID()

  override def shutdown(): Unit = jedis.close()

  override def send[C <: CommandRequestBase](cmd: C): Task[cmd.Response] = {
    cmd match {
      case c: SetRequest =>
        Task {
          val result = jedis.set(c.key, c.value)
          SetSucceeded(UUID.randomUUID(), c.id).asInstanceOf[cmd.Response]
        }
      case c: PingRequest =>
        Task {
          val result = jedis.ping()
          PingSucceeded(UUID.randomUUID(), c.id, result).asInstanceOf[cmd.Response]
        }
      case c: GetRequest =>
        Task {
          val result = jedis.get(c.key)
          GetSucceeded(UUID.randomUUID(), c.id, Some(result)).asInstanceOf[cmd.Response]
        }
    }
  }
}

private[redis] class RedisConnectionImpl(val peerConfig: PeerConfig, supervisionDecider: Option[Supervision.Decider])(
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

  protected lazy val (requestQueue: SourceQueueWithComplete[RequestContext], killSwitch: UniqueKillSwitch) = Source
    .queue[RequestContext](requestBufferSize, overflowStrategy)
    .via(connectionFlow)
    .via(InTxRequestsAggregationFlow())
    .map { responseContext =>
      val stopWatch = new StopWatch()
      stopWatch.start()
      val result = responseContext.parseResponse
      stopWatch.stop()

      if (log.isDebugEnabled)
        log.debug(s"req_id = {}, command = {}: parse = {} nano seconds",
                  responseContext.commandRequestId,
                  responseContext.commandRequestString,
                  stopWatch.getNanoTime)

      responseContext.completePromise(result.toTry)
    }
    .viaMat(KillSwitches.single)(Keep.both)
    .toMat(Sink.ignore)(Keep.left)
    .withAttributes(ActorAttributes.dispatcher("reactive-redis.dispatcher"))
    .run()

  def shutdown(): Unit = killSwitch.shutdown()

  def send[C <: CommandRequestBase](cmd: C): Task[cmd.Response] = {
    val stopWatch = new StopWatch()
    stopWatch.start()
    val promise = Promise[CommandResponse]()
    val offerResult = requestQueue
      .offer(RequestContext(cmd, promise, ZonedDateTime.now()))
    Task.deferFutureAction { implicit ec =>
      offerResult.flatMap {
        case QueueOfferResult.Enqueued =>
          val f = promise.future.map(_.asInstanceOf[cmd.Response])
          f.onComplete { _ =>
            stopWatch.stop()
            log.debug("send: {} nano seconds", stopWatch.getNanoTime)
          }
          f
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
  }

}
