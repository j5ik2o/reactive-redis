package com.github.j5ik2o.reactive.redis

import java.time.ZonedDateTime
import java.util.UUID

import akka.NotUsed
import akka.actor.ActorSystem
import akka.event.{ LogSource, Logging }
import akka.stream._
import akka.stream.scaladsl._
import akka.util.ByteString
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse }
import monix.eval.Task
import monix.execution.Scheduler

import scala.concurrent.{ Future, Promise }

object RedisConnection {

  implicit val logSource: LogSource[RedisConnection] = new LogSource[RedisConnection] {
    override def genString(o: RedisConnection): String  = s"${o.getClass.getName}:${o.id}"
    override def getClazz(o: RedisConnection): Class[_] = o.getClass
  }

  final val DEFAULT_DECIDER: Supervision.Decider = {
    case _: StreamTcpException => Supervision.Restart
    case _                     => Supervision.Stop
  }

}

class RedisConnection(connectionConfig: ConnectionConfig, supervisionDecider: Option[Supervision.Decider] = None)(
    implicit system: ActorSystem
) {

  val id: UUID = UUID.randomUUID()

  import connectionConfig._
  import connectionConfig.backoffConfig._

  private val log = Logging(system, this)

  private implicit val mat: ActorMaterializer = ActorMaterializer(
    ActorMaterializerSettings(system).withSupervisionStrategy(
      supervisionDecider.getOrElse(RedisConnection.DEFAULT_DECIDER)
    )
  )

  protected val tcpFlow: Flow[ByteString, ByteString, NotUsed] =
    RestartFlow.withBackoff(minBackoff, maxBackoff, randomFactor, maxRestarts) { () =>
      Tcp()
        .outgoingConnection(remoteAddress, localAddress, options, halfClose, connectTimeout, idleTimeout)
    }

  protected val connectionFlow: Flow[RequestContext, ResponseContext, NotUsed] =
    Flow.fromGraph(GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._
      val requestFlow = b.add(
        Flow[RequestContext]
          .map { rc =>
            log.debug(s"con_id = {}: request = [{}]", id, rc.commandRequestString)
            (ByteString.fromString(rc.commandRequest.asString + "\r\n"), rc)
          }
      )
      val responseFlow = b.add(Flow[(ByteString, RequestContext)].map {
        case (byteString, requestContext) =>
          log.debug(s"con_id = {}: response = [{}]", id, byteString.utf8String)
          ResponseContext(byteString, requestContext, ZonedDateTime.now())
      })
      val unzip = b.add(Unzip[ByteString, RequestContext]())
      val zip   = b.add(Zip[ByteString, RequestContext]())
      requestFlow.out ~> unzip.in
      unzip.out0 ~> tcpFlow ~> zip.in0
      unzip.out1 ~> zip.in1
      zip.out ~> responseFlow.in
      FlowShape(requestFlow.in, responseFlow.out)
    })

  protected val (requestQueue: SourceQueueWithComplete[RequestContext], killSwitch: UniqueKillSwitch) = Source
    .queue[RequestContext](requestBufferSize, overflowStrategy)
    .via(connectionFlow)
    .map { responseContext =>
      log.debug(s"con_id = {}, req_id = {}, command = {}: parse",
                id,
                responseContext.commandRequestId,
                responseContext.commandRequestString)
      val result = responseContext.parseResponse
      responseContext.completePromise(result.toTry)
    }
    .viaMat(KillSwitches.single)(Keep.both)
    .toMat(Sink.ignore)(Keep.left)
    .run()

  def shutdown(): Unit = killSwitch.shutdown()

  def toFlow[C <: CommandRequest](parallelism: Int = 1)(implicit scheduler: Scheduler): Flow[C, C#Response, NotUsed] =
    Flow[C].mapAsync(parallelism) { cmd =>
      send(cmd).runAsync
    }

  def send[C <: CommandRequest](cmd: C): Task[cmd.Response] = Task.deferFutureAction { implicit ec =>
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

}
