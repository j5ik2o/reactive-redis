package com.github.j5ik2o.reactive.redis

import java.time.ZonedDateTime
import java.util.UUID

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl.{ Flow, GraphDSL, Keep, RestartFlow, Sink, Source, Tcp, Unzip, Zip }
import akka.util.ByteString
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse }
import monix.eval.Task
import monix.execution.Scheduler

import scala.concurrent.{ Future, Promise }

class RedisConnection(connectionConfig: ConnectionConfig)(implicit system: ActorSystem) {

  val id: UUID = UUID.randomUUID()

  import connectionConfig._
  import connectionConfig.backoffConfig._

  private val log = system.log

  private implicit val mat = ActorMaterializer()

  private val tcpFlow = RestartFlow.withBackoff(minBackoff, maxBackoff, randomFactor, maxRestarts) { () =>
    Tcp()
      .outgoingConnection(remoteAddress, localAddress, options, halfClose, connectTimeout, idleTimeout)
  }

  private val connectionFlow: Flow[RequestContext, ResponseContext, NotUsed] =
    Flow.fromGraph(GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._
      val requestFlow = b.add(
        Flow[RequestContext]
          .map { rc =>
            log.debug("request = {}", rc.commandRequest.asString)
            (ByteString.fromString(rc.commandRequest.asString + "\r\n"), rc)
          }
      )
      val responseFlow = b.add(Flow[(ByteString, RequestContext)].map {
        case (byteString, requestContext) =>
          log.debug("response = {}", byteString.utf8String)
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

  private val (requestQueue, killSwitch) = Source
    .queue[RequestContext](requestBufferSize, OverflowStrategy.dropNew)
    .via(connectionFlow)
    .map { responseContext =>
      // TODO: 通常のリクエストの場合
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
