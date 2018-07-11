package com.github.j5ik2o.reactive.redis

import java.net.InetSocketAddress
import java.time.ZonedDateTime
import java.util.UUID

import akka.actor.ActorSystem
import akka.io.Inet.SocketOption
import akka.stream.scaladsl.{
  Flow,
  GraphDSL,
  Keep,
  RestartFlow,
  Sink,
  Source,
  SourceQueueWithComplete,
  Tcp,
  Unzip,
  Zip
}
import akka.stream.{ ActorMaterializer, FlowShape, OverflowStrategy, QueueOfferResult }
import akka.util.ByteString
import akka.{ Done, NotUsed }
import com.github.j5ik2o.reactive.redis.cmd.{ CommandRequest, CommandResponse }
import monix.eval.Task

import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{ Future, Promise }

case class ConnectionConfig(remoteAddress: InetSocketAddress,
                            localAddress: Option[InetSocketAddress] = None,
                            options: immutable.Seq[SocketOption] = immutable.Seq.empty,
                            halfClose: Boolean = false,
                            connectTimeout: Duration = Duration.Inf,
                            idleTimeout: Duration = Duration.Inf,
                            minBackoff: FiniteDuration = 3 seconds,
                            maxBackoff: FiniteDuration = 30 seconds,
                            randomFactor: Double = 0.2,
                            maxRestarts: Int = -1,
                            requestBufferSize: Int = 1024)

class RedisConnection(connectionConfig: ConnectionConfig)(implicit system: ActorSystem) {

  val id = UUID.randomUUID()

  import connectionConfig._

  private val log = system.log

  implicit val mat = ActorMaterializer()

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
            log.info("request = {}", rc.commandRequest.asString)
            (ByteString.fromString(rc.commandRequest.asString + "\r\n"), rc)
          }
      )
      val responseFlow = b.add(Flow[(ByteString, RequestContext)].map {
        case (byteString, requestContext) =>
          log.info("response = {}", byteString.utf8String)
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

  private val requestQueue: SourceQueueWithComplete[RequestContext] = Source
    .queue[RequestContext](requestBufferSize, OverflowStrategy.dropNew)
    .via(connectionFlow)
    .map { responseContext =>
      // TODO: 通常のリクエストの場合
      val r = responseContext.requestContext.commandRequest.parse(responseContext.byteString.utf8String)
      responseContext.requestContext.promise.complete(r.toTry)
      //
    }
    .toMat(Sink.ignore)(Keep.left)
    .run()

  def shutdown(): Future[Done] = requestQueue.watchCompletion()

  def sendCommandRequest[C <: CommandRequest](cmd: C): Task[cmd.Response] = Task.deferFutureAction { implicit ec =>
    val promise = Promise[CommandResponse]()
    requestQueue
      .offer(RequestContext(cmd, promise, ZonedDateTime.now()))
      .flatMap {
        case QueueOfferResult.Enqueued =>
          promise.future.map(_.asInstanceOf[cmd.Response])
        case QueueOfferResult.Failure(t) =>
          Future.failed(HttpRequestSendException("Failed to send request", Some(t)))
        case QueueOfferResult.Dropped =>
          Future.failed(
            HttpRequestSendException(
              s"Failed to send request, the queue buffer was full."
            )
          )
        case QueueOfferResult.QueueClosed =>
          Future.failed(HttpRequestSendException("Failed to send request, the queue was closed"))
      }
  }

}
