package com.github.j5ik2o.reactive.redis

import java.net.InetSocketAddress
import java.time.ZonedDateTime
import java.util.UUID

import akka.NotUsed
import akka.actor.{ ActorRef, Props }
import akka.event.Logging
import akka.io.Inet.SocketOption
import akka.stream.actor.ActorSubscriberMessage.OnNext
import akka.stream.actor._
import akka.stream.scaladsl.Tcp.OutgoingConnection
import akka.stream.scaladsl.{ Flow, GraphDSL, Keep, Sink, Source, Tcp, Unzip, Zip }
import akka.stream.{ ActorMaterializer, Attributes, FlowShape }
import akka.util.ByteString
import com.github.j5ik2o.reactive.redis.Protocol._
import com.github.j5ik2o.reactive.redis.TransactionOperations._

import scala.annotation.tailrec
import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration.Duration

object ConnectionActor {

  def props(
      responder: ActorRef,
      remoteAddress: InetSocketAddress,
      localAddress: Option[InetSocketAddress],
      options: immutable.Traversable[SocketOption],
      halfClose: Boolean,
      connectTimeout: Duration,
      idleTimeout: Duration,
      maxRequestCount: Int = 50
  ): Props =
    Props(
      new ConnectionActor(
        responder,
        remoteAddress,
        localAddress,
        options,
        halfClose,
        connectTimeout,
        idleTimeout,
        maxRequestCount
      )
    )

}

class ConnectionActor(
    responder: ActorRef,
    remoteAddress: InetSocketAddress,
    localAddress: Option[InetSocketAddress],
    options: immutable.Traversable[SocketOption],
    halfClose: Boolean,
    connectTimeout: Duration,
    idleTimeout: Duration,
    maxRequestCount: Int = 50
) extends ActorPublisher[RequestContext]
    with ActorSubscriber {

  val log = Logging(context.system, this)

  private var requestContexts = Map.empty[UUID, RequestContext]

  implicit val as  = context.system
  implicit val mat = ActorMaterializer()

  private val requestsInTransaction: collection.mutable.ArrayBuffer[SimpleRequest] =
    collection.mutable.ArrayBuffer.empty

  private val tcpFlow: Flow[ByteString, ByteString, Future[OutgoingConnection]] =
    Tcp().outgoingConnection(remoteAddress, localAddress, options, halfClose, connectTimeout, idleTimeout)

  private val connectionFlow: Flow[RequestContext, ResponseContext, NotUsed] =
    Flow.fromGraph(GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._
      val requestFlow = b.add(
        Flow[RequestContext]
          .map { rc =>
            log.debug("request: {}", rc.request.message)
            (ByteString.fromString(rc.request.message + "\r\n"), rc)
          }
      )
      val responseFlow = b.add(Flow[(ByteString, RequestContext)].map {
        case (byteString, requestContext) =>
          log.debug("response: {}", byteString.utf8String)
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

  private def parseSimpleResponse(
      request: SimpleRequest,
      responseAsByteString: ByteString
  ): Response = {
    request.responseFactory
      .createResponseFromString(request.id, responseAsByteString.utf8String)
      ._1
  }

  private def parseDiscardResponse(
      request: DiscardRequest,
      responseAsByteString: ByteString
  ): DiscardResponse = {
    request.responseFactory
      .createResponseFromString(request.id, responseAsByteString.utf8String)
      ._1
      .asInstanceOf[DiscardResponse]
  }

  private def parseExecResponse(
      request: TransactionRequest,
      responseAsByteString: ByteString,
      responseFactories: Vector[SimpleResponseFactory]
  ): ExecResponse = {
    request.responseFactory
      .createResponseFromString(request.id, responseAsByteString.utf8String, responseFactories)
      ._1
      .asInstanceOf[ExecResponse]
  }

  implicit val adapter = Logging(context.system, "customLogger")
  Source
    .fromPublisher(ActorPublisher(self))
    .log("custom")
    .withAttributes(Attributes.logLevels(onElement = Logging.DebugLevel))
    .via(connectionFlow)
    .toMat(Sink.fromSubscriber(ActorSubscriber[ResponseContext](self)))(Keep.left)
    .run()

  override val requestStrategy = new MaxInFlightRequestStrategy(max = maxRequestCount) {
    override def inFlightInternally: Int = requestContexts.size
  }

  override def receive: Receive = {
    case OnNext(ResponseContext(res, rc @ RequestContext(replyTo, req, _))) =>
      val response = req match {
        case msg: ExecRequest =>
          val requestFactories = requestsInTransaction.map(_.responseFactory).toVector
          requestsInTransaction.clear()
          TransactionExecCompleted(replyTo, msg, parseExecResponse(msg, res, requestFactories))
        case msg: DiscardRequest =>
          requestsInTransaction.clear()
          TransactionDiscardCompleted(replyTo, msg, parseDiscardResponse(msg, res))
        case InTransactionRequest(msg: SimpleRequest) =>
          SimpleRequestComplete(replyTo, msg, parseSimpleResponse(msg, res))
        case msg: SimpleRequest =>
          SimpleRequestComplete(replyTo, msg, parseSimpleResponse(msg, res))
      }
      responder ! response
      requestContexts -= rc.id
    case request: Request =>
      if (requestContexts.isEmpty && totalDemand > 0) {
        val rc = RequestContext(sender(), request, ZonedDateTime.now())
        addInTransactionRequest(rc)
        onNext(rc)
      } else {
        val rc = RequestContext(sender(), request, ZonedDateTime.now())
        addInTransactionRequest(rc)
        requestContexts += (rc.id -> rc)
        deliverBuf()
      }
    case ActorPublisherMessage.Request(_) =>
      deliverBuf()
    case ActorPublisherMessage.Cancel =>
      context.stop(self)

  }

  private def addInTransactionRequest(rc: RequestContext) = {
    rc.request match {
      case InTransactionRequest(req) =>
        requestsInTransaction.append(req)
      case _ =>
    }
  }

  @tailrec final def deliverBuf(): Unit =
    if (totalDemand > 0) {
      if (totalDemand <= Int.MaxValue) {
        val (use, keep) = requestContexts.splitAt(totalDemand.toInt)
        requestContexts = keep
        use.foreach {
          case (_, e) =>
            onNext(e)
        }
      } else {
        val (use, keep) = requestContexts.splitAt(Int.MaxValue)
        requestContexts = keep
        use.foreach {
          case (_, e) =>
            onNext(e)
        }
        deliverBuf()
      }
    }

}
