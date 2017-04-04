package com.github.j5ik2o.reactive.redis

import java.net.InetSocketAddress
import java.time.ZonedDateTime
import java.util.UUID

import akka.NotUsed
import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.io.Inet.SocketOption
import akka.stream.actor.ActorSubscriberMessage.OnNext
import akka.stream.actor._
import akka.stream.scaladsl.Tcp.OutgoingConnection
import akka.stream.scaladsl.{Flow, GraphDSL, Keep, Sink, Source, Tcp, Unzip, Zip}
import akka.stream.{ActorMaterializer, FlowShape}
import akka.util.ByteString
import com.github.j5ik2o.reactive.redis.Protocol._
import com.github.j5ik2o.reactive.redis.TransactionOperations._
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.util.parsing.input.{CharSequenceReader, Reader}

trait Request {
  val id: UUID
  val message: String

}

trait SimpleRequest extends Request {
  val responseFactory: SimpleResponseFactory
}

trait TransactionRequest extends Request {
  val responseFactory: TransactionResponseFactory
}

trait Response {
  val id: UUID
  val requestId: UUID
}

trait SimpleResponseFactory extends CommandResponseParserSupport {

  val logger = LoggerFactory.getLogger(classOf[SimpleResponseFactory])

  def createResponseFromString(requestId: UUID, message: String): (Response, Reader[Char]) =
    createResponseFromReader(requestId, new CharSequenceReader(message))

  def createResponseFromReader(requestId: UUID, message: Reader[Char]): (Response, Reader[Char])

}

trait TransactionResponseFactory extends CommandResponseParserSupport {

  val logger = LoggerFactory.getLogger(classOf[TransactionResponseFactory])

  def createResponseFromString(
      requestId: UUID,
      message: String,
      responseFactories: Vector[SimpleResponseFactory]): (Response, Reader[Char]) =
    createResponseFromReader(requestId, new CharSequenceReader(message), responseFactories)

  def createResponseFromReader(
      requestId: UUID,
      message: Reader[Char],
      responseFactories: Vector[SimpleResponseFactory]): (Response, Reader[Char])

}

// ---

object RedisActor {

  def name(id: UUID): String = s"redis-actor-$id"

  def props(id: UUID, host: String, port: Int): Props =
    props(id, new InetSocketAddress(host, port))

  def props(
      id: UUID,
      remoteAddress: InetSocketAddress,
      localAddress: Option[InetSocketAddress] = None,
      options: immutable.Traversable[SocketOption] = Nil,
      halfClose: Boolean = true,
      connectTimeout: Duration = Duration.Inf,
      idleTimeout: Duration = Duration.Inf,
      maxRequestCount: Int = 50
  ): Props =
    Props(
      new RedisActor(id,
                     remoteAddress,
                     localAddress,
                     options,
                     halfClose,
                     connectTimeout,
                     idleTimeout,
                     maxRequestCount))

}

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
      ))

}

private[redis] object Protocol {

  case class SimpleRequestComplete(replyTo: ActorRef, request: SimpleRequest, response: Response)

  case class InTransactionRequest(request: SimpleRequest) extends Request {
    override val id: UUID        = request.id
    override val message: String = request.message
  }

  case class TransactionStart(request: SimpleRequest)

  case class TransactionExecCompleted(replyTo: ActorRef,
                                      request: ExecRequest,
                                      response: ExecResponse)

  case class TransactionDiscardCompleted(replyTo: ActorRef,
                                         request: DiscardRequest,
                                         response: DiscardResponse)

  case class RequestContext(sender: ActorRef, request: Request, requestAt: ZonedDateTime) {
    val id: UUID = request.id
  }

  case class ResponseContext(byteString: ByteString, requestContext: RequestContext)

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

  private var requestContexts = Map.empty[UUID, RequestContext]

  implicit val as  = context.system
  implicit val mat = ActorMaterializer()

  private val requestsInTransaction: collection.mutable.ArrayBuffer[SimpleRequest] =
    collection.mutable.ArrayBuffer.empty

  private val tcpFlow: Flow[ByteString, ByteString, Future[OutgoingConnection]] =
    Tcp().outgoingConnection(remoteAddress,
                             localAddress,
                             options,
                             halfClose,
                             connectTimeout,
                             idleTimeout)

  private val connectionFlow: Flow[RequestContext, ResponseContext, NotUsed] =
    Flow.fromGraph(GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._
      val requestFlow = b.add(Flow[RequestContext].map { rc =>
        (ByteString.fromString(rc.request.message + "\r\n"), rc)
      })
      val responseFlow = b.add(Flow[(ByteString, RequestContext)].map {
        case (byteString, requestContext) =>
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

  Source
    .fromPublisher(ActorPublisher(self))
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

class RedisActor(
    id: UUID,
    remoteAddress: InetSocketAddress,
    localAddress: Option[InetSocketAddress],
    options: immutable.Traversable[SocketOption],
    halfClose: Boolean,
    connectTimeout: Duration,
    idleTimeout: Duration,
    maxRequestCount: Int = 50
) extends Actor
    with ActorLogging {

  private val connectionActorRef = context.actorOf(
    ConnectionActor.props(
      self,
      remoteAddress,
      localAddress,
      options,
      halfClose,
      connectTimeout,
      idleTimeout,
      maxRequestCount
    ),
    "connection"
  )

  override def receive: Receive = default

  private def default: Receive = {
    case request: MultiRequest =>
      context.become(inTransaction)
      connectionActorRef forward request
    case request: Request =>
      connectionActorRef forward request
    case SimpleRequestComplete(replyTo, _, response) =>
      replyTo ! response
  }

  private def inTransaction: Receive = {
    case TransactionDiscardCompleted(replyTo, _, response) =>
      context.become(default)
      replyTo ! response
    case TransactionExecCompleted(replyTo, _, response) =>
      context.become(default)
      replyTo ! response
    case SimpleRequestComplete(replyTo, _, response) =>
      replyTo ! response
    case request: ExecRequest =>
      connectionActorRef forward request
    case request: DiscardRequest =>
      connectionActorRef forward request
    case request: SimpleRequest =>
      connectionActorRef forward InTransactionRequest(request)
  }

}
