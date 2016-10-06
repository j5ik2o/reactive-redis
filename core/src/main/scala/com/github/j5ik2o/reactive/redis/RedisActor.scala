package com.github.j5ik2o.reactive.redis

import java.io.StringReader
import java.net.InetSocketAddress
import java.text.ParseException
import java.time.ZonedDateTime
import java.util.UUID

import akka.actor.Actor.Receive
import akka.{ Done, NotUsed }
import akka.actor.{ Actor, ActorLogging, ActorRef, Props }
import akka.io.Inet.SocketOption
import akka.stream.scaladsl.Tcp.OutgoingConnection
import akka.stream.scaladsl.{ Flow, GraphDSL, Keep, Sink, Source, Tcp, Unzip, Zip }
import akka.stream.{ ActorMaterializer, FlowShape, OverflowStrategy }
import akka.util.ByteString
import com.github.j5ik2o.reactive.redis.CommandResponseParser._
import com.github.j5ik2o.reactive.redis.TransactionOperations.{ DiscardRequest, ExecRequest, MultiRequest }
import org.slf4j.LoggerFactory

import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.concurrent.duration._
import scala.util.parsing.input.{ CharSequenceReader, Reader }

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

  def createResponseFromString(requestId: UUID, message: String, responseFactories: Vector[SimpleResponseFactory]): (Response, Reader[Char]) =
    createResponseFromReader(requestId, new CharSequenceReader(message), responseFactories)

  def createResponseFromReader(requestId: UUID, message: Reader[Char], responseFactories: Vector[SimpleResponseFactory]): (Response, Reader[Char])

}

// ---

object RedisActor {

  def name(id: UUID): String = s"redis-actor-$id"

  def props(id: UUID, host: String, port: Int): Props =
    props(id, new InetSocketAddress(host, port))

  def props(id: UUID,
             remoteAddress:  InetSocketAddress,
             localAddress:   Option[InetSocketAddress]           = None,
             options:        immutable.Traversable[SocketOption] = Nil,
             halfClose:      Boolean                             = true,
             connectTimeout: Duration                            = Duration.Inf,
             idleTimeout:    Duration                            = Duration.Inf
           ): Props =
    Props(new RedisActor(id, remoteAddress, localAddress, options, halfClose, connectTimeout, idleTimeout))

  private case class ActorRefDesc(actorRef: ActorRef, createAt: ZonedDateTime)

  private case class SimpleRequestComplete(request: SimpleRequest, responseAsByteString: ByteString)

  private case class TransactionRequestComplete(request: TransactionRequest, responseAsByteString: ByteString)

}

class RedisActor(
                id: UUID,
    remoteAddress:  InetSocketAddress,
    localAddress:   Option[InetSocketAddress],
    options:        immutable.Traversable[SocketOption],
    halfClose:      Boolean,
    connectTimeout: Duration,
    idleTimeout:    Duration
) extends Actor with ActorLogging {

  import RedisActor._

  implicit val as = context.system
  implicit val mat = ActorMaterializer()

  private val clients: collection.mutable.Map[UUID, ActorRefDesc] = collection.mutable.Map.empty[UUID, ActorRefDesc]

  private val requestsInTransaction: collection.mutable.ArrayBuffer[SimpleRequest] = collection.mutable.ArrayBuffer.empty

  private val tcpFlow: Flow[ByteString, ByteString, Future[OutgoingConnection]] =
    Tcp().outgoingConnection(remoteAddress, localAddress, options, halfClose, connectTimeout, idleTimeout)

  private val sourceActorRef: Source[Request, ActorRef] = Source.actorRef[Request](Int.MaxValue, OverflowStrategy.fail)

  private val connectionFlow: Flow[Request, (ByteString, Request), NotUsed] = Flow.fromGraph(GraphDSL.create() { implicit b =>
    import GraphDSL.Implicits._
    val requestFlow = b.add(Flow[Request].map { e => (ByteString.fromString(e.message + "\r\n"), e) })
    val unzip = b.add(Unzip[ByteString, Request]())
    val zip = b.add(Zip[ByteString, Request]())
    requestFlow.out ~> unzip.in
    unzip.out0 ~> tcpFlow ~> zip.in0
    unzip.out1 ~> zip.in1
    FlowShape(requestFlow.in, zip.out)
  })

  private val sink: Sink[(ByteString, Request), Future[Done]] = Sink.foreach {
    case (res, req) =>
      req match {
        case msg: ExecRequest =>
          self ! TransactionRequestComplete(msg, res)
        case msg: DiscardRequest =>
          self ! SimpleRequestComplete(msg, res)
        case msg: SimpleRequest =>
          self ! SimpleRequestComplete(msg, res)
      }
  }

  private val redisClientRef = sourceActorRef.via(connectionFlow).toMat(sink)(Keep.left).run()

  private def sendCommandRequest(request: Request): Unit = {
    log.debug("receive command request = {}", request)
    clients.put(request.id, ActorRefDesc(sender(), ZonedDateTime.now()))
    redisClientRef ! request
  }

  private def replyCommandResponse(request: Request, response: Response): Option[ActorRefDesc] = {
    log.debug("send command response = {}", response)
    clients(request.id).actorRef ! response
    clients.remove(request.id)
  }

  private def createSimpleResponseFromByteString(
    request:              SimpleRequest,
    responseAsByteString: ByteString
  ): Response = {
    request.responseFactory.createResponseFromString(request.id, responseAsByteString.utf8String)._1
  }

  private def createTransactionResponseFromByteString(
    request:              TransactionRequest,
    responseAsByteString: ByteString,
    responseFactories:    Vector[SimpleResponseFactory]
  ): Response = {
    request.responseFactory.createResponseFromString(request.id, responseAsByteString.utf8String, responseFactories)._1
  }

  override def receive: Receive = default

  private def default: Receive = {
    case request: MultiRequest =>
      context.become(inTransaction)
      sendCommandRequest(request)
    case request: Request =>
      sendCommandRequest(request)
    case SimpleRequestComplete(request, responseAsByteString) =>
      val response = createSimpleResponseFromByteString(request, responseAsByteString)
      replyCommandResponse(request, response)
  }

  private def inTransaction: Receive = {
    case SimpleRequestComplete(request: DiscardRequest, responseAsByteString) =>
      requestsInTransaction.clear()
      context.become(default)
      val response = createSimpleResponseFromByteString(request, responseAsByteString)
      replyCommandResponse(request, response)
    case TransactionRequestComplete(request: ExecRequest, responseAsByteString) =>
      val requestFactories = requestsInTransaction.map(_.responseFactory).toVector
      requestsInTransaction.clear()
      context.become(default)
      val response = createTransactionResponseFromByteString(request, responseAsByteString, requestFactories)
      replyCommandResponse(request, response)
    case SimpleRequestComplete(request, responseAsByteString) =>
      val response = createSimpleResponseFromByteString(request, responseAsByteString)
      replyCommandResponse(request, response)
    case request: ExecRequest =>
      sendCommandRequest(request)
    case request: DiscardRequest =>
      sendCommandRequest(request)
    case request: SimpleRequest =>
      requestsInTransaction.append(request)
      sendCommandRequest(request)
  }

}
