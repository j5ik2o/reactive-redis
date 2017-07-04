package com.github.j5ik2o.reactive.redis

import java.net.InetSocketAddress
import java.text.ParseException
import java.time.ZonedDateTime
import java.util.UUID

import akka.NotUsed
import akka.actor.{ Actor, ActorLogging, ActorRef, Props }
import akka.io.Inet.SocketOption
import akka.stream.actor.ActorSubscriberMessage.OnNext
import akka.stream.actor._
import akka.stream.scaladsl.Tcp.OutgoingConnection
import akka.stream.scaladsl.{ Flow, GraphDSL, Keep, Sink, Source, Tcp, Unzip, Zip }
import akka.stream.{ ActorMaterializer, FlowShape }
import akka.util.ByteString
import com.github.j5ik2o.reactive.redis.CommandResponseParser.{ ErrorExpr, Expr, SimpleExpr }
import com.github.j5ik2o.reactive.redis.Protocol._
import com.github.j5ik2o.reactive.redis.StringsOperations.AppendRequest.{ logger, parseResponseToExprWithInput, Input }
import com.github.j5ik2o.reactive.redis.TransactionOperations._
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration.Duration
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

  type Handler = PartialFunction[(Expr, Input), (Response, Input)]

  val logger = LoggerFactory.getLogger(classOf[SimpleResponseFactory])

  def createResponseFromString(requestId: UUID, message: String): (Response, Reader[Char]) =
    createResponseFromReader(requestId, new CharSequenceReader(message))

  def createResponseFromReader(requestId: UUID, message: Reader[Char]): (Response, Reader[Char]) = {
    val result = parseResponseToExprWithInput(message)
    receive(requestId)(result)
  }

  def receive(requestId: UUID): Handler

}

trait TransactionResponseFactory extends CommandResponseParserSupport {

  val logger = LoggerFactory.getLogger(classOf[TransactionResponseFactory])

  def createResponseFromString(requestId: UUID,
                               message: String,
                               responseFactories: Vector[SimpleResponseFactory]): (Response, Reader[Char]) =
    createResponseFromReader(requestId, new CharSequenceReader(message), responseFactories)

  def createResponseFromReader(requestId: UUID,
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
      new RedisActor(id, remoteAddress, localAddress, options, halfClose, connectTimeout, idleTimeout, maxRequestCount)
    )

}

private[redis] object Protocol {

  case class SimpleRequestComplete(replyTo: ActorRef, request: SimpleRequest, response: Response)

  case class InTransactionRequest(request: SimpleRequest) extends Request {
    override val id: UUID        = request.id
    override val message: String = request.message
  }

  case class TransactionStart(request: SimpleRequest)

  case class TransactionExecCompleted(replyTo: ActorRef, request: ExecRequest, response: ExecResponse)

  case class TransactionDiscardCompleted(replyTo: ActorRef, request: DiscardRequest, response: DiscardResponse)

  case class RequestContext(sender: ActorRef, request: Request, requestAt: ZonedDateTime) {
    val id: UUID = request.id
  }

  case class ResponseContext(byteString: ByteString, requestContext: RequestContext)

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
