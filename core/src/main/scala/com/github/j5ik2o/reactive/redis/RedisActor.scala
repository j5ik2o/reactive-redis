package com.github.j5ik2o.reactive.redis

import java.io.StringReader
import java.net.InetSocketAddress
import java.text.ParseException
import java.time.ZonedDateTime
import java.util.UUID

import akka.{ Done, NotUsed }
import akka.actor.{ Actor, ActorLogging, ActorRef, Props }
import akka.io.Inet.SocketOption
import akka.stream.scaladsl.Tcp.OutgoingConnection
import akka.stream.scaladsl.{ Flow, GraphDSL, Keep, Sink, Source, Tcp, Unzip, Zip }
import akka.stream.{ ActorMaterializer, FlowShape, OverflowStrategy }
import akka.util.ByteString
import com.github.j5ik2o.reactive.redis.CommandResponseParser._
import com.github.j5ik2o.reactive.redis.TransactionOperations.{ ExecRequest, MultiRequest }
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
  val responseFactory: ResponseFactory
}

trait Response {
  val id: UUID
  val requestId: UUID
}

trait ResponseFactory extends CommandResponseParserSupport {
  val logger = LoggerFactory.getLogger(classOf[ResponseFactory])

  def create(requestId: UUID, message: String): (Response, Reader[Char]) =
    create(requestId, new CharSequenceReader(message))

  def create(requestId: UUID, message: Reader[Char]): (Response, Reader[Char])

}

trait TransactionRequest extends Request {
  val responseFactory: TransactionResponseFactory
}

trait TransactionResponseFactory extends CommandResponseParserSupport {

  def createFromString(requestId: UUID, message: String, responseFactories: Vector[ResponseFactory]): (Response, Reader[Char]) =
    create(requestId, new CharSequenceReader(message), responseFactories)

  def create(requestId: UUID, message: Reader[Char], responseFactories: Vector[ResponseFactory]): (Response, Reader[Char])

}

// ---

private case class ActorRefDesc(actorRef: ActorRef, createAt: ZonedDateTime)

private case object CleanClients

class RedisActor(remoteAddress: InetSocketAddress,
                 localAddress: Option[InetSocketAddress],
                 options: immutable.Traversable[SocketOption],
                 halfClose: Boolean,
                 connectTimeout: Duration,
                 idleTimeout: Duration) extends Actor with ActorLogging {
  implicit val as = context.system
  implicit val mat = ActorMaterializer()

  //context.system.scheduler.schedule(0 seconds, 1 seconds, self, CleanClients)

  private val clients: collection.mutable.Map[UUID, ActorRefDesc] = collection.mutable.Map.empty[UUID, ActorRefDesc]

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

  val requestsOfTransaction: collection.mutable.ArrayBuffer[SimpleRequest] = collection.mutable.ArrayBuffer.empty

  private val sink: Sink[(ByteString, Request), Future[Done]] = Sink.foreach { case (res, req) =>
    val response = req match {
      case msg: ExecRequest =>
        val result = msg.responseFactory.createFromString(req.id, res.utf8String, requestsOfTransaction.map(_.responseFactory).toVector)._1
        requestsOfTransaction.clear()
        result
      case msg: SimpleRequest =>
        msg.responseFactory.create(req.id, res.utf8String)._1
    }
    log.debug("send command response = {}", response)
    clients(req.id).actorRef ! response
    clients.remove(req.id)
  }

  private val actorRef = sourceActorRef.via(connectionFlow).toMat(sink)(Keep.left).run()

  private var transcation = false

  override def receive: Receive = {
    case msg: Request =>
      log.debug("receive command request = {}", msg)
      clients.put(msg.id, ActorRefDesc(sender(), ZonedDateTime.now()))
      msg match {
        case cmd: MultiRequest =>
          transcation = true
        case cmd: ExecRequest =>
          transcation = false
        case cmd: SimpleRequest if transcation =>
          requestsOfTransaction.append(cmd)
        case _  =>
      }
      actorRef ! msg
    case CleanClients =>

  }

}
