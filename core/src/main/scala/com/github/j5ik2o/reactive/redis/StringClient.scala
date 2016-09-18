package com.github.j5ik2o.reactive.redis

import java.net.InetSocketAddress

import akka.actor.{ Actor, ActorLogging, Props }
import akka.pattern.pipe
import akka.stream.scaladsl.Tcp.OutgoingConnection
import akka.stream.scaladsl.{ Flow, Tcp }
import akka.util.ByteString
import com.github.j5ik2o.reactive.redis.CommonProtocol._
import com.github.j5ik2o.reactive.redis.StringClient.Protocol.String._

import scala.concurrent.Future

object StringClient {

  def props(address: InetSocketAddress) = Props(new StringClient(address))

  object Protocol {

    object String {

      // ---
      case class SetRequest(key: String, value: String)

      case object SetSucceeded

      case class SetFailure(ex: Exception)

      // ---

      case class GetRequest(key: String)

      case class GetSucceeded(value: Option[String])

      case class GetFailure(ex: Exception)

      // ---

      case class GetSetRequest(key: String, value: String)

      case class GetSetSucceeded(value: String)

      case class GetSetFailure(ex: Exception)

    }

  }

}


class StringClient(address: InetSocketAddress)
  extends Actor with ActorLogging
    with StringStreamApi with CommonStreamActor {

  log.info(address.toString)

  import context.dispatcher

  val connection: Flow[ByteString, ByteString, Future[OutgoingConnection]] =
    Tcp().outgoingConnection(address)

  val default: Receive = {
    case SetRequest(key, value) =>
      set(key, value).map { _ =>
        SetSucceeded
      }.recover { case ex: Exception =>
        SetFailure(ex)
      }.pipeTo(sender())
    case GetRequest(key) =>
      get(key).map { v =>
        GetSucceeded(v)
      }.recover { case ex: Exception =>
        GetFailure(ex)
      }.pipeTo(sender())
    case GetSetRequest(key, value) =>
      getSet(key, value).map { v =>
        GetSetSucceeded(v)
      }.recover { case ex: Exception =>
        GetSetFailure(ex)
      }.pipeTo(sender())
  }

  override def receive: Receive = handleBase orElse default

}
