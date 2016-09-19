package com.github.j5ik2o.reactive.redis

import java.net.InetSocketAddress

import akka.actor.{ Actor, ActorLogging, Props }
import akka.pattern.pipe
import akka.stream.scaladsl.Tcp.OutgoingConnection
import akka.stream.scaladsl.{ Flow, Tcp }
import akka.util.ByteString
import com.github.j5ik2o.reactive.redis.CommandResponseParser.{ ErrorExpr, SimpleExpr }
import com.github.j5ik2o.reactive.redis.StringClient.Protocol.String._
import com.github.j5ik2o.reactive.redis.connection.ConnectionActorAPI
import com.github.j5ik2o.reactive.redis.keys.KeysActorAPI
import com.github.j5ik2o.reactive.redis.server.ServerActorAPI

import scala.concurrent.Future

object StringClient {

  def props(address: InetSocketAddress) = Props(new StringClient(address))

  object Protocol {

    object String {

      // ---
      case class SetRequest(key: String, value: String) extends CommandRequest {
        class Parser extends CommandResponseParser[ResponseType] {
          override protected val responseParser: Parser[SetResponse] = {
            simpleWithCrLfOrErrorWithCrLf ^^ {
              case ErrorExpr(msg) =>
                responseAsFailed(RedisIOException(Some(msg)))
              case SimpleExpr(msg) =>
                responseAsSucceeded(())
              case _ =>
                sys.error("it's unexpected")
            }
          }
        }
        override def encodeAsString: String = s"SET $key $value"

        override type ResultType = Unit
        override type ResponseType = SetResponse

        override def responseAsSucceeded(arguments: Unit): SetResponse =
          SetSucceeded

        override def responseAsFailed(ex: Exception): SetResponse =
          SetFailed(ex)

        override val parser: CommandResponseParser[SetResponse] = new Parser
      }

      sealed trait SetResponse extends CommandResponse

      case object SetSucceeded extends SetResponse

      case class SetFailed(ex: Exception) extends SetResponse

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
    with StringStreamAPI with ConnectionActorAPI with KeysActorAPI with ServerActorAPI {

  log.info(address.toString)

  import context.dispatcher

  val connection: Flow[ByteString, ByteString, Future[OutgoingConnection]] =
    Tcp().outgoingConnection(address)

  val default: Receive = {
    case SetRequest(key, value) =>
      run(set(key, value)).pipeTo(sender())
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

  override def receive: Receive = handleConnection orElse handleKeys orElse handleServer orElse default

}
