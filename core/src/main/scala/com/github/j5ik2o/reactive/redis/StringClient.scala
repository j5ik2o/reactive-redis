package com.github.j5ik2o.reactive.redis

import java.net.InetSocketAddress

import akka.actor.{ Actor, ActorLogging, Props }
import akka.pattern.pipe
import akka.stream.scaladsl.Tcp.OutgoingConnection
import akka.stream.scaladsl.{ Flow, Tcp }
import akka.util.ByteString
import com.github.j5ik2o.reactive.redis.CommandResponseParser.{ ErrorExpr, SimpleExpr, StringExpr, StringOptExpr }
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

      case class GetRequest(key: String) extends CommandRequest {
        class Parser extends CommandResponseParser[ResponseType] {
          override protected val responseParser: Parser[GetResponse] =  {
            bulkStringWithCrLf ^^ {
              case StringOptExpr(s) =>
                responseAsSucceeded(s)
            }
          }
        }
        override def encodeAsString: String = s"GET $key"

        override type ResultType = Option[String]
        override type ResponseType = GetResponse

        override def responseAsSucceeded(arguments: Option[String]): GetResponse =
          GetSucceeded(arguments)

        override def responseAsFailed(ex: Exception): GetResponse =
          GetFailure(ex)

        override val parser: CommandResponseParser[GetResponse] = new Parser()
      }

      sealed trait GetResponse extends CommandResponse

      case class GetSucceeded(value: Option[String]) extends GetResponse

      case class GetFailure(ex: Exception) extends GetResponse

      // ---

      case class GetSetRequest(key: String, value: String) extends CommandRequest {
        class Parser extends CommandResponseParser[ResponseType] {
          override protected val responseParser: Parser[GetSetResponse] = {
            bulkStringWithCrLf ^^ {
              case StringOptExpr(s) =>
                responseAsSucceeded(s)
            }
          }
        }
        override def encodeAsString: String = s"GETSET $key $value"

        override type ResultType = Option[String]
        override type ResponseType = GetSetResponse

        override def responseAsSucceeded(arguments: Option[String]): GetSetResponse =
          GetSetSucceeded(arguments)

        override def responseAsFailed(ex: Exception): GetSetResponse =
          GetSetFailure(ex)

        override val parser: CommandResponseParser[GetSetResponse] = new Parser()
      }

      sealed trait GetSetResponse extends CommandResponse

      case class GetSetSucceeded(value: Option[String]) extends GetSetResponse

      case class GetSetFailure(ex: Exception) extends GetSetResponse

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
      run(get(key)).pipeTo(sender())
    case GetSetRequest(key, value) =>
      run(getSet(key, value)).pipeTo(sender())
  }

  override def receive: Receive = handleConnection orElse handleKeys orElse handleServer orElse default

}
