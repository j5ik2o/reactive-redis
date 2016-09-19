package com.github.j5ik2o.reactive.redis.connection

import com.github.j5ik2o.reactive.redis.CommandResponseParser.{ ErrorExpr, SimpleExpr }
import com.github.j5ik2o.reactive.redis._

object ConnectionProtocol {

  // --- AUTH

  // --- ECHO

  // --- PING

  // --- QUIT
  case object QuitRequest extends CommandRequest {
    import CommandResponseParser._
    class Parser extends CommandResponseParser[ResponseType] {
      override protected val responseParser: Parser[QuitResponse] = {
        simpleWithCrLfOrErrorWithCrLf ^^ {
          case ErrorExpr(msg) =>
            responseAsFailed(RedisIOException(Some(msg)))
          case SimpleExpr(msg) =>
            responseAsSucceeded(())
          case _ =>
            sys.error("It's unexpected.")
        }
      }
    }

    override type ResultType = Unit

    override type ResponseType = QuitResponse

    override def encodeAsString: String = "QUIT"

    override def responseAsSucceeded(arguments: Unit): QuitResponse = QuitSucceeded

    override def responseAsFailed(ex: Exception): QuitResponse = QuitFailed(ex)

    override val parser: CommandResponseParser[QuitResponse] = new Parser()
  }


  sealed trait QuitResponse extends CommandResponse

  case object QuitSucceeded extends QuitResponse

  case class QuitFailed(ex: Exception) extends QuitResponse

  // --- SELECT
  case class SelectRequest(index: Int) extends CommandRequest {

    class Parser extends CommandResponseParser[ResponseType] {
      override protected val responseParser: Parser[SelectResponse] = {
        simpleWithCrLfOrErrorWithCrLf ^^ {
          case ErrorExpr(msg) =>
            responseAsFailed(RedisIOException(Some(msg)))
          case SimpleExpr(msg) =>
            responseAsSucceeded(())
          case _ =>
            sys.error("It's unexpected.")
        }
      }
    }

    override type ResultType = Unit

    override type ResponseType = SelectResponse

    override def encodeAsString: String = s"SELECT $index"

    override def responseAsSucceeded(arguments: Unit): SelectResponse = SelectSucceeded

    override def responseAsFailed(ex: Exception): SelectResponse = SelectFailure(ex)

    override lazy val parser: CommandResponseParser[SelectResponse] = new Parser
  }

  sealed trait SelectResponse extends CommandResponse

  case object SelectSucceeded extends SelectResponse

  case class SelectFailure(ex: Exception) extends SelectResponse

}
