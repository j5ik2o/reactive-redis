package com.github.j5ik2o.reactive.redis.connection

import com.github.j5ik2o.reactive.redis.CommandResponseParser.{ ErrorExpr, SimpleExpr }
import com.github.j5ik2o.reactive.redis._

object ConnectionProtocol {
  import BaseProtocol._

  // --- AUTH ---------------------------------------------------------------------------------------------------------

  // --- ECHO ---------------------------------------------------------------------------------------------------------

  // --- PING ---------------------------------------------------------------------------------------------------------

  // --- QUIT ---------------------------------------------------------------------------------------------------------

  trait QuitRequest extends CommandRequest

  object QuitRequest {

    def apply(): QuitRequest = QuitRequestImpl

    def unapply(self: QuitRequest): Option[Unit] = Some(())

    private object QuitRequestImpl extends QuitRequest {

      override type ResultType = Unit

      override type ResponseType = QuitResponse

      override def encodeAsString: String = "QUIT"

      override def responseAsSucceeded(arguments: Unit): QuitResponse = QuitSucceeded

      override def responseAsFailed(ex: Exception): QuitResponse = QuitFailed(ex)

      override val parser: CommandResponseParser[QuitResponse] = new UnitReplyParser(
        responseAsSucceeded,
        msg => responseAsFailed(RedisIOException(Some(msg)))
      )
    }

  }

  sealed trait QuitResponse extends CommandResponse

  case object QuitSucceeded extends QuitResponse

  case class QuitFailed(ex: Exception) extends QuitResponse

  // --- SELECT -------------------------------------------------------------------------------------------------------

  trait SelectRequest extends CommandRequest {
    val index: Int
  }

  object SelectRequest {

    def apply(index: Int): SelectRequest = new SelectRequestImpl(index)

    def unapply(self: SelectRequest): Option[Int] = Some(self.index)

    private class SelectRequestImpl(val index: Int) extends SelectRequest {

      override type ResultType = Unit

      override type ResponseType = SelectResponse

      override def encodeAsString: String = s"SELECT $index"

      override def responseAsSucceeded(arguments: Unit): SelectResponse = SelectSucceeded

      override def responseAsFailed(ex: Exception): SelectResponse = SelectFailure(ex)

      override lazy val parser: CommandResponseParser[SelectResponse] = new UnitReplyParser(
        responseAsSucceeded,
        msg => responseAsFailed(RedisIOException(Some(msg)))
      )
    }

  }

  sealed trait SelectResponse extends CommandResponse

  case object SelectSucceeded extends SelectResponse

  case class SelectFailure(ex: Exception) extends SelectResponse

}
