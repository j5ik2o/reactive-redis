package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse }
import com.github.j5ik2o.reactive.redis.parser.Parsers
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, SimpleExpr }
import fastparse.all.P

case class SetRequest(id: UUID, key: String, value: String) extends CommandRequest {
  override type Response = SetResponse

  override def asString: String = s"SET $key $value"

  protected def responseParser: P[Expr] = Parsers.simpleStringReply

  override def parseResponse: Handler = {
    case SimpleExpr("OK") =>
      SetSucceeded(UUID.randomUUID(), id)
    case ErrorExpr(msg) =>
      SetFailed(UUID.randomUUID(), id, RedisIOException(Some(msg)))
  }
}

object SetRequest {
  trait ToString[A] {
    def toString(value: A): String
  }

  implicit val string = new ToString[String] {
    override def toString(value: String): String = value
  }

  implicit val number = new ToString[Int] {
    override def toString(value: Int): String = value.toString
  }

  def apply[A](id: UUID, key: String, value: A)(implicit TS: ToString[A]): SetRequest =
    new SetRequest(id, key, TS.toString(value))
}

sealed trait SetResponse                                              extends CommandResponse
case class SetSucceeded(id: UUID, requestId: UUID)                    extends SetResponse
case class SetFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends SetResponse
