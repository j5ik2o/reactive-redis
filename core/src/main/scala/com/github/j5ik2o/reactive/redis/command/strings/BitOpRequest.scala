package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command._
import com.github.j5ik2o.reactive.redis.parser.StringParsers
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr }

case class BitOpRequest(id: UUID,
                        operand: BitOpRequest.Operand,
                        outputKey: String,
                        inputKey1: String,
                        inputKey2: String)
    extends CommandRequest
    with StringParsersSupport {
  override type Response = BitOpResponse

  override def asString: String = s"BITOP ${operand.toString} $outputKey $inputKey1 $inputKey2"

  override protected def responseParser: P[Expr] = StringParsers.integerReply

  override protected def parseResponse: Handler = {
    case NumberExpr(n) =>
      BitOpSucceeded(UUID.randomUUID(), id, n)
    case ErrorExpr(msg) =>
      BitOpFailed(UUID.randomUUID(), id, RedisIOException(Some(msg)))
  }
}

object BitOpRequest {

  sealed trait Operand

  object Operand {

    case object AND extends Operand

    case object OR extends Operand

    case object XOR extends Operand

  }

}

sealed trait BitOpResponse                                       extends CommandResponse
case class BitOpSucceeded(id: UUID, requestId: UUID, value: Int) extends BitOpResponse
case class BitOpFailed(id: UUID, requestId: UUID, ex: Exception) extends BitOpResponse
