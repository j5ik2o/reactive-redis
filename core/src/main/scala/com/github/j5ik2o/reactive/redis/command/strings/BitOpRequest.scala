package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command._
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr, SimpleExpr }
import fastparse.all._

case class BitOpRequest(id: UUID,
                        operand: BitOpRequest.Operand,
                        outputKey: String,
                        inputKey1: String,
                        inputKey2: String)
    extends CommandRequest
    with StringParsersSupport {

  override type Response = BitOpResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = s"BITOP ${operand.toString} $outputKey $inputKey1 $inputKey2"

  override protected def responseParser: P[Expr] = P(integerReply | simpleStringReply)

  override protected def parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (BitOpSucceeded(UUID.randomUUID(), id, n), next)
    case (SimpleExpr(QUEUED), next) =>
      (BitOpSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (BitOpFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
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
case class BitOpSuspended(id: UUID, requestId: UUID)             extends BitOpResponse
case class BitOpSucceeded(id: UUID, requestId: UUID, value: Int) extends BitOpResponse
case class BitOpFailed(id: UUID, requestId: UUID, ex: Exception) extends BitOpResponse
