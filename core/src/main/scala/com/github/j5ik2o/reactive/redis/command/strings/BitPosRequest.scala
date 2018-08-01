package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command._
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr, SimpleExpr }
import fastparse.all._

final case class BitPosRequest(id: UUID, key: String, bit: Int, startAndEnd: Option[BitPosRequest.StartAndEnd] = None)
    extends CommandRequest
    with StringParsersSupport {

  override type Response = BitPosResponse

  override val isMasterOnly: Boolean = false

  override def asString: String =
    s"BITPOS $key $bit" + startAndEnd.fold("") { e =>
      " " + e.start + e.end.fold("") { v =>
        s" $v"
      }
    }

  override protected def responseParser: P[Expr] = wrap(integerReply | simpleStringReply)

  override protected def parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (BitPosSucceeded(UUID.randomUUID, id, n), next)
    case (SimpleExpr(QUEUED), next) =>
      (BitPosSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (BitPosFailed(UUID.randomUUID, id, RedisIOException(Some(msg))), next)
  }

}

object BitPosRequest {
  final case class StartAndEnd(start: Int, end: Option[Int] = None)
}

sealed trait BitPosResponse                                             extends CommandResponse
final case class BitPosSuspended(id: UUID, requestId: UUID)             extends BitPosResponse
final case class BitPosSucceeded(id: UUID, requestId: UUID, value: Int) extends BitPosResponse
final case class BitPosFailed(id: UUID, requestId: UUID, ex: Exception) extends BitPosResponse
