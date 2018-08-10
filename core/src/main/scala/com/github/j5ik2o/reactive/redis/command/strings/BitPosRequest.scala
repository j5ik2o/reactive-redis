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

  override val asString: String = {
    val option: Seq[Option[String]] = startAndEnd match {
      case Some(v) =>
        Seq(Some(v.start.toString), v.end.map(_.toString))
      case None =>
        Seq.empty[Option[String]]
    }
    cs("BITPOS", Some(key) :: Some(bit.toString) :: option.toList: _*)
  }

  override protected lazy val responseParser: P[Expr] = fastParse(integerReply | simpleStringReply | errorReply)

  override protected lazy val parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (BitPosSucceeded(UUID.randomUUID, id, n), next)
    case (SimpleExpr(QUEUED), next) =>
      (BitPosSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (BitPosFailed(UUID.randomUUID, id, RedisIOException(Some(msg))), next)
  }

}

object BitPosRequest {
  final case class StartAndEnd(start: Int, end: Option[Int] = None) {
    def asString: String = start + end.fold("") { v =>
      s" $v"
    }
  }
}

sealed trait BitPosResponse                                              extends CommandResponse
final case class BitPosSuspended(id: UUID, requestId: UUID)              extends BitPosResponse
final case class BitPosSucceeded(id: UUID, requestId: UUID, value: Long) extends BitPosResponse
final case class BitPosFailed(id: UUID, requestId: UUID, ex: Exception)  extends BitPosResponse
