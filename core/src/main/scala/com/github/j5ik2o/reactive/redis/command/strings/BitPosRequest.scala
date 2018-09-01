package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command._
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr, SimpleExpr }
import fastparse.all._

final class BitPosRequest(val id: UUID,
                          val key: String,
                          val bit: Int,
                          val startAndEnd: Option[BitPosRequest.StartAndEnd] = None)
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

  override def equals(other: Any): Boolean = other match {
    case that: BitPosRequest =>
      id == that.id &&
      key == that.key &&
      bit == that.bit &&
      startAndEnd == that.startAndEnd
    case _ => false
  }

  @SuppressWarnings(Array("org.wartremover.warts.JavaSerializable"))
  override def hashCode(): Int = {
    val state = Seq(id, key, bit, startAndEnd)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = s"BitPosRequest($id, $key, $bit, $startAndEnd)"
}

object BitPosRequest {

  def apply(id: UUID, key: String, bit: Int, startAndEnd: Option[BitPosRequest.StartAndEnd] = None): BitPosRequest =
    new BitPosRequest(id, key, bit, startAndEnd)

  def unapply(self: BitPosRequest): Option[(UUID, String, Int, Option[StartAndEnd])] =
    Some((self.id, self.key, self.bit, self.startAndEnd))

  def create(id: UUID, key: String, bit: Int, startAndEnd: Option[BitPosRequest.StartAndEnd] = None): BitPosRequest =
    apply(id, key, bit, startAndEnd)

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
