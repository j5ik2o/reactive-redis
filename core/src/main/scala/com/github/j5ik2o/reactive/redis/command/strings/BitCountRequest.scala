package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command._
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr, SimpleExpr }
import fastparse.all._

final class BitCountRequest(val id: UUID, val key: String, val startAndEnd: Option[StartAndEnd] = None)
    extends CommandRequest
    with StringParsersSupport {

  override type Response = BitCountResponse

  override val isMasterOnly: Boolean = false

  override def asString: String =
    cs(
      "BITCOUNT",
      Some(key) :: startAndEnd.map(v => List(v.start.toString, v.end.toString).map(Some(_))).getOrElse(Nil): _*
    )

  override protected lazy val responseParser: P[Expr] = fastParse(integerReply | simpleStringReply | errorReply)

  override protected lazy val parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (BitCountSucceeded(UUID.randomUUID(), id, n), next)
    case (SimpleExpr(QUEUED), next) =>
      (BitCountSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (BitCountFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

  override def equals(other: Any): Boolean = other match {
    case that: BitCountRequest =>
      id == that.id &&
      key == that.key &&
      startAndEnd == that.startAndEnd
    case _ => false
  }

  @SuppressWarnings(Array("org.wartremover.warts.JavaSerializable"))
  override def hashCode(): Int = {
    val state = Seq(id, key, startAndEnd)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = s"BitCountRequest($id, $key, $startAndEnd)"
}

object BitCountRequest {

  def apply(id: UUID, key: String, startAndEnd: Option[StartAndEnd] = None): BitCountRequest =
    new BitCountRequest(id, key, startAndEnd)

  def unapply(self: BitCountRequest): Option[(UUID, String, Option[StartAndEnd])] =
    Some((self.id, self.key, self.startAndEnd))

  def create(id: UUID, key: String, startAndEnd: Option[StartAndEnd] = None): BitCountRequest =
    apply(id, key, startAndEnd)

}

sealed trait BitCountResponse                                                    extends CommandResponse
final case class BitCountSuspended(id: UUID, requestId: UUID)                    extends BitCountResponse
final case class BitCountSucceeded(id: UUID, requestId: UUID, value: Long)       extends BitCountResponse
final case class BitCountFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends BitCountResponse
