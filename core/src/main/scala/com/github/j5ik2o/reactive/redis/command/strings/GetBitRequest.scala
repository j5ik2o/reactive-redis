package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr, SimpleExpr }
import fastparse.all._

final class GetBitRequest(val id: UUID, val key: String, val offset: Int)
    extends CommandRequest
    with StringParsersSupport {

  override type Response = GetBitResponse

  override val isMasterOnly: Boolean = false

  override def asString: String = cs("GETBIT", Some(key), Some(offset.toString))

  override protected lazy val responseParser: P[Expr] = fastParse(integerReply | simpleStringReply | errorReply)

  override protected lazy val parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (GetBitSucceeded(UUID.randomUUID(), id, n), next)
    case (SimpleExpr(QUEUED), next) =>
      (GetBitSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (GetBitFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

  override def equals(other: Any): Boolean = other match {
    case that: GetBitRequest =>
      id == that.id &&
      key == that.key &&
      offset == that.offset
    case _ => false
  }

  @SuppressWarnings(Array("org.wartremover.warts.JavaSerializable"))
  override def hashCode(): Int = {
    val state = Seq(id, key, offset)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = s"GetBitRequest($id, $key, $offset)"
}

object GetBitRequest {

  def apply(id: UUID, key: String, offset: Int): GetBitRequest = new GetBitRequest(id, key, offset)

  def unapply(self: GetBitRequest): Option[(UUID, String, Int)] = Some((self.id, self.key, self.offset))

  def create(id: UUID, key: String, offset: Int): GetBitRequest = apply(id, key, offset)

}

sealed trait GetBitResponse                                                    extends CommandResponse
final case class GetBitSuspended(id: UUID, requestId: UUID)                    extends GetBitResponse
final case class GetBitSucceeded(id: UUID, requestId: UUID, value: Long)       extends GetBitResponse
final case class GetBitFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends GetBitResponse
