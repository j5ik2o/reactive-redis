package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr, SimpleExpr }
import fastparse.all._

@SuppressWarnings(Array("org.wartremover.warts.ToString"))
final class MSetNxRequest(val id: UUID, val values: Map[String, Any]) extends CommandRequest with StringParsersSupport {

  override type Response = MSetNxResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = {
    val params = values.toSeq.flatMap { case (k, v) => Seq(k, v.toString) }.map(Some(_))
    cs("MSETNX", params: _*)
  }

  override protected lazy val responseParser: P[Expr] = fastParse(integerReply | simpleStringReply | errorReply)

  override protected lazy val parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (MSetNxSucceeded(UUID.randomUUID(), id, n == 1), next)
    case (SimpleExpr(QUEUED), next) =>
      (MSetNxSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (MSetNxFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

  override def equals(other: Any): Boolean = other match {
    case that: MSetNxRequest =>
      id == that.id &&
      values == that.values
    case _ => false
  }

  @SuppressWarnings(Array("org.wartremover.warts.JavaSerializable"))
  override def hashCode(): Int = {
    val state = Seq(id, values)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = s"MSetNxRequest($id, $values)"

}

object MSetNxRequest {

  def apply(id: UUID, values: Map[String, Any]): MSetNxRequest = new MSetNxRequest(id, values)

  def unapply(self: MSetNxRequest): Option[(UUID, Map[String, Any])] = Some((self.id, self.values))

  def create(id: UUID, values: Map[String, Any]): MSetNxRequest = apply(id, values)

}

sealed trait MSetNxResponse                                                    extends CommandResponse
final case class MSetNxSuspended(id: UUID, requestId: UUID)                    extends MSetNxResponse
final case class MSetNxSucceeded(id: UUID, requestId: UUID, isSet: Boolean)    extends MSetNxResponse
final case class MSetNxFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends MSetNxResponse
