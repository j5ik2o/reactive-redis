package com.github.j5ik2o.reactive.redis.command.transactions

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command._
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ArraySizeExpr, ErrorExpr, Expr }
import scodec.bits.ByteVector
import fastparse.all._

@SuppressWarnings(Array("org.wartremover.warts.EitherProjectionPartial"))
final class ExecRequest(val id: UUID) extends TransactionalCommandRequest with StringParsersSupport {

  override type Response = ExecResponse

  override def asString: String = "EXEC"

  override protected lazy val responseParser: P[Expr] = fastParse(arrayPrefixWithCrLf | errorReply)

  protected def parseResponse(text: ByteVector, requests: Seq[CommandRequest]): Handler = {
    case (ArraySizeExpr(size), next) =>
      val result =
        if (size == -1)
          (next, Seq.empty)
        else
          requests.foldLeft((next, Seq.empty[CommandResponse])) {
            case ((n, seq), e) =>
              val (res, _n) = e.parse(text, n).right.get
              (_n, seq :+ res)
          }
      (ExecSucceeded(UUID.randomUUID(), id, result._2), result._1)
    case (ErrorExpr(msg), next) =>
      (ExecFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

  override val isMasterOnly: Boolean = true

  override def equals(other: Any): Boolean = other match {
    case that: ExecRequest =>
      id == that.id
    case _ => false
  }

  @SuppressWarnings(Array("org.wartremover.warts.JavaSerializable"))
  override def hashCode(): Int = {
    val state = Seq(id)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = s"ExecRequest($id)"

}

object ExecRequest {

  def apply(id: UUID): ExecRequest = new ExecRequest(id)

  def unapply(self: ExecRequest): Option[UUID] = Some(self.id)

  def create(id: UUID): ExecRequest = apply(id)

}

sealed trait ExecResponse                                                                 extends CommandResponse
final case class ExecSucceeded(id: UUID, requestId: UUID, response: Seq[CommandResponse]) extends ExecResponse
final case class ExecFailed(id: UUID, requestId: UUID, ex: RedisIOException)              extends ExecResponse
