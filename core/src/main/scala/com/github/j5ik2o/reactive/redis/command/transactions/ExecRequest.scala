package com.github.j5ik2o.reactive.redis.command.transactions

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command._
import com.github.j5ik2o.reactive.redis.parser.StringParsers
import com.github.j5ik2o.reactive.redis.parser.model.{ ArraySizeExpr, ErrorExpr, Expr }
import scodec.bits.ByteVector

case class ExecRequest(id: UUID) extends TransactionalCommandRequest with StringParsersSupport {
  override type Response = ExecResponse

  override def asString: String = "EXEC"

  override protected def responseParser: P[Expr] = StringParsers.arrayPrefixWithCrLfOrErrorWithCrLf

  protected def parseResponse(text: ByteVector, requests: Seq[SimpleCommandRequest]): Handler = {
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

}

sealed trait ExecResponse                                                           extends CommandResponse
case class ExecSucceeded(id: UUID, requestId: UUID, response: Seq[CommandResponse]) extends ExecResponse
case class ExecFailed(id: UUID, requestId: UUID, ex: RedisIOException)              extends ExecResponse
