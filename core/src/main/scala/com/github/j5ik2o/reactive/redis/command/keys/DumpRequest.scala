package com.github.j5ik2o.reactive.redis.command.keys

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse }
import com.github.j5ik2o.reactive.redis.parser.ByteParsers
import com.github.j5ik2o.reactive.redis.parser.model.{ BytesOptExpr, ErrorExpr, Expr }
import fastparse.byte.all._

case class DumpRequest(id: UUID, key: String) extends CommandRequest {
  type Elem              = Byte
  type Repr              = Bytes
  override type Response = DumpResponse

  override def asString: String = s"DUMP $key"

  override protected def responseParser: P[Expr]               = ByteParsers.bulkBytesReply
  override protected def convertToParseSource(s: Bytes): Bytes = s

  override protected def parseResponse: Handler = {
    case BytesOptExpr(b) =>
      DumpSucceeded(UUID.randomUUID(), id, b)
    case ErrorExpr(msg) =>
      DumpFailed(UUID.randomUUID(), id, RedisIOException(Some(msg)))
  }

}

sealed trait DumpResponse                                                       extends CommandResponse
case class DumpSucceeded(id: UUID, requestId: UUID, value: Option[Array[Byte]]) extends DumpResponse
case class DumpFailed(id: UUID, requestId: UUID, ex: RedisIOException)          extends DumpResponse
