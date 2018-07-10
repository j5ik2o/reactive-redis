package com.github.j5ik2o.reactive.redis.cmd

import java.util.UUID

import com.github.j5ik2o.reactive.redis.model.Expr
import com.github.j5ik2o.reactive.redis.parser.Parsers
import fastparse.all._

case class GetSetRequest(id: UUID, key: String, value: String) extends CommandRequest {
  override type Response = GetSetResponse

  override def asString: String = s"GETSET $key $value"

  override protected def responseParser: P[Expr] = Parsers.bulkStringReply | Parsers.simpleStringReply

  override protected def parseResponse: Handler = ???
}

trait GetSetResponse extends CommandResponse