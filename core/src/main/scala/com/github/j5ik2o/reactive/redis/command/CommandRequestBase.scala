package com.github.j5ik2o.reactive.redis.command

import java.util.UUID

import akka.util.ByteString
import com.github.j5ik2o.reactive.redis.parser.model.Expr
import fastparse.core
import scodec.bits.ByteVector

trait CommandRequestBase {
  type Elem
  type Repr
  type P[+T] = core.Parser[T, Elem, Repr]

  type Response <: CommandResponse
  type Handler = PartialFunction[(Expr, Int), (Response, Int)]

  val id: UUID

  val isMasterOnly: Boolean

  def asString: String

  def toByteString: ByteString

  protected def responseParser: P[Expr]

  protected def convertToParseSource(s: ByteVector): Repr

}
