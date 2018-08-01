package com.github.j5ik2o.reactive.redis.command

import java.text.ParseException
import java.util.UUID

import com.github.j5ik2o.reactive.redis.parser.model.Expr
import fastparse.core.Parsed
import scodec.bits.ByteVector

trait Decoder[+T, Elem, Repr] {
  def parse(input: Repr, index: Int = 0): Either[ParseException, (T, Int)]
}

trait CommandRequestBase {
  type Elem
  type Repr
  type P[+T] = Decoder[T, Elem, Repr]

  def wrap[T](p: fastparse.core.Parser[T, Elem, Repr]): Decoder[T, Elem, Repr] = new Decoder[T, Elem, Repr] {
    override def parse(input: Repr, index: Int): Either[ParseException, (T, Int)] = p.parse(input, index) match {
      case f @ Parsed.Failure(_, index, _) =>
        Left(new ParseException(f.msg, index))
      case Parsed.Success(value, index) => Right((value, index))
    }
  }

  type Response <: CommandResponse
  type Handler = PartialFunction[(Expr, Int), (Response, Int)]

  val id: UUID

  val isMasterOnly: Boolean

  def asString: String

  protected def responseParser: P[Expr]

  protected def convertToParseSource(s: ByteVector): Repr

}
