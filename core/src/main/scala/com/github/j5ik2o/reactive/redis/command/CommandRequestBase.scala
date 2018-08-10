package com.github.j5ik2o.reactive.redis.command

import java.text.ParseException
import java.util.UUID

import com.github.j5ik2o.reactive.redis.parser.model.Expr
import com.github.j5ik2o.reactive.redis.parser.util.Parsers
import fastparse.core.Parsed
import scodec.bits.ByteVector

trait Decoder[+T, Elem, Repr] extends Serializable {
  def parse(input: Repr, index: Int = 0): Either[ParseException, (T, Int)]
}

trait CommandRequestBase {
  type Elem
  type Repr
  type P[+T] = Decoder[T, Elem, Repr]

  def cs(param: String, params: Option[String]*): String = {
    val _params = Some(param) :: params.toList
    val result = "*" + _params.count(_.nonEmpty) + "\r\n" +
    _params.collect { case Some(v) => "$" + v.length + "\r\n" + s"$v" + "\r\n" }.mkString
    result.stripSuffix("\r\n")
  }

  def fastParse[T](p: => fastparse.core.Parser[T, Elem, Repr]): Decoder[T, Elem, Repr] = new Decoder[T, Elem, Repr] {
    override def parse(input: Repr, index: Int): Either[ParseException, (T, Int)] = p.parse(input, index) match {
      case f @ Parsed.Failure(_, index, _) =>
        Left(new ParseException(f.msg, index))
      case Parsed.Success(value, index) => Right((value, index))
    }
  }

  def default[Parser[+ _], T](P: Parsers[Parser])(p: => Parser[T]): Decoder[T, Elem, String] =
    new Decoder[T, Elem, String] {
      override def parse(input: String, index: Int): Either[ParseException, (T, Int)] = P.run(p)(input) match {
        case Left(parseError) =>
          Left(new ParseException(parseError.toString, index))
        case Right(value) => Right((value, index))
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
