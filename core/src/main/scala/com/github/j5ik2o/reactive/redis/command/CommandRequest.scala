package com.github.j5ik2o.reactive.redis.command

import java.text.ParseException
import java.util.UUID

import com.github.j5ik2o.reactive.redis.parser.model.Expr
import fastparse.core
import fastparse.core.Parsed
import scodec.bits.ByteVector

trait CommandRequest {
  type Elem
  type Repr
  type P[+T] = core.Parser[T, Elem, Repr]

  type Response <: CommandResponse
  type Handler = PartialFunction[(Expr, Int), (Response, Int)]

  val id: UUID
  def asString: String

  protected def responseParser: P[Expr]

  protected def convertToParseSource(s: ByteVector): Repr

}

trait SimpleCommandRequest extends CommandRequest {

  def parse(text: ByteVector, index: Int = 0): Either[ParseException, (Response, Int)] = {
    responseParser.parse(convertToParseSource(text), index) match {
      case f @ Parsed.Failure(_, index, _) =>
        Left(new ParseException(f.msg, index))
      case Parsed.Success(value, index) => Right(parseResponse((value, index)))
    }
  }

  protected def parseResponse: Handler

}

trait TransactionalCommandRequest extends CommandRequest {

  def parse(text: ByteVector,
            index: Int = 0,
            requests: Seq[SimpleCommandRequest] = Seq.empty): Either[ParseException, (Response, Int)] = {
    val repr: Repr = convertToParseSource(text)
    responseParser.parse(repr, index) match {
      case f @ Parsed.Failure(_, index, _) =>
        Left(new ParseException(f.msg, index))
      case Parsed.Success(value, index) => Right(parseResponse(text, requests)((value, index)))
    }
  }

  protected def parseResponse(text: ByteVector, requests: Seq[SimpleCommandRequest]): Handler

}
