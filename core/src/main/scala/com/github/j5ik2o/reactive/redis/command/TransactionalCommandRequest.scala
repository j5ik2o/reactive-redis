package com.github.j5ik2o.reactive.redis.command

import java.text.ParseException

import fastparse.core.Parsed
import scodec.bits.ByteVector

trait TransactionalCommandRequest extends CommandRequestBase {

  def parse(text: ByteVector,
            index: Int = 0,
            requests: Seq[CommandRequest] = Seq.empty): Either[ParseException, (Response, Int)] = {
    val repr: Repr = convertToParseSource(text)
    responseParser.parse(repr, index) match {
      case f @ Parsed.Failure(_, index, _) =>
        Left(new ParseException(f.msg, index))
      case Parsed.Success(value, index) => Right(parseResponse(text, requests)((value, index)))
    }
  }

  protected def parseResponse(text: ByteVector, requests: Seq[CommandRequest]): Handler

}
