package com.github.j5ik2o.reactive.redis.command

import java.text.ParseException

import fastparse.core.Parsed
import scodec.bits.ByteVector

trait CommandRequest extends CommandRequestBase {

  def parse(text: ByteVector, index: Int = 0): Either[ParseException, (Response, Int)] = {
    responseParser.parse(convertToParseSource(text), index) match {
      case f @ Parsed.Failure(_, index, _) =>
        Left(new ParseException(f.msg, index))
      case Parsed.Success(value, index) => Right(parseResponse((value, index)))
    }
  }

  protected def parseResponse: Handler

}
