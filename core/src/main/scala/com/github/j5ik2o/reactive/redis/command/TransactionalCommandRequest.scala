package com.github.j5ik2o.reactive.redis.command

import java.text.ParseException

import cats.implicits._
import scodec.bits.ByteVector

trait TransactionalCommandRequest extends CommandRequestBase {

  def parse(
      text: ByteVector,
      index: Int = 0,
      requests: Seq[CommandRequest] = Seq.empty
  ): Either[ParseException, (Response, Int)] = {
    val repr: Repr = convertToParseSource(text)
    responseParser.parse(repr, index).map {
      case (v, i) =>
        parseResponse(text, requests)((v, i))
    }
  }

  protected def parseResponse(text: ByteVector, requests: Seq[CommandRequest]): Handler

}
