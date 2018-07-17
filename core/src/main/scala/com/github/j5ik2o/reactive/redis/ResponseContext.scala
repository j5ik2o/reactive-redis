package com.github.j5ik2o.reactive.redis

import java.text.ParseException
import java.time.ZonedDateTime
import java.util.UUID

import akka.util.ByteString
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, TransactionalCommandRequest }
import scodec.bits.ByteVector

import scala.util.Try

trait ResponseBase {
  val requestContext: RequestContext
  def commandRequestId: UUID       = requestContext.commandRequest.id
  def commandRequestString: String = requestContext.commandRequest.asString

  def completePromise(result: Try[CommandResponse]): requestContext.promise.type =
    requestContext.promise.complete(result)
}

case class ResponseContext(byteString: ByteString,
                           requestContext: RequestContext,
                           requestsInTx: Seq[CommandRequest] = Seq.empty,
                           responseAt: ZonedDateTime = ZonedDateTime.now)
    extends ResponseBase {

  val commandRequest = requestContext.commandRequest

  def withRequestsInTx(values: Seq[CommandRequest]): ResponseContext = copy(requestsInTx = values)

  def parseResponse: Either[ParseException, CommandResponse] = {
    requestContext.commandRequest match {
      case scr: CommandRequest =>
        scr.parse(ByteVector(byteString.toByteBuffer)).map(_._1)
      case tcr: TransactionalCommandRequest =>
        tcr.parse(ByteVector(byteString.toByteBuffer), requests = requestsInTx).map(_._1)
    }
  }

}
