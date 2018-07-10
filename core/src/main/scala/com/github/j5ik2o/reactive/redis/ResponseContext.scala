package com.github.j5ik2o.reactive.redis

import java.text.ParseException
import java.time.ZonedDateTime

import akka.util.ByteString
import com.github.j5ik2o.reactive.redis.command.CommandResponse

import scala.util.Try

case class ResponseContext(byteString: ByteString, requestContext: RequestContext, responseAt: ZonedDateTime) {

  def parseResponse: Either[ParseException, requestContext.commandRequest.Response] =
    requestContext.commandRequest.parse(byteString.utf8String)

  def completePromise(result: Try[CommandResponse]): requestContext.promise.type =
    requestContext.promise.complete(result)

}
