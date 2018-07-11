package com.github.j5ik2o.reactive.redis

import java.time.ZonedDateTime

import akka.util.ByteString

case class ResponseContext(byteString: ByteString, requestContext: RequestContext, responseAt: ZonedDateTime)
