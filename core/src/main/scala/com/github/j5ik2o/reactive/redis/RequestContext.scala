package com.github.j5ik2o.reactive.redis

import java.time.ZonedDateTime

import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse }

import scala.concurrent.Promise

case class RequestContext(commandRequest: CommandRequest, promise: Promise[CommandResponse], requestAt: ZonedDateTime) {
  val id = commandRequest.id
}
