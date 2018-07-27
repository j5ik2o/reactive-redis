package com.github.j5ik2o.reactive.redis

import java.time.ZonedDateTime
import java.util.UUID

import akka.util.ByteString
import com.github.j5ik2o.reactive.redis.command.transactions.{ ExecRequest, MultiRequest }
import com.github.j5ik2o.reactive.redis.command.{ CommandRequestBase, CommandResponse }

import scala.concurrent.Promise

final case class RequestContext(commandRequest: CommandRequestBase,
                                promise: Promise[CommandResponse],
                                requestAt: ZonedDateTime) {
  val id: UUID                             = commandRequest.id
  val commandRequestString: String         = commandRequest.toByteString.utf8String
  val commandRequestByteString: ByteString = commandRequest.toByteString
  def isMulti: Boolean                     = commandRequest.isInstanceOf[MultiRequest]
  def isExec: Boolean                      = commandRequest.isInstanceOf[ExecRequest]
}
