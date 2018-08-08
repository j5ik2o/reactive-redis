package com.github.j5ik2o.reactive.redis

import java.time.ZonedDateTime
import java.util.UUID

import com.github.j5ik2o.reactive.redis.command.connection.QuitRequest
import com.github.j5ik2o.reactive.redis.command.transactions.{ ExecRequest, MultiRequest }
import com.github.j5ik2o.reactive.redis.command.{ CommandRequestBase, CommandResponse }

import scala.concurrent.Promise

final case class RequestContext(commandRequest: CommandRequestBase,
                                promise: Promise[CommandResponse],
                                requestAt: ZonedDateTime) {
  lazy val id: UUID                     = commandRequest.id
  lazy val commandRequestString: String = commandRequest.asString
  def isQuit: Boolean                   = commandRequest.isInstanceOf[QuitRequest]
  def isMulti: Boolean                  = commandRequest.isInstanceOf[MultiRequest]
  def isExec: Boolean                   = commandRequest.isInstanceOf[ExecRequest]
}
