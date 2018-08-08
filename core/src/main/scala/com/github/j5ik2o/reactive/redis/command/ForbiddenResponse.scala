package com.github.j5ik2o.reactive.redis.command
import java.util.UUID

final case class ForbiddenResponse(id: UUID) extends CommandResponse {
  override def requestId: UUID = throw new NoSuchElementException
}
