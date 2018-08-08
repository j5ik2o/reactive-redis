package com.github.j5ik2o.reactive.redis.command

import java.util.UUID

trait CommandResponse {
  def id: UUID
  def requestId: UUID
}
