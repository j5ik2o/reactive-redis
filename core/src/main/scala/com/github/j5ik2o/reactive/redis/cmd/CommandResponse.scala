package com.github.j5ik2o.reactive.redis.cmd

import java.util.UUID

trait CommandResponse {
  val id: UUID
  val requestId: UUID
}
