package com.github.j5ik2o.reactive.redis.command

import akka.util.ByteString

trait ByteStringSerializer[A] {
  def serialize(source: A): ByteString
}
