package com.github.j5ik2o.reactive.redis.parser.model

import java.nio.charset.StandardCharsets

case class BytesExpr(value: Array[Byte]) extends Expr {
  def valueAsString = new String(value, StandardCharsets.UTF_8)
}
