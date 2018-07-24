package com.github.j5ik2o.reactive.redis.parser.model

import java.nio.charset.StandardCharsets

@SuppressWarnings(Array("org.wartremover.warts.ArrayEquals"))
final case class BytesExpr(value: Array[Byte]) extends Expr {
  def valueAsString: String = new String(value, StandardCharsets.UTF_8)
}
