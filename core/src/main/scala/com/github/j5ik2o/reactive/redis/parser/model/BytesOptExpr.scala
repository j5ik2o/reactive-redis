package com.github.j5ik2o.reactive.redis.parser.model

import java.nio.charset.StandardCharsets

final case class BytesOptExpr(value: Option[Array[Byte]]) extends Expr {
  def valueAsString: Option[String] = value.map { v =>
    new String(v, StandardCharsets.UTF_8)
  }
}
