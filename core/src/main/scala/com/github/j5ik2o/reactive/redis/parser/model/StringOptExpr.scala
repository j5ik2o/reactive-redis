package com.github.j5ik2o.reactive.redis.parser.model

case class StringOptExpr(value: Option[String]) extends Expr {
  def toStringExpr = StringExpr(value.get)
}
