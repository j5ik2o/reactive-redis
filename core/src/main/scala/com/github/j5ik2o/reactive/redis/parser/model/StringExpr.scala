package com.github.j5ik2o.reactive.redis.parser.model

final case class StringExpr(value: String) extends Expr {
  def toStringOptExpr: StringOptExpr = StringOptExpr(Some(value))
}
