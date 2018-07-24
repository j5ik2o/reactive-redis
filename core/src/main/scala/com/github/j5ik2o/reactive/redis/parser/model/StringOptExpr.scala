package com.github.j5ik2o.reactive.redis.parser.model

final case class StringOptExpr(value: Option[String]) extends Expr {
  def toStringExpr: StringExpr = StringExpr(value.getOrElse(throw NoSuchValueException(Some("value is Nothing"))))
}
