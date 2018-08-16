package com.github.j5ik2o.reactive.redis.parser.model

final case class ScanExpr(cursor: Option[String], values: Seq[String]) extends Expr
