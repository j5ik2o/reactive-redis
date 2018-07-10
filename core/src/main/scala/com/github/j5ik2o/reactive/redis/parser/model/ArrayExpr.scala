package com.github.j5ik2o.reactive.redis.parser.model

case class ArrayExpr[A](values: Seq[A]) extends Expr
