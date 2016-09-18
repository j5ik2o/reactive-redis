package com.github.j5ik2o.reactive.redis.keys

object ValueType extends Enumeration {
  val None = Value("none")
  val String = Value("string")
  val List = Value("list")
  val Set = Value("set")
  val ZSet = Value("zset")
  val Hash = Value("hash")
}
