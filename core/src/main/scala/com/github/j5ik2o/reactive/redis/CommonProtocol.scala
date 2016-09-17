package com.github.j5ik2o.reactive.redis

object CommonProtocol {

  case object QuitRequest

  case object QuitSucceeded


  // --- EXISTS

  case class ExistsRequest(key: String)

  case class ExistsSucceeded(value: Boolean)

  case class ExistsFailure(ex: Exception)

  // --- DEL

  case class DelRequest(keys: Seq[String])

  case class DelSucceeded(value: Int)

  case class DelFailure(ex: Exception)

  // --- TYPE

  case class TypeRequest(key: String)

  case class TypeSucceeded(value: ValueType.Value)

  case class TypeFailure(ex: Exception)

  // --- KEYS

  case class KeysRequest(keyPattern: String = "*")

  case class KeysSucceeded(values: Seq[String])

  case class KeysFailure(ex: Exception)

  // --- RANDOMKEY

  case object RandomKeyRequest

  case class RandomKeySucceeded(value: String)

  case class RandomKeyFailure(ex: Exception)

  // --- DBSIZE

  case object DBSizeRequest

  case class DBSizeSucceeded(value: Int)

  case class DBSizeFailure(ex: Exception)

  // --- EXPIRE

  case class ExpireRequest(key: String, timeout: Long)

  case class ExpireSucceeded(value: Boolean)

  case class ExpireFailure(ex: Exception)

  // --- EXPIREAT

  case class ExpireAtRequest(key: String, unixTimeout: Long)

  case class ExpireAtSucceeded(value: Boolean)

  case class ExpireAtFailure(ex: Exception)

  // --- PERSIST

  case class PersistRequest(key: String)

  case class PersistSucceeded(value: Boolean)

  case class PersistFailure(ex: Exception)

  // --- TTL

  case class TTLRequest(key: String)

  case class TTLSucceeded(value: Int)

  case class TTLFailure(ex: Exception)

  // --- FLUSHDB

  case object FlushDBRequest

  case object FlushDBSucceeded

  case class FlushDBFailure(ex: Exception)

  // --- FLUSHALL

  case object FlushAllRequest

  case object FlushAllSucceeded

  case class FlushAllFailure(ex: Exception)

}
