package com.github.j5ik2o.reactive.redis.keys

import com.github.j5ik2o.reactive.redis._

object KeysProtocol {

  // --- DEL
  case class DelRequest(keys: Seq[String])

  case class DelSucceeded(value: Int)

  case class DelFailure(ex: Exception)

  // --- DUMP

  // --- EXISTS
  case class ExistsRequest(key: String)

  case class ExistsSucceeded(value: Boolean)

  case class ExistsFailure(ex: Exception)

  // --- EXPIRE

  case class ExpireRequest(key: String, timeout: Long)

  case class ExpireSucceeded(value: Boolean)

  case class ExpireFailure(ex: Exception)

  // --- EXPIREAT

  case class ExpireAtRequest(key: String, unixTimeout: Long)

  case class ExpireAtSucceeded(value: Boolean)

  case class ExpireAtFailure(ex: Exception)

  // --- KEYS
  case class KeysRequest(keyPattern: String = "*") extends CommandRequest {

    class Parser extends CommandResponseParser[ResponseType] {
      override protected val responseParser: Parser[KeysResponse] = {
        stringArrayWithCrLf ^^ { array =>
          responseAsSucceeded(array.values.map(_.value))
        }
      }
    }

    override def encodeAsString: String = s"KEYS $keyPattern"

    override type ResultType = Seq[String]

    override type ResponseType = KeysResponse

    override def responseAsSucceeded(arguments: Seq[String]): KeysResponse =
      KeysSucceeded(arguments)

    override def responseAsFailed(ex: Exception): KeysResponse =
      KeysFailure(ex)

    override val parser: CommandResponseParser[ResponseType] = new Parser
  }

  sealed trait KeysResponse extends CommandResponse

  case class KeysSucceeded(values: Seq[String]) extends KeysResponse

  case class KeysFailure(ex: Exception) extends KeysResponse

  // --- MIGRATE

  // --- MOVE
  case class MoveRequest(key: String, index: Int)

  case class MoveSucceeded(value: Boolean)

  case class MoveFailure(ex: Exception)

  // --- OBJECT

  // --- PERSIST

  case class PersistRequest(key: String)

  case class PersistSucceeded(value: Boolean)

  case class PersistFailure(ex: Exception)

  // --- PEXPIRE

  // --- PEXPIREAT

  // --- PTTL

  // --- RANDOMKEY
  case object RandomKeyRequest

  case class RandomKeySucceeded(value: String)

  case class RandomKeyFailure(ex: Exception)

  // --- RENAME

  case class RenameRequest(oldKey: String, newKey: String)

  case object RenameSucceeded

  case class RenameFailure(ex: Exception)

  // --- RENAMENX

  case class RenameNxRequest(oldKey: String, newKey: String)

  case class RenameNxSucceeded(value: Boolean)

  case class RenameNxFailure(ex: Exception)

  // --- RESTORE

  // --- SCAN

  // --- SORT

  // --- TTL
  case class TTLRequest(key: String)

  case class TTLSucceeded(value: Int)

  case class TTLFailure(ex: Exception)

  // --- TYPE

  case class TypeRequest(key: String)

  case class TypeSucceeded(value: ValueType.Value)

  case class TypeFailure(ex: Exception)

  // --- WAIT
}
