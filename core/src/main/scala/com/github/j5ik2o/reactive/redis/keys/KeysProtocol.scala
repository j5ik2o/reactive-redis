package com.github.j5ik2o.reactive.redis.keys

import com.github.j5ik2o.reactive.redis._

object KeysProtocol {

  // --- DEL  ---------------------------------------------------------------------------------------------------
  object DelRequest {
    def apply(keys: Seq[String]): DelRequest = new DelRequestImpl(keys)

    def unapply(self: DelRequest): Option[Seq[String]] = Some(self.keys)
  }

  trait DelRequest extends CommandRequest {
    val keys: Seq[String]
  }

  private class DelRequestImpl(val keys: Seq[String]) extends DelRequest {

    override def encodeAsString: String = s"DEL ${keys.mkString(" ")}"

    override type ResultType = Int

    override type ResponseType = DelResponse

    override def responseAsSucceeded(arguments: Int): DelResponse =
      DelSucceeded(arguments)

    override def responseAsFailed(ex: Exception): DelResponse =
      DelFailed(ex)

    override val parser: CommandResponseParser[DelResponse] = new IntegerReplyParser(
      responseAsSucceeded,
      msg => responseAsFailed(RedisIOException(Some(msg)))
    )
  }

  sealed trait DelResponse extends CommandResponse

  case class DelSucceeded(value: Int) extends DelResponse

  case class DelFailed(ex: Exception) extends DelResponse

  // --- DUMP  ---------------------------------------------------------------------------------------------------
  object DumpRequest {
    def apply(key: String): DumpRequest = new DumpRequestImpl(key)
    def unapply(self: DumpRequest): Option[String] = Some(self.key)
  }
  trait DumpRequest extends CommandRequest {
    val key: String
  }

  private class DumpRequestImpl(val key: String) extends DumpRequest {

    override def encodeAsString: String = s"DUMP $key"

    override type ResultType = Option[String]
    override type ResponseType = DumpResponse

    override def responseAsSucceeded(arguments: Option[String]): DumpResponse =
      DumpSucceeded(arguments)

    override def responseAsFailed(ex: Exception): DumpResponse =
      DumpFailed(ex)

    override val parser: CommandResponseParser[DumpResponse] = new BulkStringParser(
      responseAsSucceeded,
      msg => responseAsFailed(RedisIOException(Some(msg)))
    )
  }

  sealed trait DumpResponse extends CommandResponse

  case class DumpSucceeded(value: Option[String]) extends DumpResponse

  case class DumpFailed(ex: Exception) extends DumpResponse

  // --- EXISTS  ---------------------------------------------------------------------------------------------------

  case class ExistsRequest(key: String) extends CommandRequest {

    override def encodeAsString: String = s"EXISTS $key"

    override type ResultType = Int
    override type ResponseType = ExistsResponse

    override def responseAsSucceeded(arguments: Int): ExistsResponse =
      ExistsSucceeded(arguments)

    override def responseAsFailed(ex: Exception): ExistsResponse =
      ExistsFailed(ex)

    override val parser: CommandResponseParser[ExistsResponse] = new IntegerReplyParser(
      responseAsSucceeded,
      msg => responseAsFailed(RedisIOException(Some(msg)))
    )
  }

  sealed trait ExistsResponse extends CommandResponse

  case class ExistsSucceeded(value: Int) extends ExistsResponse

  case class ExistsFailed(ex: Exception) extends ExistsResponse

  // --- EXPIRE ---------------------------------------------------------------------------------------------------

  case class ExpireRequest(key: String, timeout: Long) extends CommandRequest {

    override def encodeAsString: String = s"EXPIRE $key $timeout"

    override type ResultType = Int

    override type ResponseType = ExpireResponse

    override def responseAsSucceeded(arguments: Int): ExpireResponse =
      ExpireSucceeded(arguments)

    override def responseAsFailed(ex: Exception): ExpireResponse =
      ExpireFailed(ex)

    override val parser: CommandResponseParser[ExpireResponse] = new IntegerReplyParser(
      responseAsSucceeded,
      msg => responseAsFailed(RedisIOException(Some(msg)))
    )
  }

  sealed trait ExpireResponse extends CommandResponse

  case class ExpireSucceeded(value: Int) extends ExpireResponse

  case class ExpireFailed(ex: Exception) extends ExpireResponse

  // --- EXPIREAT  ---------------------------------------------------------------------------------------------------

  case class ExpireAtRequest(key: String, unixTimeout: Long) extends CommandRequest {

    override def encodeAsString: String = ???

    override type ResultType = Int

    override type ResponseType = ExpireAtResponse

    override def responseAsSucceeded(arguments: Int): ExpireAtResponse =
      ExpireAtSucceeded(arguments)

    override def responseAsFailed(ex: Exception): ExpireAtResponse =
      ExpireAtFailed(ex)

    override val parser: CommandResponseParser[ExpireAtResponse] = new IntegerReplyParser(
      responseAsSucceeded,
      msg => responseAsFailed(RedisIOException(Some(msg)))
    )
  }

  sealed trait ExpireAtResponse extends CommandResponse

  case class ExpireAtSucceeded(value: Int) extends ExpireAtResponse

  case class ExpireAtFailed(ex: Exception) extends ExpireAtResponse

  // --- KEYS --------------------------------------------------------------------------------------------------------
  case class KeysRequest(keyPattern: String = "*") extends CommandRequest {

    override def encodeAsString: String = s"KEYS $keyPattern"

    override type ResultType = Seq[String]

    override type ResponseType = KeysResponse

    override def responseAsSucceeded(arguments: Seq[String]): KeysResponse =
      KeysSucceeded(arguments)

    override def responseAsFailed(ex: Exception): KeysResponse =
      KeysFailure(ex)

    override val parser: CommandResponseParser[ResponseType] = new StringArrayParser(
      responseAsSucceeded,
      msg => responseAsFailed(RedisIOException(Some(msg)))
    )
  }

  sealed trait KeysResponse extends CommandResponse

  case class KeysSucceeded(values: Seq[String]) extends KeysResponse

  case class KeysFailure(ex: Exception) extends KeysResponse

  // --- MIGRATE ------------------------------------------------------------------------------------------------------

  // --- MOVE ---------------------------------------------------------------------------------------------------------
  case class MoveRequest(key: String, index: Int) extends CommandRequest {

    override def encodeAsString: String = s"MOVE $key $index"

    override type ResultType = Int

    override type ResponseType = MoveResponse

    override def responseAsSucceeded(arguments: Int): MoveResponse =
      MoveSucceeded(arguments)

    override def responseAsFailed(ex: Exception): MoveResponse =
      MoveFailed(ex)

    override val parser: CommandResponseParser[MoveResponse] = new IntegerReplyParser(
      responseAsSucceeded,
      msg => responseAsFailed(RedisIOException(Some(msg)))
    )
  }

  sealed trait MoveResponse extends CommandResponse

  case class MoveSucceeded(value: Int) extends MoveResponse

  case class MoveFailed(ex: Exception) extends MoveResponse

  // --- OBJECT -------------------------------------------------------------------------------------------------------

  // --- PERSIST ------------------------------------------------------------------------------------------------------

  case class PersistRequest(key: String) extends CommandRequest {

    override def encodeAsString: String = s"PERSIST $key"

    override type ResultType = Int

    override type ResponseType = PersistResponse

    override def responseAsSucceeded(arguments: Int): PersistResponse =
      PersistSucceeded(arguments)

    override def responseAsFailed(ex: Exception): PersistResponse =
      PersistFailed(ex)

    override val parser: CommandResponseParser[PersistResponse] = new IntegerReplyParser(
      responseAsSucceeded,
      msg => responseAsFailed(RedisIOException(Some(msg)))
    )
  }

  sealed trait PersistResponse extends CommandResponse

  case class PersistSucceeded(value: Int) extends PersistResponse

  case class PersistFailed(ex: Exception) extends PersistResponse

  // --- PEXPIRE ------------------------------------------------------------------------------------------------------

  // --- PEXPIREAT ----------------------------------------------------------------------------------------------------

  // --- PTTL

  // --- RANDOMKEY
  object RandomKeyRequest {
    def apply(): RandomKeyRequest = RandomKeyRequestImpl

    def unapply(self: RandomKeyRequest): Option[Unit] = Some(())
  }

  trait RandomKeyRequest extends CommandRequest

  private object RandomKeyRequestImpl extends RandomKeyRequest {

    override def encodeAsString: String = "RANDOMKEY"

    override type ResultType = Option[String]
    override type ResponseType = RandomKeyResponse

    override def responseAsSucceeded(arguments: Option[String]): RandomKeyResponse =
      RandomKeySucceeded(arguments)

    override def responseAsFailed(ex: Exception): RandomKeyResponse =
      RandomKeyFailure(ex)

    override val parser: CommandResponseParser[RandomKeyResponse] = new BulkStringParser(
      responseAsSucceeded,
      msg => responseAsFailed(RedisIOException(Some(msg)))
    )
  }

  sealed trait RandomKeyResponse extends CommandResponse

  case class RandomKeySucceeded(value: Option[String]) extends RandomKeyResponse

  case class RandomKeyFailure(ex: Exception) extends RandomKeyResponse

  // --- RENAME -------------------------------------------------------------------------------------------------------

  case class RenameRequest(oldKey: String, newKey: String) extends CommandRequest {

    override def encodeAsString: String = s"RENAME $oldKey $newKey"

    override type ResultType = Unit

    override type ResponseType = RenameResponse

    override def responseAsSucceeded(arguments: Unit): RenameResponse =
      RenameSucceeded

    override def responseAsFailed(ex: Exception): RenameResponse =
      RenameFailed(ex)

    override val parser: CommandResponseParser[RenameResponse] = new UnitReplyParser(
      responseAsSucceeded,
      msg => responseAsFailed(RedisIOException(Some(msg)))
    )

  }

  sealed trait RenameResponse extends CommandResponse

  case object RenameSucceeded extends RenameResponse

  case class RenameFailed(ex: Exception) extends RenameResponse

  // --- RENAMENX -----------------------------------------------------------------------------------------------------
  case class RenameNxRequest(oldKey: String, newKey: String) extends CommandRequest {

    override def encodeAsString: String = s"RENAMENX $oldKey $newKey"

    override type ResultType = Int

    override type ResponseType = RenameNxResponse

    override def responseAsSucceeded(arguments: Int): RenameNxResponse =
      RenameNxSucceeded(arguments)

    override def responseAsFailed(ex: Exception): RenameNxResponse =
      RenameNxFailure(ex)

    override val parser: CommandResponseParser[RenameNxResponse] = new IntegerReplyParser(
      responseAsSucceeded,
      msg => responseAsFailed(RedisIOException(Some(msg)))
    )

  }

  sealed trait RenameNxResponse extends CommandResponse

  case class RenameNxSucceeded(value: Int) extends RenameNxResponse

  case class RenameNxFailure(ex: Exception) extends RenameNxResponse

  // --- RESTORE ------------------------------------------------------------------------------------------------------

  // --- SCAN ---------------------------------------------------------------------------------------------------------

  // --- SORT ---------------------------------------------------------------------------------------------------------

  // --- TTL ----------------------------------------------------------------------------------------------------------
  case class TTLRequest(key: String) extends CommandRequest {

    override def encodeAsString: String = s"TTL $key"

    override type ResultType = Int
    override type ResponseType = TTLResponse

    override def responseAsSucceeded(arguments: Int): TTLResponse =
      TTLSucceeded(arguments)

    override def responseAsFailed(ex: Exception): TTLResponse =
      TTLFailed(ex)

    override val parser: CommandResponseParser[TTLResponse] = new IntegerReplyParser(
      responseAsSucceeded,
      msg => responseAsFailed(RedisIOException(Some(msg)))
    )
  }

  sealed trait TTLResponse extends CommandResponse

  case class TTLSucceeded(value: Int) extends TTLResponse

  case class TTLFailed(ex: Exception) extends TTLResponse

  // --- TYPE ---------------------------------------------------------------------------------------------------------

  case class TypeRequest(key: String) extends CommandRequest {

    override def encodeAsString: String = s"TYPE $key"

    override type ResultType = ValueType.Value
    override type ResponseType = TypeResponse

    override def responseAsSucceeded(arguments: ValueType.Value): TypeResponse =
      TypeSucceeded(arguments)

    override def responseAsFailed(ex: Exception): TypeResponse =
      TypeFailed(ex)

    override val parser: CommandResponseParser[TypeResponse] = new SimpleReplyParser(
      typeString => responseAsSucceeded(ValueType.withName(typeString)),
      msg => responseAsFailed(RedisIOException(Some(msg)))
    )
  }

  sealed trait TypeResponse extends CommandResponse

  case class TypeSucceeded(value: ValueType.Value) extends TypeResponse

  case class TypeFailed(ex: Exception) extends TypeResponse

  // --- WAIT ---------------------------------------------------------------------------------------------------------

  case class WaitRequest(numSlaves: Int, timeout: Int) extends CommandRequest {

    override def encodeAsString: String = s"WAIT $numSlaves $timeout"

    override type ResultType = Int

    override type ResponseType = WaitResponse

    override def responseAsSucceeded(arguments: Int): WaitResponse =
      WaitSucceeded(arguments)

    override def responseAsFailed(ex: Exception): WaitResponse =
      WaitFailed(ex)

    override val parser: CommandResponseParser[WaitResponse] = new IntegerReplyParser(
      responseAsSucceeded,
      msg => responseAsFailed(RedisIOException(Some(msg)))
    )
  }

  sealed trait WaitResponse extends CommandResponse

  case class WaitSucceeded(value: Int) extends WaitResponse

  case class WaitFailed(ex: Exception) extends WaitResponse

}
