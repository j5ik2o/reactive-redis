package com.github.j5ik2o.reactive.redis.keys

import com.github.j5ik2o.reactive.redis._

object KeysProtocol {

  import BaseProtocol._

  // --- DEL  ---------------------------------------------------------------------------------------------------

  trait DelRequest extends CommandRequest {
    val keys: Seq[String]
  }

  object DelRequest {

    def apply(keys: Seq[String]): DelRequest = new DelRequestImpl(keys)

    def unapply(self: DelRequest): Option[Seq[String]] = Some(self.keys)

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

  }

  sealed trait DelResponse extends CommandResponse

  case class DelSucceeded(value: Int) extends DelResponse

  case class DelFailed(ex: Exception) extends DelResponse

  // --- DUMP  ---------------------------------------------------------------------------------------------------

  trait DumpRequest extends CommandRequest {
    val key: String
  }

  object DumpRequest {

    def apply(key: String): DumpRequest = new DumpRequestImpl(key)

    def unapply(self: DumpRequest): Option[String] = Some(self.key)

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

  }

  sealed trait DumpResponse extends CommandResponse

  case class DumpSucceeded(value: Option[String]) extends DumpResponse

  case class DumpFailed(ex: Exception) extends DumpResponse

  // --- EXISTS  ---------------------------------------------------------------------------------------------------

  trait ExistsRequest extends CommandRequest {
    val key: String
  }

  object ExistsRequest {

    def apply(key: String): ExistsRequest = new ExistsRequestImpl(key)

    def unapply(self: ExistsRequest): Option[String] = Some(self.key)

    private class ExistsRequestImpl(val key: String) extends ExistsRequest {

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

  }

  sealed trait ExistsResponse extends CommandResponse

  case class ExistsSucceeded(value: Int) extends ExistsResponse

  case class ExistsFailed(ex: Exception) extends ExistsResponse

  // --- EXPIRE ---------------------------------------------------------------------------------------------------

  trait ExpireRequest extends CommandRequest {
    val key: String
    val timeout: Long
  }

  object ExpireRequest {

    def apply(key: String, timeout: Long): ExpireRequest = new ExpireRequestImpl(key, timeout)

    def unapply(self: ExpireRequest): Option[(String, Long)] = Some(self.key, self.timeout)

    private class ExpireRequestImpl(val key: String, val timeout: Long) extends ExpireRequest {

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

  }

  sealed trait ExpireResponse extends CommandResponse

  case class ExpireSucceeded(value: Int) extends ExpireResponse

  case class ExpireFailed(ex: Exception) extends ExpireResponse

  // --- EXPIREAT  ---------------------------------------------------------------------------------------------------

  trait ExpireAtRequest extends CommandRequest {
    val key: String
    val timestamp: Long
  }

  object ExpireAtRequest {

    def apply(key: String, timestamp: Long): ExpireAtRequest = new ExpireAtRequestImpl(key, timestamp)

    def unapply(self: ExpireAtRequest): Option[(String, Long)] = Some(self.key, self.timestamp)

    private class ExpireAtRequestImpl(val key: String, val timestamp: Long) extends ExpireAtRequest {

      override def encodeAsString: String = s"EXPIREAT $key $timestamp"

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

  }

  sealed trait ExpireAtResponse extends CommandResponse

  case class ExpireAtSucceeded(value: Int) extends ExpireAtResponse

  case class ExpireAtFailed(ex: Exception) extends ExpireAtResponse

  // --- KEYS --------------------------------------------------------------------------------------------------------

  trait KeysRequest extends CommandRequest {
    val keyPattern: String
  }

  object KeysRequest {

    def apply(keyPattern: String = "*"): KeysRequest = new KeysRequestImpl(keyPattern)

    def unapply(self: KeysRequest): Option[String] = Some(self.keyPattern)

    private class KeysRequestImpl(val keyPattern: String = "*") extends KeysRequest {

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

  }

  sealed trait KeysResponse extends CommandResponse

  case class KeysSucceeded(values: Seq[String]) extends KeysResponse

  case class KeysFailure(ex: Exception) extends KeysResponse

  // --- MIGRATE ------------------------------------------------------------------------------------------------------

  // --- MOVE ---------------------------------------------------------------------------------------------------------

  trait MoveRequest extends CommandRequest {
    val key: String
    val index: Int
  }

  object MoveRequest {

    def apply(key: String, index: Int): MoveRequest = new MoveRequestImpl(key, index)

    def unapply(self: MoveRequest): Option[(String, Int)] = Some(self.key, self.index)

    private class MoveRequestImpl(val key: String, val index: Int) extends MoveRequest {

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

  }

  sealed trait MoveResponse extends CommandResponse

  case class MoveSucceeded(value: Int) extends MoveResponse

  case class MoveFailed(ex: Exception) extends MoveResponse

  // --- OBJECT -------------------------------------------------------------------------------------------------------

  // --- PERSIST ------------------------------------------------------------------------------------------------------

  trait PersistRequest extends CommandRequest {
    val key: String
  }

  object PersistRequest {

    def apply(key: String): PersistRequest = new PersistRequestImpl(key)

    def unapply(self: PersistRequest): Option[String] = Some(self.key)

    private class PersistRequestImpl(val key: String) extends PersistRequest {

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

  }

  sealed trait PersistResponse extends CommandResponse

  case class PersistSucceeded(value: Int) extends PersistResponse

  case class PersistFailed(ex: Exception) extends PersistResponse

  // --- PEXPIRE ------------------------------------------------------------------------------------------------------

  // --- PEXPIREAT ----------------------------------------------------------------------------------------------------

  // --- PTTL

  // --- RANDOMKEY

  trait RandomKeyRequest extends CommandRequest

  object RandomKeyRequest {

    def apply(): RandomKeyRequest = RandomKeyRequestImpl

    def unapply(self: RandomKeyRequest): Option[Unit] = Some(())

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

  }

  sealed trait RandomKeyResponse extends CommandResponse

  case class RandomKeySucceeded(value: Option[String]) extends RandomKeyResponse

  case class RandomKeyFailure(ex: Exception) extends RandomKeyResponse

  // --- RENAME -------------------------------------------------------------------------------------------------------

  trait RenameRequest extends CommandRequest {
    val oldKey: String
    val newKey: String
  }

  object RenameRequest {

    def apply(oldKey: String, newKey: String): RenameRequest = new RenameRequestImpl(oldKey, newKey)

    def unapply(self: RenameRequest): Option[(String, String)] = Some((self.oldKey, self.newKey))

    private class RenameRequestImpl(val oldKey: String, val newKey: String) extends RenameRequest {

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

  }

  sealed trait RenameResponse extends CommandResponse

  case object RenameSucceeded extends RenameResponse

  case class RenameFailed(ex: Exception) extends RenameResponse

  // --- RENAMENX -----------------------------------------------------------------------------------------------------

  trait RenameNxRequest extends CommandRequest {
    val oldKey: String
    val newKey: String
  }

  object RenameNxRequest {

    def apply(oldKey: String, newKey: String): RenameNxRequest = new RenameNxRequestImpl(oldKey, newKey)

    def unapply(self: RenameNxRequest): Option[(String, String)] = Some((self.oldKey, self.newKey))

    private class RenameNxRequestImpl(val oldKey: String, val newKey: String) extends RenameNxRequest {

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

  }

  sealed trait RenameNxResponse extends CommandResponse

  case class RenameNxSucceeded(value: Int) extends RenameNxResponse

  case class RenameNxFailure(ex: Exception) extends RenameNxResponse

  // --- RESTORE ------------------------------------------------------------------------------------------------------

  // --- SCAN ---------------------------------------------------------------------------------------------------------

  // --- SORT ---------------------------------------------------------------------------------------------------------

  // --- TTL ----------------------------------------------------------------------------------------------------------

  trait TTLRequest extends CommandRequest {
    val key: String
  }

  object TTLRequest {

    def apply(key: String): TTLRequest = new TTLRequestImpl(key)

    def unapply(self: TTLRequest): Option[String] = Some(self.key)

    private class TTLRequestImpl(val key: String) extends TTLRequest {

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

  }

  sealed trait TTLResponse extends CommandResponse

  case class TTLSucceeded(value: Int) extends TTLResponse

  case class TTLFailed(ex: Exception) extends TTLResponse

  // --- TYPE ---------------------------------------------------------------------------------------------------------

  trait TypeRequest extends CommandRequest {
    val key: String
  }

  object TypeRequest {

    def apply(key: String): TypeRequest = new TypeRequestImpl(key)

    def unapply(self: TypeRequest): Option[String] = Some(self.key)

    private class TypeRequestImpl(val key: String) extends TypeRequest {

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

  }

  sealed trait TypeResponse extends CommandResponse

  case class TypeSucceeded(value: ValueType.Value) extends TypeResponse

  case class TypeFailed(ex: Exception) extends TypeResponse

  // --- WAIT ---------------------------------------------------------------------------------------------------------

  trait WaitRequest extends CommandRequest {
    val numSlaves: Int
    val timeout: Int
  }

  object WaitRequest {

    def apply(numSlaves: Int, timeout: Int): WaitRequest = new WaitRequestImpl(numSlaves, timeout)

    def unapply(self: WaitRequest): Option[(Int, Int)] = Some(self.numSlaves, self.timeout)

    private class WaitRequestImpl(val numSlaves: Int, val timeout: Int) extends WaitRequest {

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

  }

  sealed trait WaitResponse extends CommandResponse

  case class WaitSucceeded(value: Int) extends WaitResponse

  case class WaitFailed(ex: Exception) extends WaitResponse

}
