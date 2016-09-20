package com.github.j5ik2o.reactive.redis.server

import com.github.j5ik2o.reactive.redis.CommandResponseParser._
import com.github.j5ik2o.reactive.redis._

object ServerProtocol {

  // --- BGREWRITEAOF
  // --- BGSAVE
  object BgSaveRequest {

    def apply(): BgSaveRequest = BgSaveRequestImpl

    def unapply(self: BgSaveRequest): Option[Unit] = Some(())

  }

  trait BgSaveRequest extends CommandRequest

  private object BgSaveRequestImpl extends BgSaveRequest {

    class Parser extends CommandResponseParser[ResponseType] {
      override protected val responseParser: Parser[BgSaveResponse] = {
        simpleWithCrLfOrErrorWithCrLf ^^ {
          case SimpleExpr(msg) =>
            responseAsSucceeded(())
          case ErrorExpr(msg) =>
            responseAsFailed(RedisIOException(Some(msg)))
          case _ =>
            sys.error("it's unexpected.")
        }
      }
    }

    override def encodeAsString: String = "BGSAVE"

    override type ResultType = Unit

    override type ResponseType = BgSaveResponse

    override def responseAsSucceeded(arguments: Unit): BgSaveResponse = BgSaveSucceeded

    override def responseAsFailed(ex: Exception): BgSaveResponse = BgSaveFailed(ex)

    override val parser: CommandResponseParser[BgSaveResponse] = new Parser()
  }

  sealed trait BgSaveResponse extends CommandResponse

  case object BgSaveSucceeded extends BgSaveResponse

  case class BgSaveFailed(ex: Exception) extends BgSaveResponse

  // --- CLIENT GETNAME -----------------------------------------------------------------------------------------------
  // --- CLIENT KILL --------------------------------------------------------------------------------------------------
  // --- CLIENT LIST --------------------------------------------------------------------------------------------------
  // --- CLIENT PAUSE -------------------------------------------------------------------------------------------------
  // --- CLIENT REPLY -------------------------------------------------------------------------------------------------
  // --- CLIENT SETNAME -----------------------------------------------------------------------------------------------
  // --- COMMAND ------------------------------------------------------------------------------------------------------
  // --- COMMAND COUNT ------------------------------------------------------------------------------------------------
  // --- COMMAND GETKEYS ----------------------------------------------------------------------------------------------
  // --- COMMAND INFO -------------------------------------------------------------------------------------------------
  // --- CONFIG GET ---------------------------------------------------------------------------------------------------
  // --- CONFIG RESETSTAT ---------------------------------------------------------------------------------------------
  // --- CONFIG REWRITE -----------------------------------------------------------------------------------------------
  // --- CONFIG SET ---------------------------------------------------------------------------------------------------

  // --- DBSIZE -------------------------------------------------------------------------------------------------------
  object DBSizeRequest {

    def apply(): DBSizeRequest = DBSizeRequestImpl

    def unapply(self: DBSizeRequest): Option[Unit] = Some(())

  }

  trait DBSizeRequest extends CommandRequest

  object DBSizeRequestImpl extends DBSizeRequest {

    class Parser extends CommandResponseParser[ResponseType] {
      override protected val responseParser: Parser[DBSizeResponse] = {
        numberWithCrLfOrErrorWithCrLf ^^ {
          case NumberExpr(n) =>
            responseAsSucceeded(n)
          case ErrorExpr(msg) =>
            responseAsFailed(RedisIOException(Some(msg)))
          case _ =>
            sys.error("it's unexpected.")
        }
      }
    }

    override type ResultType = Int

    override type ResponseType = DBSizeResponse

    override val encodeAsString: String = "DBSIZE"

    override def responseAsSucceeded(arguments: Int): DBSizeResponse = DBSizeSucceeded(arguments)

    override def responseAsFailed(ex: Exception): DBSizeResponse = DBSizeFailed(ex)

    override lazy val parser: CommandResponseParser[DBSizeResponse] = new Parser()
  }

  sealed trait DBSizeResponse extends CommandResponse

  case class DBSizeSucceeded(value: Int) extends DBSizeResponse

  case class DBSizeFailed(ex: Exception) extends DBSizeResponse

  // --- DEBUG OBJECT -------------------------------------------------------------------------------------------------
  // --- DEBUG SEGFAULT -----------------------------------------------------------------------------------------------

  // --- FLUSHALL -----------------------------------------------------------------------------------------------------
  object FlushAllRequest {
    def apply(): FlushAllRequest = FlushAllRequestImpl

    def unapply(self: FlushAllRequest): Option[Unit] = Some(())
  }

  trait FlushAllRequest extends CommandRequest

  object FlushAllRequestImpl extends FlushAllRequest {

    class Parser extends CommandResponseParser[ResponseType] {
      override protected val responseParser: Parser[FlushAllResponse] = {
        simpleWithCrLfOrErrorWithCrLf ^^ {
          case SimpleExpr(msg) =>
            responseAsSucceeded(())
          case ErrorExpr(msg) =>
            responseAsFailed(RedisIOException(Some(msg)))
          case _ =>
            sys.error("it's unexpected.")
        }
      }
    }

    override def encodeAsString: String = "FLUSHALL"

    override type ResultType = Unit
    override type ResponseType = FlushAllResponse

    override def responseAsSucceeded(arguments: Unit): FlushAllResponse =
      FlushAllSucceeded

    override def responseAsFailed(ex: Exception): FlushAllResponse =
      FlushAllFailed(ex)

    override val parser: CommandResponseParser[FlushAllResponse] = new Parser()
  }

  sealed trait FlushAllResponse extends CommandResponse

  case object FlushAllSucceeded extends FlushAllResponse

  case class FlushAllFailed(ex: Exception) extends FlushAllResponse

  // --- FLUSHDB ------------------------------------------------------------------------------------------------------
  object FlushDBRequest {

    def apply(): FlushDBRequest = FlushDBRequestImpl

    def unapply(self: FlushDBRequest): Option[Unit] = Some(())

  }

  trait FlushDBRequest extends CommandRequest

  object FlushDBRequestImpl extends FlushDBRequest {

    class Parser extends CommandResponseParser[ResponseType] {
      override protected val responseParser: Parser[FlushDBResponse] = {
        simpleWithCrLfOrErrorWithCrLf ^^ {
          case ErrorExpr(msg) =>
            responseAsFailed(RedisIOException(Some(msg)))
          case SimpleExpr(msg) =>
            responseAsSucceeded(())
          case _ =>
            sys.error("it's unexpected.")
        }
      }
    }

    override def encodeAsString: String = "FLUSHDB"

    override type ResultType = Unit
    override type ResponseType = FlushDBResponse

    override def responseAsSucceeded(arguments: Unit): FlushDBResponse =
      FlushDBSucceeded

    override def responseAsFailed(ex: Exception): FlushDBResponse =
      FlushDBFailed(ex)

    override val parser: CommandResponseParser[FlushDBResponse] = new Parser()
  }

  sealed trait FlushDBResponse extends CommandResponse

  case object FlushDBSucceeded extends FlushDBResponse

  case class FlushDBFailed(ex: Exception) extends FlushDBResponse

  // --- INFO ---------------------------------------------------------------------------------------------------------
  object InfoRequest {

    def apply(): InfoRequest = InfoRequestImpl

    def unapply(self: InfoRequest): Option[Unit] = Some(())

  }

  trait InfoRequest extends CommandRequest

  private object InfoRequestImpl extends InfoRequest {

    class Parser extends CommandResponseParser[ResponseType] {
      override protected val responseParser: Parser[InfoResponse] = {
        bulkStringWithCrLfOrErrorWithCrLf ^^ {
          case StringOptExpr(value) =>
            responseAsSucceeded(value.get)
          case ErrorExpr(msg) =>
            responseAsFailed(RedisIOException(Some(msg)))
          case _ =>
            sys.error("it's unexpected.")
        }
      }
    }

    override def encodeAsString: String = "INFO"

    override type ResultType = String

    override type ResponseType = InfoResponse

    override def responseAsSucceeded(arguments: String): InfoResponse =
      InfoSucceeded(arguments)

    override def responseAsFailed(ex: Exception): InfoResponse =
      InfoFailed(ex)

    override val parser: CommandResponseParser[InfoResponse] = new Parser()
  }

  sealed trait InfoResponse extends CommandResponse

  case class InfoSucceeded(value: String) extends InfoResponse

  case class InfoFailed(ex: Exception) extends InfoResponse

  // --- LASTSAVE -----------------------------------------------------------------------------------------------------
  // --- MONITOR ------------------------------------------------------------------------------------------------------
  // --- ROLE ---------------------------------------------------------------------------------------------------------
  // --- SAVE ---------------------------------------------------------------------------------------------------------
  // --- SHUTDOWN -----------------------------------------------------------------------------------------------------
  object ShutdownRequest {
    def apply(save: Boolean): ShutdownRequest = new ShutdownRequestImpl(save)

    def unapply(self: ShutdownRequest): Option[Boolean] = Some(self.save)
  }

  trait ShutdownRequest extends CommandRequest {
    val save: Boolean
  }

  private class ShutdownRequestImpl(val save: Boolean) extends ShutdownRequest {

    class Parser extends CommandResponseParser[ResponseType] {
      override protected val responseParser: Parser[ShutdownResponse] = {
        simpleWithCrLfOrErrorWithCrLf ^^ {
          case SimpleExpr(msg) =>
            responseAsSucceeded(())
          case ErrorExpr(msg) =>
            responseAsFailed(RedisIOException(Some(msg)))
          case _ =>
            sys.error("it's unexpected.")
        }
      }
    }

    val saveOption = if (save) "SAVE" else "NOSAVE"

    override def encodeAsString: String = s"SHUTDOWN $saveOption"

    override type ResultType = Unit
    override type ResponseType = ShutdownResponse

    override def responseAsSucceeded(arguments: Unit): ShutdownResponse =
      ShutdownSucceeded

    override def responseAsFailed(ex: Exception): ShutdownResponse =
      ShutdownFailed(ex)

    override val parser: CommandResponseParser[ShutdownResponse] = new Parser()
  }

  sealed trait ShutdownResponse extends CommandResponse

  case object ShutdownSucceeded extends ShutdownResponse

  case class ShutdownFailed(ex: Exception) extends ShutdownResponse

  // --- SLAVEOF ------------------------------------------------------------------------------------------------------
  // --- SLOWLOG ------------------------------------------------------------------------------------------------------
  // --- SYNC ---------------------------------------------------------------------------------------------------------

  // --- TIME ---------------------------------------------------------------------------------------------------------
  object TimeRequest {
    def apply(): TimeRequest = TimeRequestImpl

    def unapply(self: TimeRequest): Option[Unit] = Some(())
  }

  trait TimeRequest extends CommandRequest

  object TimeRequestImpl extends TimeRequest {

    class Parser extends CommandResponseParser[ResponseType] {
      override protected val responseParser: Parser[TimeResponse] = {
        stringArrayWithCrLfOrErrorWithCrLf ^^ {
          case ArrayExpr(values) =>
            val valuesTyped = values.asInstanceOf[Seq[StringExpr]]
            responseAsSucceeded((valuesTyped(0).value.toInt, valuesTyped(1).value.toInt))
          case ErrorExpr(msg) =>
            responseAsFailed(RedisIOException(Some(msg)))
          case _ =>
            sys.error("it's unexpected.")
        }
      }
    }

    override def encodeAsString: String = "TIME"

    override type ResultType = (Int, Int)

    override type ResponseType = TimeResponse

    override def responseAsSucceeded(arguments: (Int, Int)): TimeResponse =
      TimeSucceeded(arguments._1, arguments._2)

    override def responseAsFailed(ex: Exception): TimeResponse =
      TimeFailed(ex)

    override val parser: CommandResponseParser[TimeResponse] = new Parser()
  }

  sealed trait TimeResponse extends CommandResponse

  case class TimeSucceeded(unixTime: Int, millis: Int) extends TimeResponse

  case class TimeFailed(ex: Exception) extends TimeResponse

}
