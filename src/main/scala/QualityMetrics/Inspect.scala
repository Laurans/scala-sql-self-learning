package qualitymetrics

import org.postgresql.jdbc.PgArray
import org.postgresql.util.PGobject
import java.util.Date

def isNumeric(typed: String, value: Any): Boolean =
  (typed, value) match
    case (_, _: Int)   => true
    case (_, _: Float) => true
    case ("int4", _)   => true
    case _             => false

def isObject(typed: String, value: Any): Boolean =
  (typed, value) match
    case (_, _: PGobject) => true
    case ("jsonb", _)     => true
    case _                => false

def isIterable(typed: String, value: Any): Boolean =
  (typed, value) match
    case (_, _: Seq[Any])                => true
    case (_, _: PgArray)                 => true
    case (str, _) if str.startsWith("_") => true
    case _                               => false

def isTime(typed: String, value: Any): Boolean =
  (typed, value) match
    case (_, _: Date)     => true
    case ("timestamp", _) => true
    case _                => false

def isString(typed: String, value: Any): Boolean =
  (typed, value) match
    case (_, _: String) => true
    case ("text", _)    => true
    case _              => false

case class ColumnStats(
    columnName: String,
    typed: String,
    example: Option[Any],
    var isNumber: Boolean = false,
    var isList: Boolean = false,
    var isDict: Boolean = false,
    var isDateTime: Boolean = false,
    var isString: Boolean = false,
    var stats: Map[String, Any] = Map(
      "count"  -> None,
      "mean"   -> None,
      "max"    -> None,
      "min"    -> None,
      "std"    -> None,
      "p25"    -> None,
      "p50"    -> None,
      "p75"    -> None,
      "unique" -> None,
      "top"    -> None,
      "freq"   -> None
    )
):
  def updateStats(newStats: Map[String, Any]) =
    this.stats = this.stats ++ newStats
