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
    case ("varchar", _) => true
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
    var stats: Map[String, String | Int | Long | Float] = Map.empty
):
  def updateStats(newStats: Map[String, String | Int | Long | Float]) =
    this.stats = this.stats ++ newStats

  def getDataQualityRatio(nbRows: Int) =
    if nbRows > 0 then
      stats ++ Seq(
        "completeness" -> stats
          .get("not_null_count")
          .getOrElse(0)
          .asInstanceOf[Int] / nbRows,
        "distinctness" -> stats.get("unique_count").getOrElse(0).asInstanceOf[Int] / nbRows
      )
      if isNumber then
        stats ++ Seq(
          "zero_rate"     -> stats.get("zero_count").getOrElse(0).asInstanceOf[Int] / nbRows,
          "negative_rate" -> stats.get("negative_count").getOrElse(0).asInstanceOf[Int] / nbRows
        )
      else if isString then
        stats ++ Seq(
          "text_int_rate" -> stats.get("text_int_count").getOrElse(0).asInstanceOf[Int] / nbRows,
          "text_number_rate" -> stats
            .get("text_number_count")
            .getOrElse(0)
            .asInstanceOf[Int] / nbRows,
          "text_uuid_rate" -> stats.get("text_uuid_count").getOrElse(0).asInstanceOf[Int] / nbRows,
          "text_all_spaces_rate" -> stats
            .get("text_all_spaces_count")
            .getOrElse(0)
            .asInstanceOf[Int] / nbRows,
          "text_null_keyword_rate" -> stats
            .get("text_null_keyword_count")
            .getOrElse(0)
            .asInstanceOf[Int] / nbRows
        )
      end if
    end if

  def transformToCSVRow(): List[String | Int | Long | Float] =
    val columns = List(
      "top",
      "freq",
      "min",
      "max",
      "mean",
      "std",
      "completeness",
      "distinctness",
      "p25",
      "p50",
      "p75",
      "zero_rate",
      "negative_rate",
      "text_int_rate",
      "text_number_rate",
      "text_uuid_rate",
      "text_all_spaces_rate",
      "text_null_keyword_rate"
    )
    List(this.columnName, this.typed) ++ columns.map { s =>
      this.stats.get(s).getOrElse("")
    }
