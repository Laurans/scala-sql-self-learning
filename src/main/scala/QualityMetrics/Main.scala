import scala.concurrent.{Future, Await}
import scala.concurrent.duration.Duration
import scalikejdbc.*
import scalikejdbc.config.*
import qualitymetrics.*

object SQLinspect:
  def get_table_statistics(schemaName: String, tableName: String) =
    val isTableExists =
      checkIfTableExists(tableName = tableName, schemaName = schemaName)

    if isTableExists then
      val tableIdentifier = escapeTable(schemaName, tableName)
      val columns         = getOneExample(tableIdentifier)

      columns
        .map { col =>
          val stat = ColumnStats(col._1, col._2, col._3)
          stat.isNumber = isNumeric(stat.typed, stat.example)
          stat.isDateTime = isTime(stat.typed, stat.example)
          stat.isList = isIterable(stat.typed, stat.example.getOrElse(None))
          stat.isDict = isObject(stat.typed, stat.example.getOrElse(None))
          stat.isString = isString(stat.typed, stat.example)
          stat
        }
        .map { colStats =>

          var dataMap: Map[String, Any] = Map.empty
          if colStats.isNumber then
            dataMap = getStatsForNumericField(tableIdentifier, colStats.columnName)
          else if colStats.isString then
            dataMap = getStatsForStringField(tableIdentifier, colStats.columnName)
          end if

          colStats.updateStats(dataMap)
          colStats
        }
        .map(println)
    end if

@main def describe(
    schemaName: String,
    tableName: String,
    params: String*
): Unit = {
  DBs.setupAll()
  SQLinspect.get_table_statistics(schemaName, tableName)
}
