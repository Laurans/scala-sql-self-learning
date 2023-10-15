import scala.concurrent.{Future, Await}
import scala.concurrent.duration.Duration
import scalikejdbc.*
import scalikejdbc.config.*
import qualitymetrics.*

object SQLinspect:
    def get_table_statistics(schemaName: String, tableName: String) = 
        val isTableExists = checkIfTableExists(tableName = tableName, schemaName = schemaName)
        
        if isTableExists == true then
            val tableIdentifier = escapeTable(schemaName, tableName)
            val columns = getOneExample(tableIdentifier)

            columns.map{col =>  
                val stat = ColumnStats(col._1, col._2, col._3)
                stat.isNumber = isNumeric(stat.typed, stat.example)
                stat}.map{
                    stat =>
                        if stat.isNumber then
                            val dataMap = getStatsForNumericField(tableIdentifier, stat.columnName)
                            val updatedStat = Nil
                        end if
                        stat
                }
        end if


@main def describe(schemaName: String, tableName: String, params: String*): Unit = {
    DBs.setupAll()
    SQLinspect.get_table_statistics(schemaName, tableName)
}
