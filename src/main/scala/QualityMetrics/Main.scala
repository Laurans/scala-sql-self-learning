import scala.concurrent.{Future, Await}
import scala.concurrent.duration.Duration
import scalikejdbc._
import scalikejdbc.config._
import qualitymetrics.{checkIfTableExists, getOneExample, isNumeric, ColumnStats}

object SQLinspect:
    def get_table_statistics(table_name: String, schema_name: String) = 
        val isTableExists = checkIfTableExists(table_name = table_name, schema_name = schema_name)
        
        if isTableExists == true then
            val columns = getOneExample(s"$schema_name.$table_name")
            println(s"Columns for $schema_name.$table_name=$columns")
            //columns.map(col => (col._1, col._2, isNumeric(col._2, col._3))).foreach(t => println(s"$t"))
            columns.map{col =>  
                val stat = ColumnStats(col._1, col._2, col._3)
                stat.isNumber = isNumeric(stat.typed, stat.example)
                stat}.foreach(println)
        end if

        

@main def describe(table_name: String, schema_name: String, params: String*): Unit = {
    DBs.setupAll()
    SQLinspect.get_table_statistics(table_name, schema_name)
}