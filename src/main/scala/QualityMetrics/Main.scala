import scala.concurrent.{Future, Await}
import scala.concurrent.duration.Duration
import scalikejdbc._
import scalikejdbc.config._
import qualitymetrics.{checkIfTableExists}

object SQLinspect:
    def get_table_statistics(table_name: String, schema_name: String) = 
        val isTableExists = checkIfTableExists(table_name = table_name, schema_name = schema_name)
        println(isTableExists)

@main def describe(table_name: String, schema_name: String, params: String*): Unit = {
    DBs.setupAll()
    SQLinspect.get_table_statistics(table_name, schema_name)
}