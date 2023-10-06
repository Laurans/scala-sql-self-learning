package qualitymetrics
import scalikejdbc._
import scala.io.Source

def loadSQLFromFile(fileName: String): String = {
    val resourcePath = s"$fileName" // Note the leading slash for a resource path
    val source = Source.fromResource(resourcePath)
    source.mkString
}

def checkIfTableExists(table_name: String, schema_name: String) =
    val query= sql"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = ${schema_name} AND table_name = ${table_name});"
    DB readOnly { implicit session => query.map(rs => rs.boolean("exists")).single.apply()}
