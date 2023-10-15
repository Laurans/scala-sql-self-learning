package qualitymetrics
import scalikejdbc._
import scala.io.Source
import scalikejdbc.interpolation.SQLSyntax

def loadSQLFromFile(fileName: String): String = {
    val resourcePath = s"$fileName" // Note the leading slash for a resource path
    val source = Source.fromResource(resourcePath)
    source.mkString
}

def checkIfTableExists(tableName: String, schemaName: String) =
    val query= sql"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = ${schemaName} AND table_name = ${tableName});"
    DB readOnly { implicit session => query.map(rs => rs.boolean("exists")).single.apply().getOrElse(false)}

def escapeTable(schemaName: String, tableName: String): SQLSyntax = SQLSyntax.createUnsafely(s"\"${schemaName.replace("`", "")}\".\"${tableName.replace("`", "")}\"")

def getOneExample(tableNameIdentifier: SQLSyntax) =
    val query = sql"SELECT * FROM ${tableNameIdentifier} limit 1;"

    def toMap(rs: WrappedResultSet) =
        (1 to rs.metaData.getColumnCount()).map{ (i) => 
            val label = rs.metaData.getColumnLabel(i)
            (label, rs.metaData.getColumnTypeName(i), rs.any(label)) 
        }

    DB.readOnly {implicit session => query.map(rs => toMap(rs)).single.apply().getOrElse(Vector())}


def getStatsForNumericField(tableNameIdentifier: SQLSyntax, columnName: String) = 
    var query = SQL(s"""
        SELECT 
            DISTINCT "${columnName}" as top,
            COUNT("${columnName}") as frequency
            FROM ${tableNameIdentifier.value}
            GROUP BY top ORDER BY frequency desc 
            LIMIT 1
        """)
    println(query)
    val map1 = DB.readOnly{implicit session =>
        query.map{rs => 
                Map("top" -> rs.any("top"), "frequency" -> rs.int("frequency"))}
                .single.apply()}

    
    query = SQL(s"""
        SELECT MIN("${columnName}") as min,
        MAX("${columnName}") as max,
        COUNT("${columnName}") as count
        FROM ${tableNameIdentifier.value}
        WHERE "${columnName}" IS NOT NULL
    """)
    val map2 = DB.readOnly{implicit session =>
        query.map{rs => 
                Map("mmin" -> rs.float("min"), "mmax" -> rs.float("max"), "count" -> rs.int("count"))}
                .single.apply()}
    map1 ++ map2


/* object Something extends SQLSyntaxSupport[Something] {
  override val columns = Seq("id", "name", "`limit`")
}
*/ 