package qualitymetrics
import scalikejdbc.*
import scala.io.Source
import scalikejdbc.interpolation.SQLSyntax

def loadSQLFromFile(fileName: String): String = {
  val resourcePath = s"$fileName" // Note the leading slash for a resource path
  val source       = Source.fromResource(resourcePath)
  source.mkString
}

def checkIfTableExists(tableName: String, schemaName: String) =
  val query =
    sql"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = ${schemaName} AND table_name = ${tableName});"
  DB readOnly { implicit session =>
    query.map(rs => rs.boolean("exists")).single.apply().getOrElse(false)
  }

def escapeTable(schemaName: String, tableName: String): SQLSyntax =
  SQLSyntax.createUnsafely(s"\"${schemaName.replace("`", "")}\".\"${tableName.replace("`", "")}\"")

def getOneExample(tableNameIdentifier: SQLSyntax) =
  val query = sql"SELECT * FROM ${tableNameIdentifier} limit 1;"

  def toMap(rs: WrappedResultSet) =
    (1 to rs.metaData.getColumnCount()).map { (i) =>
      val label = rs.metaData.getColumnLabel(i)
      (label, rs.metaData.getColumnTypeName(i), Option(rs.any(label)))
    }

  DB.readOnly { implicit session =>
    query.map(rs => toMap(rs)).single.apply().getOrElse(Vector())
  }

def getTopFreq(tableNameIdentifier: SQLSyntax, columnName: String): Map[String, Any] =
  val query = SQL(s"""
        SELECT 
            DISTINCT "${columnName}" as top,
            COUNT("${columnName}") as frequency
            FROM ${tableNameIdentifier.value}
            GROUP BY top ORDER BY frequency desc 
            LIMIT 1
        """)
  DB.readOnly { implicit session =>
    query
      .map { rs =>
        Map("top" -> Option(rs.any("top")), "freq" -> Option(rs.any("frequency")))
      }
      .single
      .apply()
  }.get

def getMinMaxCount(tableNameIdentifier: SQLSyntax, columnName: String): Map[String, Any] =
  val query = SQL(s"""
        SELECT MIN("${columnName}") as min,
        MAX("${columnName}") as max,
        COUNT("${columnName}") as count
        FROM ${tableNameIdentifier.value}
        WHERE "${columnName}" IS NOT NULL
    """)
  DB.readOnly { implicit session =>
    query
      .map { rs =>
        Map(
          "min"   -> Option(rs.any("min")),
          "max"   -> Option(rs.any("max")),
          "count" -> Option(rs.any("count")))
      }
      .single
      .apply()
  }.get

def getUniqueCount(tableNameIdentifier: SQLSyntax, columnName: String): Map[String, Any] =
  val query = SQL(s"""
    SELECT
    COUNT(DISTINCT "${columnName}") as unique_count
    FROM ${tableNameIdentifier.value}
    WHERE "${columnName}" IS NOT NULL
  """)
  DB.readOnly { implicit session =>
    query.map { rs => Map("unique" -> Option(rs.any("unique_count"))) }.single.apply()
  }.get

def getAvg(tableNameIdentifier: SQLSyntax, columnName: String): Map[String, Any] =
  val query = SQL(s"""
    SELECT
    AVG ("${columnName}") as avg
    FROM ${tableNameIdentifier.value}
    WHERE "${columnName}" IS NOT NULL
  """)
  DB.readOnly { implicit session =>
    query.map { rs => Map("mean" -> Option(rs.any("avg"))) }.single.apply()
  }.get

def getStdPercentile(tableNameIdentifier: SQLSyntax, columnName: String): Map[String, Any] =
  val query = SQL(s"""
    SELECT
    stddev_pop("${columnName}") as key_std,
    percentile_disc(0.25) WITHIN GROUP (ORDER BY "${columnName}") as key_25,
    percentile_disc(0.50) WITHIN GROUP (ORDER BY "${columnName}") as key_50,
    percentile_disc(0.75) WITHIN GROUP (ORDER BY "${columnName}") as key_75
    FROM ${tableNameIdentifier.value}
  """)
  DB.readOnly { implicit session =>
    query
      .map { rs =>
        Map(
          "std" -> Option(rs.any("key_std")),
          "p25" -> Option(rs.any("key_25")),
          "p50" -> Option(rs.any("key_50")),
          "p75" -> Option(rs.any("key_75")))
      }
      .single
      .apply()
  }.get

def getStatsForNumericField(
    tableNameIdentifier: SQLSyntax,
    columnName: String
) =
  val map1 = getTopFreq(tableNameIdentifier, columnName)
  val map2 = getMinMaxCount(tableNameIdentifier, columnName)
  val map3 = getUniqueCount(tableNameIdentifier, columnName)
  val map4 = getAvg(tableNameIdentifier, columnName)
  val map5 = getStdPercentile(tableNameIdentifier, columnName)

  map1 ++ map2 ++ map3 ++ map4 ++ map5

def getStatsForStringField(
    tableNameIdentifier: SQLSyntax,
    columnName: String
) =
  val map1 = getTopFreq(tableNameIdentifier, columnName)
  val map2 = getMinMaxCount(tableNameIdentifier, columnName)
  val map3 = getUniqueCount(tableNameIdentifier, columnName)

  map1 ++ map2 ++ map3
