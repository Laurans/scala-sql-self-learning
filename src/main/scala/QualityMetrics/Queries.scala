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

def getNbRows(tableNameIdentifier: SQLSyntax) =
  val query = sql"SELECT COUNT(*) as count FROM ${tableNameIdentifier};"

  DB.readOnly { implicit session =>
    query
      .map { rs => rs.int("count") }
      .single
      .apply()
  }.get

def getTopFreq(
    tableNameIdentifier: SQLSyntax,
    columnName: String
): Map[String, String | Int | Long | Float] =
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
        Map("top" -> rs.string("top"), "freq" -> rs.int("frequency"))
      }
      .single
      .apply()
  }.get

def getMinMaxCountNumeric(
    tableNameIdentifier: SQLSyntax,
    columnName: String
): Map[String, String | Int | Long | Float] =
  val query = SQL(s"""
        SELECT MIN("${columnName}") as min,
        MAX("${columnName}") as max,
        AVG ("${columnName}") as avg,
        COUNT("${columnName}") as not_null_count,
        COUNT(DISTINCT "${columnName}") as unique_count
        FROM ${tableNameIdentifier.value}
        WHERE "${columnName}" IS NOT NULL
    """)
  DB.readOnly { implicit session =>
    query
      .map { rs =>
        Map(
          "min"            -> rs.int("min"),
          "max"            -> rs.int("max"),
          "mean"           -> rs.float("avg"),
          "not_null_count" -> rs.int("not_null_count"),
          "unique_count"   -> rs.int("unique_count")
        )
      }
      .single
      .apply()
  }.get

def getMinMaxCountString(
    tableNameIdentifier: SQLSyntax,
    columnName: String
): Map[String, String | Int | Long | Float] =
  val query = SQL(s"""
        SELECT MIN(LENGTH("${columnName}")) as min,
        MAX(LENGTH("${columnName}")) as max,
        AVG(LENGTH("${columnName}")) as avg,
        COUNT("${columnName}") as not_null_count,
        stddev_pop(LENGTH("${columnName}")) as key_std,
        COUNT(DISTINCT "${columnName}") as unique_count
        FROM ${tableNameIdentifier.value}
        WHERE "${columnName}" IS NOT NULL
    """)
  DB.readOnly { implicit session =>
    query
      .map { rs =>
        Map(
          "min"            -> rs.int("min"),
          "max"            -> rs.int("max"),
          "mean"           -> rs.float("avg"),
          "not_null_count" -> rs.int("not_null_count"),
          "std"            -> rs.float("key_std"),
          "unique_count"   -> rs.int("unique_count")
        )
      }
      .single
      .apply()
  }.get

def getAdvancedStatString(
    tableNameIdentifier: SQLSyntax,
    columnName: String
): Map[String, String | Int | Long | Float] =

  def format_regex(regex: String) =
    s"SUM(CASE WHEN REGEXP_COUNT(\"${columnName}\", ${regex}, 1, 'i') != 0 THEN 1 ELSE 0 END)"

  val query = SQL(s"""SELECT 
    ${format_regex("'^([-+]?[0-9]+)$'")} AS text_int_count,
    ${format_regex("'^([-+]?[0-9]*[.]?[0-9]+([eE][-+]?[0-9]+)?)$'")} AS text_number_count,
    ${format_regex(
      "'^([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})$'"
    )} AS text_uuid_count,
    ${format_regex("'^(\\s+)$'")} as text_all_spaces_count,
    SUM(CASE WHEN UPPER(\"${columnName}\") IN ('NULL', 'NONE', 'NIL', 'NOTHING') THEN 1 ELSE 0 END) as text_null_keyword_count
    FROM ${tableNameIdentifier.value}
    """)
  DB.readOnly { implicit session =>
    query
      .map { rs =>
        Map(
          "text_int_count"          -> rs.int("text_int_count"),
          "text_number_count"       -> rs.int("text_number_count"),
          "text_uuid_count"         -> rs.int("text_uuid_count"),
          "text_all_spaces_count"   -> rs.int("text_all_spaces_count"),
          "text_null_keyword_count" -> rs.int("text_null_keyword_count")
        )
      }
      .single
      .apply()
  }.get

def getStdPercentileNumeric(
    tableNameIdentifier: SQLSyntax,
    columnName: String
): Map[String, String | Int | Long | Float] =
  val query = SQL(s"""
    SELECT
    stddev_pop("${columnName}") as key_std,
    percentile_disc(0.25) WITHIN GROUP (ORDER BY "${columnName}") as key_25,
    percentile_disc(0.50) WITHIN GROUP (ORDER BY "${columnName}") as key_50,
    percentile_disc(0.75) WITHIN GROUP (ORDER BY "${columnName}") as key_75,
    SUM(CASE WHEN "${columnName}" = 0 THEN 1 ELSE 0 END ) as zero_count,
    SUM(CASE WHEN "${columnName}" < 0 THEN 1 ELSE 0 END ) as negative_count
    FROM ${tableNameIdentifier.value}
  """)
  DB.readOnly { implicit session =>
    query
      .map { rs =>
        Map(
          "std"            -> rs.float("key_std"),
          "p25"            -> rs.float("key_25"),
          "p50"            -> rs.float("key_50"),
          "p75"            -> rs.float("key_75"),
          "zero_count"     -> rs.int("zero_count"),
          "negative_count" -> rs.int("negative_count")
        )
      }
      .single
      .apply()
  }.get

def getStatsForNumericField(
    tableNameIdentifier: SQLSyntax,
    columnName: String
): Map[String, String | Int | Long | Float] =
  val map1 = getTopFreq(tableNameIdentifier, columnName)
  val map2 = getMinMaxCountNumeric(tableNameIdentifier, columnName)
  val map3 = getStdPercentileNumeric(tableNameIdentifier, columnName)

  val res = map1 ++ map2 ++ map3
  res

def getStatsForStringField(
    tableNameIdentifier: SQLSyntax,
    columnName: String
): Map[String, String | Int | Long | Float] =
  val map1 = getTopFreq(tableNameIdentifier, columnName)
  val map2 = getMinMaxCountString(tableNameIdentifier, columnName)
  val map3 = getAdvancedStatString(tableNameIdentifier, columnName)

  val res = map1 ++ map2 ++ map3
  res
