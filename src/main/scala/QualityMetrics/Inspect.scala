package qualitymetrics

def isNumeric(typed: String, value: Any): Boolean = 
    (typed, value) match
        case (_, _: Int) => true
        case (_, _: Float) => true
        case ("int4", _) => true
        case _ => false

case class ColumnStats(columnName: String, typed: String, example: Any, var isNumber: Boolean, var count: Int, var mmean: Float, var mmax: Float, var mmin: Float, var std: Float, var p25: Float, var p50: Float, var p75: Float, var unique: Int, var top: Any, var freq: Float)
//    def this(columnName: String, typed: String, example: Any) = this(columnName, typed, example, false, 0, 0, 0, 0, 0, 0, 0, 0, 0, "", 0)

case object ColumnStats:
    def apply(columnName: String, typed: String, example: Any): ColumnStats = 
        apply(columnName, typed, example, false, 0, 0, 0, 0, 0, 0, 0, 0, 0, "", 0)

