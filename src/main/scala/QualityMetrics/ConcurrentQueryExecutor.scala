package qualitymetrics

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}
import scala.concurrent.ExecutionContext.Implicits.global
import concurrent.duration.DurationInt
import java.sql.DriverManager

def sleep(millis: Long) = Thread.sleep(millis)

object ConcurrentQueryExecutor:
    def executeQueries(queries: List[String]): Unit = {
        val futures: List[Future[Try[String]]] = queries.map(str => 
            Future {
                Try {
                    val duration = (math.random * 1000).toLong
                    sleep(duration)
                    val result = s"Result of $str"
                    if str  == "table3" then throw RuntimeException(s"$str -> $duration")
                    result
                }
            }
        )
        val aggregatedFuture: Future[List[Try[String]]] = Future.sequence(futures)
        aggregatedFuture.onComplete {
        case Success(results) =>
            println("All queries completed successfully!")
            results.foreach(result => println(s"Result: $result"))
        case Failure(throwable) =>
            println(s"FAILURE! $throwable")
        }

    }

    def executeQuery(query: String): Unit = {
        
    }