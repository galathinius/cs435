import org.apache.spark.sql.SparkSession
// import reflect

object Ranking {

  def main(args: Array[String]): Unit = {

    val sc = SparkSession
      .builder()
      .master("spark://salem:30356")
      .getOrCreate()
      .sparkContext

    val lines = sc.textFile(args(0), 10)
    val titles =
      sc.textFile(args(1), 10).zipWithIndex().mapValues(x => x + 1).map(_.swap)
    val numOfPages = lines.count()
    val links = lines.map(s => (s.split(": ")(0).toLong, s.split(": ")(1)))
    var ranks = links.mapValues(v => 1.0 / numOfPages)

    for (i <- 1 to 25) {
      val tempRank = links.join(ranks, 10).values.flatMap { case (urls, rank) =>
        val outgoingLinks = urls.split(" ")
        outgoingLinks.map(url => (url.toLong, rank / outgoingLinks.length))
      }
      ranks = tempRank.reduceByKey(_ + _)
    }

    val sortedRanks = ranks.sortBy(_._2, false).take(10)
    val rddRanks = sc.parallelize(sortedRanks)

    val titleRank = rddRanks.join(titles, 10)

    val forOutput = titleRank.map { rec =>
      rec._2._2 + ' ' + rec._2._1 + ' ' + rec._1
    }
    forOutput.saveAsTextFile(args(2))

  }
}

// $SPARK_HOME/bin/spark-submit --class Ranking --deploy-mode client --supervise target/scala-2.12/simple-project_2.12-1.0.jar /inputReal /outReal
