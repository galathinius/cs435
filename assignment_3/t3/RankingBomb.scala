import org.apache.spark.sql.SparkSession

object RankingBomb {

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
    var ranks_2 = links.mapValues(v => 1.0 / numOfPages)
    val beta = 0.85
    val surfingTitles = titles.filter(rec => rec._2 contains "surfing")

    for (i <- 1 to 1) {
      val tempRank_1 =
        links.join(ranks_2, 10).values.flatMap { case (urls, rank) =>
          val outgoingLinks = urls.split(" ")
          outgoingLinks.map(url => (url.toLong, rank / outgoingLinks.length))
        }

      ranks_2 = tempRank_1.reduceByKey(_ + _)
      // ranks_2 =
      //   ranks_2.map(rec => (rec._1, rec._2 * beta + ((1 - beta) / numOfPages)))
    }

    val surfingRankings = surfingTitles.join(ranks_2, 10).map { rec =>
      (rec._1, rec._2._2, rec._2._1)
    }

    val sortedRanks = surfingRankings.sortBy(_._2, false).take(10)
    val rddRanks = sc.parallelize(sortedRanks)

    rddRanks.saveAsTextFile(args(2))

  }
}

// $SPARK_HOME/bin/spark-submit --class Ranking --deploy-mode client --supervise target/scala-2.12/simple-project_2.12-1.0.jar /inputReal /outReal
