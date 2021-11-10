import org.apache.spark.sql.SparkSession
// import reflect


object Ranking {

  def main(args: Array[String]): Unit = {

    val sc = SparkSession.builder().master("spark://salt-lake-city:30356").getOrCreate().sparkContext


    // val text = sc.textFile(args(0))
    val lines = sc.textFile(args(0), 10)
    val titles = sc.textFile(args(1),10).zipWithIndex().mapValues(x => x + 1)//.map(_.swap)
    val numOfPages = lines.count()
    val links = lines.map(s=>(s.split(": ")(0), s.split(": ")(1)))
    var ranks = links.mapValues(v => 1.0 / numOfPages)  

    // ranks.saveAsTextFile(args(1))

    for ( i <- 1 to 2) {
        val tempRank = links.join(ranks, 10).values.flatMap {  
            case (urls, rank) =>
            val outgoingLinks = urls.split(" ")
            outgoingLinks.map(url => (url, rank / outgoingLinks.length))
        }
        // Updated ranks: { (B, _), (C, _), (D, _), (A, _), ... }  
        ranks = tempRank.reduceByKey(_+_)
        }
    //rddRanks.saveAsTextFile(args(2))
    val sortedRanks = ranks.sortBy(_._2, false).take(10)
    val rddRanks = sc.parallelize(sortedRanks)
    


    val tempRank = titles.join(rddRanks, 10).values.flatMap {  
            case (id, rank) =>
            val outgoingValue = id +" "+ rank
            titles.map(title => (title, outgoingValue ))
        }
       titles.saveAsTextFile(args(2))
    // println(ranks.getClass)
    // val counts = text.flatMap(line => line.split(" ")
    // ).map(word => (word,1)).reduceByKey(_+_)
    // counts.saveAsTextFile(args(1))
  }
}

// $SPARK_HOME/bin/spark-submit --class Ranking --deploy-mode client --supervise target/scala-2.12/simple-project_2.12-1.0.jar /inputReal /outReal 