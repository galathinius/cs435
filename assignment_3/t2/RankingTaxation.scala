import org.apache.spark.sql.SparkSession
// import reflect


object RankingTaxation {

  def main(args: Array[String]): Unit = {

    val sc = SparkSession.builder().master("spark://salem:30356").getOrCreate().sparkContext


    // val text = sc.textFile(args(0))
    val lines = sc.textFile(args(0), 10)
    val titles = sc.textFile(args(1),10).zipWithIndex().mapValues(x => x + 1).map(_.swap)
    val numOfPages = lines.count()
    val links = lines.map(s=>(s.split(": ")(0).toLong, s.split(": ")(1)))
    var ranks_2 = links.mapValues(v => 1.0 / numOfPages)  
    val beta = 0.85

    // ranks.saveAsTextFile(args(1))

    for ( i <- 1 to 25) {
        val tempRank_1 = links.join(ranks_2, 10).values.flatMap {  
            case (urls, rank) =>
            val outgoingLinks = urls.split(" ")
            outgoingLinks.map(url => (url.toLong, rank / outgoingLinks.length ))
        }
        // Updated ranks: { (B, _), (C, _), (D, _), (A, _), ... }
        ranks_2 = tempRank_1.reduceByKey(_+_)      
        ranks_2 = ranks_2.map(rec => (rec._1, rec._2 *beta + ((1-beta)/numOfPages) ))
        }
    //rddRanks.saveAsTextFile(args(2))
    //val sortedRanks = ranks.sortBy(_._2, false).take(10)
    val sortedRanks = ranks_2.sortBy(_._2, false).take(10)
    val rddRanks = sc.parallelize(sortedRanks)
    
  // ('a', (1, 2))    common key, left value, right value

    val titleRank = rddRanks.join(titles, 10)
    
    
    // println(rddRanks.getClass)

    
    val forOutput = titleRank.map {
        // .map (rec =>(rec._2._2, rec._2._1)) 
            rec => (rec._1 , rec._2._2, rec._2._1 )
            // val outgoingValue = id +" "+ rank
            // titles.map(title => (title, outgoingValue ))
        }

    val forOutputSorted = forOutput.sortBy(_._3, false)



       forOutputSorted.saveAsTextFile(args(2))

    // println(ranks.getClass)
    // val counts = text.flatMap(line => line.split(" ")
    // ).map(word => (word,1)).reduceByKey(_+_)
    // counts.saveAsTextFile(args(1))
  }
}

// $SPARK_HOME/bin/spark-submit --class Ranking --deploy-mode client --supervise target/scala-2.12/simple-project_2.12-1.0.jar /inputReal /outReal 



// (5302153,United_States,0.012148240057036516)
// (1921890,Geographic_coordinate_system,0.009798659806309465)
// (88822,2008,0.008768879900912463)
// (84707,2007,0.00820044316213305)
// (687324,Biography,0.0056510337316915925)
// (5300058,United_Kingdom,0.00452315823523726)
// (81615,2006,0.00447346852292637)
// (3492254,Music_genre,0.004270252707745092)
// (5308545,United_States_postal_abbreviations,0.004128535535847111)
// (1804986,France,0.004079017591796452)