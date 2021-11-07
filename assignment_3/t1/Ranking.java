import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;

public class Ranking {
  private static final Pattern SPACE = Pattern.compile(" ");

  public static void main(String[] args) throws Exception {

    if (args.length < 2) {
      System.err.println("Usage: Ranking input output");
      System.exit(1);
    }

    SparkSession spark = SparkSession.builder().appName("Ranking").getOrCreate();

    JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();
    long lineCount = lines.count(); // for v0

    JavaRDD<MatrixEntry> words = lines.flatMap(s -> {

      String[] keyAndNeighbors = s.split(": ");
      String[] neighbors = keyAndNeighbors[1].split(" ");
      int neighboursCount = neighbors.length;

      // MatrixEntry[] maybe?
      JavaRDD<MatrixEntry> neighboursAsMatrixE = neighbors
          .map(n -> MatrixEntry(Integer.parseInt(keyAndNeighbors[0]), Integer.parseInt(n), 1 / neighboursCount));

      return neighboursAsMatrixE;
    });

    CoordinateMatrix adjMatrix = new CoordinateMatrix(words, lineCount, lineCount);

    adjMatrix.saveAsTextFile(args[1]); // doesnt work of course
    // System.out.println(lineCount);
    spark.stop();
  }
}

// input row
// 2: 3 747213 1664968 1691047 4095634 5535664

// $SPARK_HOME/bin/spark-submit --class Ranking --deploy-mode client --supervise
// target/simple-project-1.0.jar /input /outR
