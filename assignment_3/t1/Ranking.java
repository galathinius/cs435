import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.util.regex.Pattern;

import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.IndexedRow;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.Vector;

public class Ranking {
  private static final Pattern SPACE = Pattern.compile(" ");

  public static void main(String[] args) throws Exception {
    ArrayList<ArrayList<Integer>> M = new ArrayList();
    if (args.length < 2) {
      System.err.println("Usage: Ranking input output");
      System.exit(1);
    }

    SparkSession spark = SparkSession.builder().appName("Ranking").getOrCreate();

    JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();
    long lineCount = lines.count(); // for v0

    JavaRDD<IndexedRow> rows = lines.map(s -> {
      // System.out.println(" !!!!!!!!!");
      String[] keyAndNeighbors = s.split(": ");
      String node = keyAndNeighbors[0];
      String[] neighbors = keyAndNeighbors[1].split(" ");
      int k = neighbors.length;
      int[] neighbors_arrayList = new int[k];
      double[] p = new double[k];
      int i = 0;
      for (String c : neighbors) {
        neighbors_arrayList[i] = Integer.parseInt(c);
        p[i] = (1.0 / k);
        i++;
      }
      int line_Count = (int) lineCount;
      Integer index = Integer.parseInt(node);
      Vector v = Vectors.sparse(line_Count, neighbors_arrayList, p);
      IndexedRow row = new IndexedRow(index, v);
      return row;

    });

    // CoordinateMatrix adjMatrix = new CoordinateMatrix(words, lineCount,
    // lineCount);

    rows.saveAsTextFile(args[1]); // doesnt work of course
    // System.out.println(lineCount);
    // System.out.println(words.toString());
    // words.show();

    // words.map(s -> System.out.prints);

    spark.stop();
  }
}

// input row
// 2: 3 747213 1664968 1691047 4095634 5535664

// $SPARK_HOME/bin/spark-submit --class Ranking --deploy-mode client --supervise
// target/simple-project-1.0.jar /input /outR
