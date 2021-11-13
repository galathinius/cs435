#!/bin/bash
set -e

echo "Compiling the program..."
# Compile the program
sbt package
echo "Deleting output directory..."
# Delete the output directory
$HADOOP_HOME/bin/hadoop fs -rm -r /outReal
echo "Running the program..."
# Run the program...

# /inputReal/links-simple-sorted.txt /inputReal/titles-sorted.txt /outReal 
#$SPARK_HOME/bin/spark-submit --class RankingTaxation --deploy-mode client --supervise target/scala-2.12/simple-project_2.12-1.0.jar /input/testInput.txt /input/testTitles.txt /outTest 
$SPARK_HOME/bin/spark-submit --class RankingTaxation --deploy-mode client --supervise target/scala-2.12/simple-project_2.12-1.0.jar /inputReal/links-simple-sorted.txt /inputReal/titles-sorted.txt /outReal 

echo "Showing the output ..."
$HADOOP_HOME/bin/hadoop fs -cat /outReal/*