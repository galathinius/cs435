import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

import java.util.List;
import java.util.ArrayList;

public class Adjacency {

    public static class AdjacencyMapper extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] nodes = value.toString().split(" ");

            context.write(new Text(nodes[0]), new Text(nodes[1]));
            context.write(new Text(nodes[1]), new Text(nodes[0]));

        }
    }

    public static class AdjacencyReducer extends Reducer<Text, Text, Text, NullWritable> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            List<Text> valuesList = new ArrayList<Text>();
            for (Text val : values) {
                valuesList.add(new Text(val));
            }

            int valuesLength = valuesList.size();
            String valuesString = new String(key.toString());

            for (Text value : valuesList) {
                valuesString += ' ' + value.toString();
            }

            context.write(new Text(valuesString), NullWritable.get());

            // if (valuesLength > 1) {
            // for (int i = 0; i < valuesLength - 1; i++) {
            // for (int j = i + 1; j < valuesLength; ++j) {

            // String Adjacency = valuesList.get(i).toString() + ' ' + key.toString() + ' '
            // + valuesList.get(j).toString();
            // context.write(new Text(Adjacency), NullWritable.get());
            // }
            // }
            // }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Adjacency new");
        job.setJarByClass(Adjacency.class);
        job.setMapperClass(Adjacency.AdjacencyMapper.class);
        job.setReducerClass(Adjacency.AdjacencyReducer.class);
        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

// $HADOOP_HOME/bin/hadoop jar ~/pa2/t3/Adjacency/Adjacency.jar Adjacency
// ~/pa2/input
// ~/pa2/Adjacency-result