import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
// import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;

public class Tritri {
    private final static IntWritable one = new IntWritable(1);

    public static class TripleMapper extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] nodes = value.toString().split(" ");

            context.write(new Text(nodes[0]), new Text(nodes[1]));
            context.write(new Text(nodes[1]), new Text(nodes[0]));

        }
    }

    public static class TripleReducer extends Reducer<Text, Text, Text, NullWritable> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            List<Text> valuesList = new ArrayList<Text>();
            for (Text val : values) {
                valuesList.add(new Text(val));
            }

            int valuesLength = valuesList.size();
            String valuesString = new String(key.toString());

            for (Text value : valuesList) {
                valuesString += value.toString();
            }

            // if (valuesLength > 1) {
            for (int i = 0; i < valuesLength - 1; i++) {
                for (int j = i + 1; j < valuesLength; ++j) {

                    String triple = valuesList.get(i).toString() + ' ' + key.toString() + ' '
                            + valuesList.get(j).toString();
                    context.write(new Text(triple), NullWritable.get());
                }
            }
            // }
        }
    }

    public static class TriangleMapper extends Mapper<Object, Text, Text, IntWritable> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] nodes = value.toString().split(" ");
            Arrays.sort(nodes);

            context.write(new Text(nodes[0] + nodes[1] + nodes[2]), one);
            // context.write(new Text(nodes[1]), new Text(nodes[0]));

        }
    }

    public static class TriangleReducer extends Reducer<Text, IntWritable, Text, NullWritable> {

        private int tripleCount;
        private int triangleCount;

        @Override
        protected void setup(Context context) {
            tripleCount = 0;
            triangleCount = 0;

        }

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            // List<Text> valuesList = new ArrayList<Text>();
            int valCount = 0;
            for (IntWritable val : values) {
                tripleCount++;
                valCount++;
            }

            if (valCount == 3) {
                triangleCount++;
            }

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            double clusterCoef = 3.0 * triangleCount / tripleCount;
            context.write(new Text(String.valueOf(tripleCount)), NullWritable.get());
            context.write(new Text(String.valueOf(triangleCount)), NullWritable.get());
            context.write(new Text(String.valueOf(clusterCoef)), NullWritable.get());

        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Triples new");
        job1.setJarByClass(Tritri.class);
        job1.setMapperClass(Tritri.TripleMapper.class);
        job1.setReducerClass(Tritri.TripleReducer.class);
        job1.setNumReduceTasks(1);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        job1.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Tritri new");
        job2.setJarByClass(Tritri.class);
        job2.setMapperClass(Tritri.TriangleMapper.class);
        job2.setReducerClass(Tritri.TriangleReducer.class);
        job2.setNumReduceTasks(1);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job2, new Path(args[1])); // + "/part-r-00000"
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);

    }
}

// $HADOOP_HOME/bin/hadoop jar ~/pa2/t3/tritri/Tritri.jar Tritri ~/pa2/input
// ~/pa2/t3/tritri/triples ~/pa2/t3/tritri/triangles/