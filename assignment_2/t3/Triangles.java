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

public class Triangles {
    private final static IntWritable one = new IntWritable(1);

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
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Triangles new");
        job.setJarByClass(Triangles.class);
        job.setMapperClass(Triangles.TriangleMapper.class);
        job.setReducerClass(Triangles.TriangleReducer.class);
        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

// $HADOOP_HOME/bin/hadoop jar ~/pa2/t3/triangles/Triangles.jar Triangles
// ~/pa2/triples-result/part-r-00000 ~/pa2/triangles-result