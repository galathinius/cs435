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
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

public class Edges {

    public static class EdgesMapper extends Mapper<Object, Text, IntWritable, NullWritable> {
        private final static IntWritable one = new IntWritable(1);

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            context.write(one, NullWritable.get());

        }
    }

    public static class CountReducer extends Reducer<IntWritable, NullWritable, IntWritable, NullWritable> {

        @Override
        protected void reduce(IntWritable key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {
            int counter = 0;
            for (Object i : values) {
                counter++;
            }

            IntWritable noOfEdges = new IntWritable(counter);
            context.write(noOfEdges, NullWritable.get());

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "edges new");
        job.setJarByClass(Edges.class);
        job.setMapperClass(Edges.EdgesMapper.class);
        job.setReducerClass(Edges.CountReducer.class);
        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
