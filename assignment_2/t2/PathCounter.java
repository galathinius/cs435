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
import java.util.ArrayList;
import java.util.Map;

import java.util.List;
import java.util.Map.Entry;
import java.util.Collections;
import static java.util.Comparator.comparing;
import static java.util.Comparator.reverseOrder;
import static java.util.stream.Collectors.toList;

public class PathCounter {

    public static class TopTenMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] nodes = value.toString().split(" ");
            String lastNode = nodes[nodes.length - 1];

            if (nodes[0].compareTo(lastNode) > 0) {
                context.write(new Text(lastNode + " " + nodes[0]), new IntWritable(nodes.length - 1));
            } else {
                context.write(new Text(nodes[0] + " " + lastNode), new IntWritable(nodes.length - 1));
            }

        }
    }

    public static class TopTenReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        // private Integer PathCounterhipCount;

        // @Override
        // protected void setup(Context context) {
        // PathCounterhipCount = new Integer(0);
        // // OutDegreeMap = new TreeMap<Text, Integer>();
        // }

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            // String valuesString = new String(key.toString());

            List<Integer> valuesList = new ArrayList<Integer>();

            for (IntWritable val : values) {
                valuesList.add(val.get());
                // valuesString += String.valueOf(val);
            }

            int minimum = Collections.min(valuesList);
            context.write(new Text(valuesString), new IntWritable(minimum));

        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "PathCounter new");
        job.setJarByClass(PathCounter.class);
        job.setMapperClass(PathCounter.TopTenMapper.class);
        job.setReducerClass(PathCounter.TopTenReducer.class);
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

// $HADOOP_HOME/bin/hadoop jar ~/pa2/t2/pathCounter/PathCounter.jar PathCounter
// ~/pa2/t2/pathGen/result ~/pa2/t2/pathCounter/result
