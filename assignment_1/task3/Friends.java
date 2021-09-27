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
import java.util.TreeMap;
import java.util.Map;

import java.util.List;
import java.util.Map.Entry;
import static java.util.Comparator.comparing;
import static java.util.Comparator.reverseOrder;
import static java.util.stream.Collectors.toList;

public class Friends {

    public static class TopTenMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] nodes = value.toString().split(" ");

            if (nodes[0].compareTo(nodes[1]) > 0) {
                context.write(new Text(nodes[1] + " " + nodes[0]), one);
            } else {
                context.write(new Text(nodes[0] + " " + nodes[1]), one);
            }

        }
    }

    public static class TopTenReducer extends Reducer<Text, IntWritable, Text, NullWritable> {

        private Integer friendshipCount;

        @Override
        protected void setup(Context context) {
            friendshipCount = new Integer(0);
            // OutDegreeMap = new TreeMap<Text, Integer>();
        }

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int frCount = 0;

            for (IntWritable value : values) {
                frCount++;
            }
            if ((frCount == 2) && (friendshipCount < 100)) {
                friendshipCount++;
                context.write(key, NullWritable.get());
            }

        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Friends new");
        job.setJarByClass(Friends.class);
        job.setMapperClass(Friends.TopTenMapper.class);
        job.setReducerClass(Friends.TopTenReducer.class);
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
