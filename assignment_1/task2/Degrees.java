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

public class Degrees {
    private final static Text inDegree = new Text("i");
    private final static Text outDegree = new Text("o");

    public static class TopTenMapper extends Mapper<Object, Text, Text, Text> {
        // private final static Text inDegree = new Text("i");
        // private final static Text outDegree = new Text("o");

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] nodes = value.toString().split(" ");

            // context.write(new Text(nodes[0]), outDegree);
            context.write(new Text(nodes[1]), inDegree);

        }
    }

    public static class TopTenReducer extends Reducer<Text, Text, Text, Integer> {
        // Text, Integer
        // Integer, Text

        private TreeMap<Integer, Text> InDegreeMap;
        // private TreeMap<Text, Integer> OutDegreeMap;

        // private final static Text inDegree = new Text("i");
        // private final static Text outDegree = new Text("o");

        @Override
        protected void setup(Context context) {
            InDegreeMap = new TreeMap<Integer, Text>();
            // OutDegreeMap = new TreeMap<Text, Integer>();
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int inCount = 0;
            // int outCount = 0;

            for (Text value : values) {
                // if (value == inDegree) {
                inCount++;
                // }

                // if (value == outDegree) {
                // outCount++;
                // }
            }
            context.write(key, inCount);
            // context.write(key, outCount);

            InDegreeMap.put(inCount, key);
            // OutDegreeMap.put(key, outCount);

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

            // List<Entry<Text, Integer>> topIn10 = InDegreeMap.entrySet().stream()
            // .sorted(comparing(Entry::getValue, reverseOrder()))
            // .limit(10)
            // .collect(toList());

            int i = 0;
            while ((!InDegreeMap.isEmpty()) && (i < 100)) {

                i++;

                Map.Entry<Integer, Text> entry = InDegreeMap.lastEntry();
                int count = entry.getKey();
                Text node = entry.getValue();
                InDegreeMap.remove(count);
                context.write(node, count);
            }

        }
    }

    // $HADOOP_HOME/bin/hadoop com.sun.tools.javac.Main Degrees.java
    // jar cf Degrees.jar Degrees*.class
    // $HADOOP_HOME/bin/hadoop jar ~/pa1/t2/Degrees.jar Degrees ~/pa1/test
    // ~/pa1/test_result

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "degrees new");
        job.setJarByClass(Degrees.class);
        job.setMapperClass(Degrees.TopTenMapper.class);
        job.setReducerClass(Degrees.TopTenReducer.class);
        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Integer.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
