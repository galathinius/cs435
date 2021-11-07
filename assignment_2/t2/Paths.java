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

import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.ArrayList;

public class Paths {
    private static Text one = new Text(new String("a"));

    public static class PathsMapper extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] nodes = value.toString().split(" ");

            context.write(one, value);
            context.write(new Text(nodes[0]), new Text(nodes[1]));
            context.write(new Text(nodes[1]), new Text(nodes[0]));

        }
    }

    public static class PathsReducer extends Reducer<Text, Text, Text, NullWritable> {

        // private int tripleCount;
        // private int triangleCount;
        private HashMap<Text, List<Text>> adjMap;
        private List<List<Text>> pathsList;

        @Override
        protected void setup(Context context) {
            // tripleCount = 0;
            // triangleCount = 0;
            adjMap = new HashMap<Text, List<Text>>();
            pathsList = new ArrayList<List<Text>>();
            for (int i = 0; i < 3; i++) {
                List<Text> temp = new ArrayList<Text>();
                pathsList.add(temp);
            }
            // List<Text> temp = new ArrayList<Text>();
            // pathsList.add(temp);

        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            List<Text> valuesList = new ArrayList<Text>();
            List<Text> tripleList = new ArrayList<Text>();

            for (Text val : values) {

                if (key.toString().compareTo(one.toString()) == 0) {
                    context.write(new Text(val), NullWritable.get());
                    continue;
                }

                valuesList.add(new Text(val));
            }

            if (key.toString().compareTo(one.toString()) == 0) {

                return;
            }

            // write to the adjacency list
            adjMap.put(new Text(key), valuesList);

            int valuesLength = valuesList.size();
            String valuesString = new String(key.toString());

            for (Text value : valuesList) {
                valuesString += value.toString();
            }

            // create triples
            for (int i = 0; i < valuesLength - 1; i++) {
                for (int j = i + 1; j < valuesLength; ++j) {

                    String triple = valuesList.get(i).toString() + ' ' + key.toString() + ' '
                            + valuesList.get(j).toString();
                    context.write(new Text(triple), NullWritable.get());
                    tripleList.add(new Text(triple));

                }
            }

            // add triples to existing paths
            for (Text triple : tripleList) {
                pathsList.get(0).add(triple);
            }

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

            for (int i = 0; i < 3; i++) {
                // List<Text> temp = new ArrayList<Text>();
                // pathsList.add(temp);

                // for each existing path
                for (Text path : pathsList.get(i)) {
                    String pathAsString = path.toString();
                    String firstChar = pathAsString.substring(0, 1);
                    String lastChar = pathAsString.substring(pathAsString.length() - 1, pathAsString.length());

                    List<Text> firstNeighs = adjMap.get(new Text(firstChar));
                    List<Text> lastNeighs = adjMap.get(new Text(lastChar));

                    for (Text firstNeigh : firstNeighs) {
                        String fNeighString = firstNeigh.toString();
                        String fNewPath = pathAsString;

                        if (!pathAsString.contains(fNeighString)) {
                            fNewPath = fNeighString + " " + fNewPath;
                            context.write(new Text(fNewPath), NullWritable.get());
                        }

                        for (Text lastNeigh : lastNeighs) {
                            String lNeighString = lastNeigh.toString();
                            String lNewPath = pathAsString;

                            if (!pathAsString.contains(lNeighString)) {
                                lNewPath = lNewPath + " " + lNeighString;
                                context.write(new Text(lNewPath), NullWritable.get());
                            }

                            if (!fNewPath.contains(lNeighString) && (pathAsString.compareTo(fNewPath) != 0)) {
                                String fullNewPath = fNewPath;
                                fullNewPath = fullNewPath + " " + lNeighString;
                                pathsList.get(i + 1).add(new Text(fullNewPath));
                                context.write(new Text(fullNewPath), NullWritable.get());
                            }

                        }
                    }

                }

            }

            // for (Text path : pathsList.get(0)) {
            // context.write(new Text(path), NullWritable.get());
            // }

            // for (Map.Entry<Text, List<Text>> entry : adjMap.entrySet()) {
            // Text node = entry.getKey();
            // List<Text> neighs = entry.getValue();

            // String valuesString = new String(node.toString());

            // for (Text value : neighs) {
            // valuesString += value.toString();
            // }

            // context.write(new Text(valuesString), NullWritable.get());
            // }

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Paths new");
        job.setJarByClass(Paths.class);
        job.setMapperClass(Paths.PathsMapper.class);
        job.setReducerClass(Paths.PathsReducer.class);
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

// $HADOOP_HOME/bin/hadoop jar ~/pa2/t2/exp/Paths.jar Paths ~/pa2/input
// ~/pa2/t2/exp/result-real