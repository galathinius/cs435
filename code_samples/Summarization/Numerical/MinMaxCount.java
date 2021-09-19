public class MinMaxCount {
    // calculating the Min, Max and Count of comments per user
    public static class MinMaxCountMapper extends Mapper<Object, Text, Text, MinMaxCountTuple> {
        // Our output key and value Writables
        private Text outUserId = new Text();

        private MinMaxCountTuple outTuple = new MinMaxCountTuple();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> parsed = transformXmlToMap(value.toString());
            // Assume we have developed a class, parse.
            // Grab the ”comment" field since it is what we are finding
            // the min and max value of
            String comment = parsed.get("comment");
            // Grab the “UserID” since it is what we are grouping by
            String userId = parsed.get("UserId");
            // Set the minimum and maximum date values to the comment
            outTuple.setValue(comment);
            // Set the comment count to 1 outTuple.setCount(1);
            // Set our user ID as the output key
            outUserId.set(userId);
            // Write out the hour and the average comment length
            context.write(outUserId, outTuple);
        }
    }

    public static class MinMaxCountReducer extends Reducer<Text, MinMaxCountTuple, Text, MinMaxCountTuple> {
        // Our output value Writable
        private MinMaxCountTuple result = new MinMaxCountTuple();

        public void reduce(Text key, Iterable<MinMaxCountTuple> values, Context context)
                throws IOException, InterruptedException {
            // Initialize our result
            result.setMin(null);
            result.setMax(null);
            result.setCount(0);
            int sum = 0;
            // Iterate through all input values for this key
            for (MinMaxCountTuple val : values) {
                // If the value is less than the result's min
                // Set the result's min to value
                if (result.getMin() == null || val.getValue().compareTo(result.getMin()) < 0) {
                    result.setMin(val.getValue());
                }
                // If the value is more than the result's max
                // Set the result's max to value
                if (result.getMax() == null || val.getValue().compareTo(result.getMax()) > 0) {
                    result.setMax(val.getValue());
                }
                // Add to our sum the count for value
                sum += val.getCount();
            }
            // Set our count to the number of input values
            result.setCount(sum);
            context.write(key, result);
        }
    }
}
