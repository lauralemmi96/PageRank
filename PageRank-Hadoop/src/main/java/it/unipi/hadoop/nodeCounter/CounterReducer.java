package it.unipi.hadoop.nodeCounter;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;

//  Reducer to count the pages. It will obtain an array, we have just to count the number of elements
public class CounterReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private final IntWritable N = new IntWritable();

    public void reduce(final Text key, final Iterable<IntWritable> values, final Context context)
            throws IOException, InterruptedException {

        // counting all the pages
        int sum = 0;
        for (final IntWritable val : values) {
            sum += val.get();
        }
        N.set(sum);
        context.write(key, N);
    }
}


