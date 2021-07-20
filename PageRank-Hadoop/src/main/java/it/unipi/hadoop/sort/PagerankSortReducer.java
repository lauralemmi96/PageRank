package it.unipi.hadoop.sort;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import java.io.IOException;

//  The values are already sorted by the modified split and shuffle. We have just to emit the values twisting them
public class PagerankSortReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {
    public void reduce(final DoubleWritable key, final Iterable<Text> values, final Context context)
            throws IOException, InterruptedException {

        //  We have the key/value reversed in order to perform an automatic sort. We have now to restore them
        for(Text title: values)
            context.write(title, key);

    }

}
