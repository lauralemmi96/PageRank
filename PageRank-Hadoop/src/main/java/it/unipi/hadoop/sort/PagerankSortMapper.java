package it.unipi.hadoop.sort;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;

//  Mapper for sorting the evaluated page rank. We have just to emits all the ranks and they will be sorted
//  in an automatic way by the split and shuffle of hadoop
public class PagerankSortMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {

    private static final DoubleWritable outputKey = new DoubleWritable();
    private static final Text text = new Text();

    public void map(final LongWritable key, final Text value, final Context context)throws IOException, InterruptedException {

        String title = value.toString().split("\\t")[0];
        String content = value.toString().split("\\t")[1];

        int firstSpace = content.indexOf(" ", 0);
        String myPageRank = content.substring(0, firstSpace);

        outputKey.set(Double.parseDouble(myPageRank));
        text.set(title);

        context.write(outputKey, text);

    }
}
