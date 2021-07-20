package it.unipi.hadoop.nodeCounter;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class CounterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private final Text page = new Text();

    public void map(final LongWritable key, final Text value, final Context context)
            throws IOException, InterruptedException {

        //  only one key emitted by all the mapper jobs
        page.set("page");

        String line = value.toString();
        int startIndex;
        if ((startIndex = line.indexOf("<title>")) >= 0){
            if (line.indexOf("</title>") >= startIndex) {
                //  if a title is present and well formatted we emits a one on the "page" key
                context.write(page, one);
            }
        }

    }
}