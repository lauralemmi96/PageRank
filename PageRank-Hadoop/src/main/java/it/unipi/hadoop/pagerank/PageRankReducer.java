package it.unipi.hadoop.pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PageRankReducer extends Reducer<Text, ReducerReceivedData, Text, Text> {

    private final Text outValue = new Text();
    private int N;
    private float a;
    private float myPageRank = 0;
    String value = null;

    // getting the values from the hadoop configuration
    public void setup(Context context){
        Configuration conf = context.getConfiguration();
        N = Integer.parseInt(conf.get("N"));      // number of pages
        a = Float.parseFloat(conf.get("alpha"));  // alpha parameter to be used into the page rank conf


    }

    public void reduce(final Text key, final Iterable<ReducerReceivedData> values, final Context context)
            throws IOException, InterruptedException {

        float sum = 0;
        String myOutLinks = null;
        boolean outLink = false;

        //  value associated to the key could be a structure(only one) or an outlink(all the others)
        for (final ReducerReceivedData data : values) {
           if(!data.getType() && data.getData() != null){     // MY OUTLINKS(structure)
               myOutLinks = data.getData();
               outLink = true;
           }else{       //CONTRIBUTIONS
               sum += Float.parseFloat(data.getData());
           }

        }

        if(outLink) {
            //  evaluation of page rank
            myPageRank = a / N + (1 - a) * sum;

            //  we save the results to be used by the next iteration or by the sorting phase
            value = myPageRank + " " + myOutLinks;
            outValue.set(value);
            context.write(key, outValue);
        }



    }


}
