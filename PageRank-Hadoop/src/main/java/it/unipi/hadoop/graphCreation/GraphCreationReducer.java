package it.unipi.hadoop.graphCreation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import java.io.IOException;

//  Reducer class to generate the graph of the pages. It will aggregate the results
//  generating a class containing the page title, its rank and outgoing links

public class GraphCreationReducer extends Reducer<Text, Text, Text, Text> {

    private final Text outValue = new Text();
    private float pagerank;

    //  gets from the configuration the number of pages and realize the base page rank(1/N)
    public void setup(Context context){
        Configuration conf = context.getConfiguration();
        String N = conf.get("N");
        pagerank = 1/Float.parseFloat(N);

    }

    public void reduce(final Text key, final Iterable<Text> values, final Context context)
            throws IOException, InterruptedException {

        String links = null;
        int i = 0;
        //  we generate a string containing all the links
        for (final Text inLinks : values) {
            if(i == 0)
                links = inLinks.toString();
            else
                links = links + inLinks.toString();
            i++;

        }
        String rankList = pagerank + " " + links;
        outValue.set(rankList);
        context.write(key, outValue);
    }
}
