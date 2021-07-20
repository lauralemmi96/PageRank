package it.unipi.hadoop.pagerank;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;

// Mapper class to perform the page rank. It gets all the page information and compute
// basing on its current page rank the outLinks weight
public class PageRankMapper extends Mapper<LongWritable, Text, Text, ReducerReceivedData> {

    private final Text myKey = new Text();
    private Text otherKey = new Text();
    private final ReducerReceivedData myRrd = new ReducerReceivedData();
    public void map(final LongWritable key, final Text value, final Context context)throws IOException, InterruptedException {

        //  Data are stored in the form key\\tcontent

        String title = value.toString().split("\\t")[0];
        String content = value.toString().split("\\t")[1];

        int firstSpace = content.indexOf(" ", 0);
        String myPageRank = content.substring(0, firstSpace);

        myKey.set(title);

        String links = content.substring(firstSpace);

        //  we emit the structure associated with the page to have it in each phase
        myRrd.setData(links);
        myRrd.setType(false); //structure
        context.write(myKey, myRrd);

        //  extraction of outlinks from the saved format
        ArrayList<String> outLinks = new ArrayList<>();

        int openBrackets, closedBrackets, text_index = 0, numberLinks = 0;
        while (true){
            String initialBrackets = "[[";
            String finalBrackets = "]]";
            openBrackets = links.indexOf(initialBrackets, text_index);
            if (openBrackets == -1) break;
            closedBrackets = links.indexOf(finalBrackets, openBrackets); // Starting from start
            String find = links.substring(openBrackets+initialBrackets.length(), closedBrackets);
            outLinks.add(find);
            numberLinks++;
            text_index = openBrackets + find.length() + finalBrackets.length();

        }
        String weight =  String.valueOf(Float.parseFloat(myPageRank)/numberLinks);

        //  we emit the contribution to the outgoing links
        for(String pagelink: outLinks){
            ReducerReceivedData otherRrd = new ReducerReceivedData();
            otherKey.set(pagelink);
            otherRrd.setData(weight);
            otherRrd.setType(true); //weight
            context.write(otherKey, otherRrd);
        }



    }
}
