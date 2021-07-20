package it.unipi.hadoop.graphCreation;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.*;
import java.io.IOException;

//  Mapper class to generate the graph of the pages. The mapper will emit for each class
//  the set of out going links
public class GraphCreationMapper extends Mapper<LongWritable, Text, Text, Text> {

    private final Text pagetitle = new Text();
    private final Text link = new Text();

    public void map(final LongWritable key, final Text value, final Context context)throws IOException, InterruptedException {

        String line = value.toString();
        int startIndex, endTitle, startText, endSquared, startSquared, text_index = 0;
        String initialBrackets = "[[";
        String finalBrackets = "]]";

        //  getting the title of the page
        if ((startIndex = line.indexOf("<title>")) >= 0){
            if((endTitle = line.indexOf("</title>")) > startIndex)
            {
                String title = line.substring(startIndex + 7, endTitle);
                pagetitle.set(title);

                //  the content of the page is inside a text tag
                if((startText = line.indexOf("<text")) >= endTitle){
                    text_index = startText;

                    //  getting the links
                    while (true){
                        //  each link is inside two double brackets
                        startSquared = line.indexOf(initialBrackets, text_index);
                        if (startSquared == -1) break;
                        endSquared = line.indexOf(finalBrackets, startSquared); // Starting from start
                        String find = line.substring(startSquared, endSquared+finalBrackets.length());
                        link.set(find);
                        //  for each outlink we emit a couple (title, outlink)
                        context.write(pagetitle, link);
                        text_index = startSquared + find.length();

                    }

                }

            }
        }
    }


}
