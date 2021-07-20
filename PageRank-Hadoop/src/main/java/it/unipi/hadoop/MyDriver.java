package it.unipi.hadoop;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import it.unipi.hadoop.pagerank.ReducerReceivedData;
import it.unipi.hadoop.sort.PageRankComparable;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;


//   Main class, realizes the page rank of a set of web pages. Pages must be formatted as follows:
//      - one page per line
//      - each page to be classified must have a title field and a text field to maintain its content
//      - each page must be unique
//      - links to other pages must be formatted in square brackets containing the title of the page referred to

public class MyDriver {

    ////  hadoop environment parameters
    private static final String OUTPUT = "output";    //  general output folder name
    private static final String OUTPUT1 = OUTPUT+"1"; //  output for page construction
    private static final String OUTPUT2 = OUTPUT+"2"; //  output for page ranking
    private static final int NUMBER_OF_REDUCERS = 3;  //  number of reducers used by the hadoop environment
    private static final Configuration conf = new Configuration();
    private static final Logger logger = LoggerFactory.getLogger(MyDriver.class);

    //   To perform the page ranking our application goes through four phases:
    //      - a first step in which we count the number of pages in order to identify the base rank of each page
    //      - a second step in which we create a graph describing all the pages and their relations
    //      - a third step in which iteratively we evaluate the page rank and we update the global node graph
    //      - a forth step in which we sort all the page rank results in ascending order

    public static void main(final String[] args) throws Exception {

        //  the application requires three parameters:
        //      - a path for the data
        //      - an alpha to be applied into the page rank formula
        //      - the number of iterations to be used for the page rank

        if(args.length != 3){
            logger.error("3 arguments expected <input_path><alpha><number_iterations>");
            return;
        }

        conf.set("alpha", args[1]);  //  we set the alpha parameter to be used by the hadoop jobs

        final int num_iterations = Integer.parseInt(args[2]);
        if( num_iterations < 1 ){
            logger.error("Error, iteration must be greater than 0");
            return;
        }

        ////// STEP 1: NODES COUNTING
        /*
            we count all the pages in order to identify the base rank of every page

            WRITE output1
            READ  input file
         */

        //  we measure the time for perform the page rank in order to test the application
        long startExecutionTime = System.currentTimeMillis();

        final Job nodeCounterJob = new Job(conf, "NodeCounter");
        nodeCounterJob.setJarByClass(MyDriver.class);

        nodeCounterJob.setOutputKeyClass(Text.class);
        nodeCounterJob.setOutputValueClass(IntWritable.class);

        nodeCounterJob.setMapperClass(it.unipi.hadoop.nodeCounter.CounterMapper.class);
        nodeCounterJob.setReducerClass(it.unipi.hadoop.nodeCounter.CounterReducer.class);

        //  using more reducers is useless because the phase produces only one key
        nodeCounterJob.setNumReduceTasks(1);

        //  we remove old outputs if exists
        dropOutput(1);
        FileInputFormat.addInputPath(nodeCounterJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(nodeCounterJob, new Path(OUTPUT1));

        nodeCounterJob.waitForCompletion(true);

        logger.info("Node count completed");

        //STEP 2: GRAPH CONSTRUCTION
        /*
            we generate a graph which represents the pages as a set of nodes characterized by
            a rank and a list of outLinks

            WRITE output2
            READ output1/part-r-00000

        */

        Path graphOrderFile =new Path(OUTPUT1+"/part-r-00000");
        FileSystem fileSystem = FileSystem.get(conf);

        if (!fileSystem.exists(graphOrderFile))
            logger.error("Graph Order File Not Found");

        // The file will contain just one line containing a key and the number of pages counted at the previous step
        FSDataInputStream in = fileSystem.open(graphOrderFile);
        String line = in.readLine();
        //  file format: key\\tvalue
        String N = line.split("\\t")[1].trim();

        //  we set the number of pages into the hadoop environment
        conf.set("N", N);

        final Job graphConstructorJob = new Job(conf, "Graph Constructor");
        graphConstructorJob.setJarByClass(MyDriver.class);

        graphConstructorJob.setOutputKeyClass(Text.class);
        graphConstructorJob.setOutputValueClass(Text.class);

        graphConstructorJob.setMapperClass(it.unipi.hadoop.graphCreation.GraphCreationMapper.class);
        graphConstructorJob.setReducerClass(it.unipi.hadoop.graphCreation.GraphCreationReducer.class);

        graphConstructorJob.setNumReduceTasks(NUMBER_OF_REDUCERS);

        FileInputFormat.addInputPath(graphConstructorJob, new Path(args[0]));

        //  we remove old outputs if exists
        dropOutput(2);
        FileOutputFormat.setOutputPath(graphConstructorJob, new Path(OUTPUT2));
        graphConstructorJob.waitForCompletion(true);
        logger.info("Graph construction completed");

        //STEP 3: PAGERANK
        /*
            now we have a graph representing all the pages, from the graph information we evaluate
            each page rank and then we update the graph information. This phase can be executed more times
            until the page rank became stable

            WRITE outputN+1
            READ outputN/part-r-0000REDUCER

            We have decided to not overwrite the last information generating each time a new folder  in order
            to see the page rank updates

        */

        int lastIndex = 2;
        for(int i = 0; i < num_iterations; i++) {

            final Job pageRankJob = new Job(conf, "PageRank");
            pageRankJob.setJarByClass(MyDriver.class);

            pageRankJob.setMapOutputKeyClass(Text.class);
            pageRankJob.setMapOutputValueClass(ReducerReceivedData.class);

            pageRankJob.setOutputKeyClass(Text.class);
            pageRankJob.setOutputValueClass(Text.class);

            pageRankJob.setMapperClass(it.unipi.hadoop.pagerank.PageRankMapper.class);
            pageRankJob.setReducerClass(it.unipi.hadoop.pagerank.PageRankReducer.class);

            pageRankJob.setNumReduceTasks(NUMBER_OF_REDUCERS);

            //  we need to get all the output files produced by the reducers
            FileInputFormat.setInputPaths(pageRankJob, generatePaths(lastIndex));

            //  we remove the next old output if exists
            dropOutput(lastIndex+1);
            FileOutputFormat.setOutputPath(pageRankJob, new Path(OUTPUT+ (lastIndex+1)));
            pageRankJob.waitForCompletion(true);
            logger.info("Iteration number " + (i + 1)  + " completed");
            lastIndex++;

        }
        logger.info("Page rank completed");

        //STEP 4: PAGE SORTING
        /*
            We sort the page ranking in ascending order

            WRITE outputN+1
            READ outputN/part-r-0000REDUCER
        */

        //  we have to remove the output files if exists
        dropOutput(lastIndex+1);

        final Job pageRankSortingJob = new Job(conf, "PageRank Sorting");
        pageRankSortingJob.setJarByClass(MyDriver.class);

        pageRankSortingJob.setMapOutputKeyClass(DoubleWritable.class);
        pageRankSortingJob.setMapOutputValueClass(Text.class);

        pageRankSortingJob.setOutputKeyClass(Text.class);
        pageRankSortingJob.setOutputValueClass(DoubleWritable.class);

        pageRankSortingJob.setMapperClass(it.unipi.hadoop.sort.PagerankSortMapper.class);
        pageRankSortingJob.setReducerClass(it.unipi.hadoop.sort.PagerankSortReducer.class);
        pageRankSortingJob.setSortComparatorClass(PageRankComparable.class);

        //  we want to generate only one output file containing the sorted page ranks
        pageRankSortingJob.setNumReduceTasks(1);

        //  we need to get all the output files produced by the reducers
        FileInputFormat.setInputPaths(pageRankSortingJob, generatePaths(lastIndex));

        FileOutputFormat.setOutputPath(pageRankSortingJob, new Path(OUTPUT+ (lastIndex+1)));
        pageRankSortingJob.waitForCompletion(true);
        logger.info("Sorting completed");

        long endExecutionTime = System.currentTimeMillis();
        float execTimeSec = ((float)endExecutionTime - startExecutionTime)/1000L;
        logger.info("Total Execution Time: " + execTimeSec);
    }

    // generate all the paths for the inputs of an iteration of page rank
    static Path[] generatePaths(int index){
        Path[] paths = new Path[NUMBER_OF_REDUCERS];
        for( int a = 0; a<NUMBER_OF_REDUCERS; a++)
            paths[a] = new Path(OUTPUT + index+"/part-r-0000"+a);
        return paths;
    }

    //  removes old outputs that has to be overwritten by hadoop jobs
    static void dropOutput(int index){
        Path out_last_path = new Path(OUTPUT+ index);
        try {
            FileSystem fs_last = out_last_path.getFileSystem(conf);
            if (fs_last.exists(out_last_path)) {
                fs_last.delete(out_last_path, true);
            }
        }catch(Exception e){
            logger.info("Folder not present");
        }

    }
}
