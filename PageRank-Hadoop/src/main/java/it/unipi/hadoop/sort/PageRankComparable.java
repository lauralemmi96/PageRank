package it.unipi.hadoop.sort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.DoubleWritable;

public class PageRankComparable extends WritableComparator {
    protected PageRankComparable(){
        super(DoubleWritable.class,true);
    }

    //  we need to change the comparable class in order to sort the elements in ascending order
    @Override
    public int compare(WritableComparable w1, WritableComparable w2){
        DoubleWritable key1 = (DoubleWritable) w1;
        DoubleWritable key2 = (DoubleWritable) w2;
        return -1 * key1.compareTo(key2);
    }
}
