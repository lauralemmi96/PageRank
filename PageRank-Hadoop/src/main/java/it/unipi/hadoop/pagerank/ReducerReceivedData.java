package it.unipi.hadoop.pagerank;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ReducerReceivedData implements Writable {

    boolean datatype; //0: structure, 1: weight
    String data;


    public ReducerReceivedData(){
        data = null;
        datatype = false;
    }

    public ReducerReceivedData(String data, boolean datatype){
        this.data = data;
        this.datatype = datatype;
    }

    public void setData(String data){
        this.data = data;
    }

    public void setType(boolean datatype){
        this.datatype = datatype;
    }

    public String getData(){
        return this.data;
    }

    public boolean getType(){
        return this.datatype;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeBoolean(this.datatype);
        dataOutput.writeUTF(this.data);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.datatype = dataInput.readBoolean();
        this.data = dataInput.readUTF();
    }

}
