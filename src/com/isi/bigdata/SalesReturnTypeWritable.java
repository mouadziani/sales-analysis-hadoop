package com.isi.bigdata;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SalesReturnTypeWritable implements Writable
{
    private DoubleWritable total;
    private IntWritable onlineQuantity;
    private IntWritable offlineQuantity;

    public SalesReturnTypeWritable() {
        onlineQuantity = new IntWritable();
        offlineQuantity = new IntWritable();
        total = new DoubleWritable();
    }

    public SalesReturnTypeWritable(DoubleWritable total, IntWritable onlineQuantity, IntWritable offlineQuantity) {
        this.total = total;
        this.onlineQuantity = onlineQuantity;
        this.offlineQuantity = offlineQuantity;
    }

    public DoubleWritable getTotal() {
        return total;
    }

    public void setTotal(DoubleWritable total) {
        this.total = total;
    }

    public IntWritable getOnlineQuantity() {
        return onlineQuantity;
    }

    public void setOnlineQuantity(IntWritable onlineQuantity) {
        this.onlineQuantity = onlineQuantity;
    }

    public IntWritable getOfflineQuantity() {
        return offlineQuantity;
    }

    public void setOfflineQuantity(IntWritable offlineQuantity) {
        this.offlineQuantity = offlineQuantity;
    }


    public void write(DataOutput dataOutput) throws IOException {
        onlineQuantity.write(dataOutput);
        offlineQuantity.write(dataOutput);
        total.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        onlineQuantity.readFields(dataInput);
        offlineQuantity.readFields(dataInput);
        total.readFields(dataInput);
    }
}
