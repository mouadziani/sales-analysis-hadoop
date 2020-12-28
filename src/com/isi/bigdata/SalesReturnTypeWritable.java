package com.isi.bigdata;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;

public class SalesReturnTypeWritable
{
    private DoubleWritable total;
    private IntWritable onlineQuantity;
    private IntWritable offlineQuantity;

    public SalesReturnTypeWritable() {}

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
}
