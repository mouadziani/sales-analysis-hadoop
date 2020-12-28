package com.isi.bigdata;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * The reduce class
 */
public class ReducerClass extends Reducer<Text, SalesReturnTypeWritable,Text,Text>
{
    public void reduce(Text key, Iterable<SalesReturnTypeWritable> values, Context context) throws IOException, InterruptedException
    {
        double offlineQuantity = 0;
        double onlineQuantity = 0;
        double totalAmount = 0;

        String field = context.getConfiguration().get("field");
        if(!field.equals("itemType"))
        {
            for(SalesReturnTypeWritable value: values){
                totalAmount += value.getTotal().get();
            }
            context.write(key, new Text(String.valueOf(totalAmount)));
        }
        else
        {
            for(SalesReturnTypeWritable value: values){
                totalAmount += value.getTotal().get();
                offlineQuantity += value.getOfflineQuantity().get();
                onlineQuantity += value.getOnlineQuantity().get();
            }

            String result = "\n Quantité en ligne : " + onlineQuantity + " ==== Quantité en hors ligne : " + offlineQuantity + " === Total de profit : " + totalAmount;
            context.write(key, new Text(result));
        }
        totalAmount = 0;
    }
}
