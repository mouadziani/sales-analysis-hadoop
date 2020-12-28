package com.isi.bigdata;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class SalesAnalyser {

    /**
     * The map class
     */
    public static class MapClass extends Mapper<Text, Text, Text, SalesReturnTypeWritable>
    {
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String region;
            String country;
            String itemType;
            String total;
            IntWritable onlineQuantity = new IntWritable();
            IntWritable offlineQuantity = new IntWritable();

            Configuration conf = context.getConfiguration();
            String field = conf.get("field");
            StringTokenizer stringTokenizer = new StringTokenizer(key.toString(), "\n");
            Text textLine = new Text();

            switch (field) {
                case "country":
                    while (stringTokenizer.hasMoreTokens()) {
                        textLine.set(stringTokenizer.nextToken());
                        country = textLine.toString().split(",")[1];
                        if (!country.equals("Country")) {
                            total = textLine.toString().split(",")[13];
                            SalesReturnTypeWritable salesValueTypeWritable = new SalesReturnTypeWritable();
                            salesValueTypeWritable.setTotal(new DoubleWritable(Double.parseDouble(total)));
                            context.write(new Text(country), salesValueTypeWritable);
                        }
                    }
                    break;
                case "region":
                    while (stringTokenizer.hasMoreTokens()) {
                        textLine.set(stringTokenizer.nextToken());
                        region = textLine.toString().split(",")[0];
                        if (!region.equals("Region")) {
                            total = textLine.toString().split(",")[13];
                            SalesReturnTypeWritable salesValueTypeWritable = new SalesReturnTypeWritable();
                            salesValueTypeWritable.setTotal(new DoubleWritable(Double.parseDouble(total)));
                            context.write(new Text(region), salesValueTypeWritable);
                        }
                    }
                    break;
                case "item-type":
                    while (stringTokenizer.hasMoreTokens()) {
                        SalesReturnTypeWritable salesReturnTypeWritable = new SalesReturnTypeWritable();
                        DoubleWritable itemTotal;
                        textLine.set(stringTokenizer.nextToken());
                        itemType = textLine.toString().split(",")[2];
                        if (!itemType.equals("Item Type")) {
                            total = textLine.toString().split(",")[13];
                            if (textLine.toString().split(",")[3].equals("Online")) {
                                onlineQuantity = new IntWritable(Integer.parseInt(textLine.toString().split(",")[8]));
                                offlineQuantity = new IntWritable(0);
                            } else {
                                onlineQuantity = new IntWritable(0);
                                offlineQuantity = new IntWritable(Integer.parseInt(textLine.toString().split(",")[8]));
                            }
                            itemTotal = new DoubleWritable(Double.parseDouble(total));
                            salesReturnTypeWritable.setOnlineQuantity(onlineQuantity);
                            salesReturnTypeWritable.setOfflineQuantity(offlineQuantity);
                            salesReturnTypeWritable.setTotal(itemTotal);
                            context.write(new Text(itemType), salesReturnTypeWritable);
                        }
                    }
                    break;
            }
        }
    }

    /**
     * The reduce class
     */
    public static class ReduceClass extends Reducer<Text, SalesReturnTypeWritable,Text,Text>
    {
        public void reduce(Text key, Iterable<SalesReturnTypeWritable> values, Context context) throws IOException, InterruptedException
        {
            double offlineQuantity = 0;
            double onlineQuantity = 0;
            double totalAmount = 0;

            String field = context.getConfiguration().get("field");
            if(!field.equals("item-type")) {
                for(SalesReturnTypeWritable value: values){
                    totalAmount += value.getTotal().get();
                }
                context.write(key, new Text(String.valueOf(totalAmount)));
            } else {
                for(SalesReturnTypeWritable value: values){
                    totalAmount += value.getTotal().get();
                    offlineQuantity += value.getOfflineQuantity().get();
                    onlineQuantity += value.getOnlineQuantity().get();
                }

                String result = "\n la quantité en ligne : " + onlineQuantity + " ==== La quantité en hors ligne : " + offlineQuantity + " === Total de profit : " + totalAmount;
                context.write(key, new Text(result));
            }
            totalAmount = 0;
        }
    }

}
