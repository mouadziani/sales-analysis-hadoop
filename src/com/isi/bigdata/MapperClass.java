package com.isi.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * The map class
 */
public class MapperClass extends Mapper<Text, Text, Text, SalesReturnTypeWritable>
{
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException
    {
        Configuration conf = context.getConfiguration();
        String field = conf.get("field");
        StringTokenizer stringTokenizer = new StringTokenizer(key.toString(), "\n");
        Text textLine = new Text();

        while (stringTokenizer.hasMoreTokens()) {
            textLine.set(stringTokenizer.nextToken());
            SalesReturnTypeWritable contextValue = getContextValueByField(textLine, field);
            Text contextKey = getContextKeyByField(textLine, field);
            context.write(contextKey, contextValue);
        }
    }

    private Text getContextKeyByField(Text textLine, String field)
    {
        if(field.equals("country")) {
            return new Text(textLine.toString().split(",")[1]);
        } else if(field.equals("region")) {
            return new Text(textLine.toString().split(",")[0]);
        } else {
            return new Text(textLine.toString().split(",")[2]);
        }
    }

    private SalesReturnTypeWritable getContextValueByField(Text textLine, String field)
    {
        String total;
        SalesReturnTypeWritable salesValueTypeWritable = new SalesReturnTypeWritable();

        if(field.equals("country")) {
            String country = textLine.toString().split(",")[1];
            if (!country.equals("Country")) {
                total = textLine.toString().split(",")[13];
                salesValueTypeWritable.setTotal(new DoubleWritable(Double.parseDouble(total)));
            }
        } else if (field.equals("region")) {
            String region = textLine.toString().split(",")[0];
            if (!region.equals("Region")) {
                total = textLine.toString().split(",")[13];
                salesValueTypeWritable.setTotal(new DoubleWritable(Double.parseDouble(total)));
            }
        } else if(field.equals("itemType")) {
            IntWritable onlineQuantity;
            IntWritable offlineQuantity;
            DoubleWritable itemTotal;
            String itemType = textLine.toString().split(",")[2];
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
                salesValueTypeWritable.setOnlineQuantity(onlineQuantity);
                salesValueTypeWritable.setOfflineQuantity(offlineQuantity);
                salesValueTypeWritable.setTotal(itemTotal);
            }
        }


        return salesValueTypeWritable;
    }
}
