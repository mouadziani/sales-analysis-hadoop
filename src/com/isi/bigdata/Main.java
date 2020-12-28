package com.isi.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Main
{
    public static void main(String[] args) throws Exception
    {
//        Configuration configuration = new Configuration();
//        String field = args[2];
//        configuration.set("field", field);
//        System.out.println(args[2]);
//        Job job = Job.getInstance(configuration, "SalesAnalyser");
//        job.setJarByClass(SalesAnalyser.class);
//        job.setMapperClass(SalesAnalyser.MapClass.class);
//        job.setReducerClass(SalesAnalyser.ReduceClass.class);
//
//        // Set the MapClass output key/value config
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);
//
//        // Set the ReduceClass output key/value config
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(SalesReturnTypeWritable.class);
//
//        job.setInputFormatClass(KeyValueTextInputFormat.class);
//        job.setOutputFormatClass(TextOutputFormat.class);
//
//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
//        boolean result = job.waitForCompletion(true);
//        System.exit(result ? 0 : 1);

        Configuration conf = new Configuration();
        conf.set("field", args[2]);
        Job job = Job.getInstance(conf, "Sales");
        job.setJarByClass(SalesAnalyser.class);
        job.setMapperClass(SalesAnalyser.MapClass.class);
        job.setReducerClass(SalesAnalyser.ReduceClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SalesReturnTypeWritable.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
