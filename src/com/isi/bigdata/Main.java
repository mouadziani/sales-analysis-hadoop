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
        Configuration configuration = new Configuration();
        String field = args[2];
        configuration.set("field", field);
        System.out.println(args[2]);
        Job job = Job.getInstance(configuration, "Sales Analyser");
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);

        // Set the MapperClass output key/value config
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set the ReducerClass output key/value config
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
