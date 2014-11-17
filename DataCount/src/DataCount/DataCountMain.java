package DataCount;

import java.util.ArrayList;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import DataCount.DataCountMain;
import DataCount.DataCountMap;
import DataCount.DataCountReduce;

public class DataCountMain {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		args = new GenericOptionsParser(conf, args).getRemainingArgs();
		conf.set("mapreduce.output.textoutputformat.separator", ",");
		
		//job configuration
		Job job = new Job(conf, "Summary of data in database");
			
		//set up job configuration
		job.setJarByClass(DataCountMain.class);

		job.setMapperClass(DataCountMap.class);
		job.setReducerClass(DataCountReduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);  


		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		FileInputFormat.addInputPath(job, new Path(args[0]));
		job.waitForCompletion(true);

	}

}
