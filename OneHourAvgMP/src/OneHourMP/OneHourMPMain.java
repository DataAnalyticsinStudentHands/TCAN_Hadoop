package OneHourMP;

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

import OneHourMP.OneHourMPMain;
import OneHourMP.OneHourMPMap;
import OneHourMP.OneHourMPReduce;

public class OneHourMPMain {

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		args = new GenericOptionsParser(conf, args).getRemainingArgs();
		conf.set("mapreduce.output.textoutputformat.separator", ",");
		
		//The 3rd argument (parameter id) is set as property of the configuration so 
		//that it is possible to retrieve it in mapper
		conf.set("param_id", args[2]);
		
		//job configuration
		Job job = new Job(conf, "One Hour Avg Multi-Pollutant");
		Path toCache = new Path("/gtoti/sites/sites.txt"); 
		job.addCacheFile(toCache.toUri()); 
		job.createSymlink(); 
			
		//set up job configuration
		job.setJarByClass(OneHourMPMain.class);

		job.setMapperClass(OneHourMPMap.class);
		job.setReducerClass(OneHourMPReduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);  


		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		FileInputFormat.addInputPath(job, new Path(args[0]));
		job.waitForCompletion(true);

	}

}
