package Test;

import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Main
{
	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		args = new GenericOptionsParser(conf, args).getRemainingArgs();
		conf.set("mapreduce.output.textoutputformat.separator", ",");
		
		//The 3rd argument (parameter id) is set as property of the configuration so 
		//that it is possible to retrieve it in mapper
		//4th argument is sites list
		conf.set("param_id", args[2]);
		conf.set("sites",args[3]);

		//job configuration
		Job job = new Job(conf, "BD HW2 part2 step1 YZ");

		

		//if there is a timeout for the scanner, the timeout is extended extra 3 minutes
		//int scannerTimeout = (int) conf.getLong(
				//HConstants.HBASE_REGIONSERVER_LEASE_PERIOD_KEY, -1);

			
		//set up job configuration
		job.setJarByClass(Main.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);  


		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		FileInputFormat.addInputPath(job, new Path(args[0]));
		job.waitForCompletion(true);
	}
}