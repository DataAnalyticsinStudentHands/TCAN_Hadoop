package DataCount;

import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class DataCountMap extends Mapper<LongWritable, Text, Text, Text> {
	
	public void map(LongWritable key, Text value, Context context)   throws IOException, InterruptedException {

		try {
			String line= value.toString();
			Scanner scan = new Scanner (line);
			scan.useDelimiter(",|" + System.getProperty("line.separator"));

			//retrieve data from file
			int i=0;
			String infos[] = new String[13];
			while ( i <13 && scan.hasNext())
			{
				infos[i]= scan.next();
				i++; 
			} 		

			//the 3rd argument set in main is used to identify the pollutant
			Configuration conf = context.getConfiguration();
			String param_arg = conf.get("param_id");

			//convert retrieved value to string
			String regionStr = infos[5];
			String siteStr = infos[8];
			String yearStr = infos[0];
			String monthStr = infos[1];
			String dayStr = infos[2];
			String hourStr = infos[3];
			String minStr = infos[4];
			String param_idStr = infos[6];
			String valueStr = infos[10];
			String flagStr = infos[11];
			String o3Value;

			//generate key for output
			String Key = yearStr + "," + param_idStr + "," + regionStr + "," + siteStr;

			context.write(new Text(Key), new Text("1"));			
		}
		catch (RuntimeException e){
			e.printStackTrace();
		}
	}
}
