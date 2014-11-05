package HourMax;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class HourMaxMap extends Mapper<LongWritable, Text, Text, Text> {


	public void map(LongWritable key, Text value, Context context)   throws IOException, InterruptedException {

		try {
			String line= value.toString();
			Scanner scan = new Scanner (line);
			scan.useDelimiter(",|" + System.getProperty("line.separator"));

			//retrieve data from file
			int i=0;
			String infos[] = new String[9];
			while ( i < 9 && scan.hasNext())
			{
				infos[i]= scan.next();
				i++; 
			} 		

			//convert retrieved value to string
			String regionStr = infos[1];
			String siteStr = infos[2];
			String yearStr = infos[3];
			String monthStr = infos[4];
			String dayStr = infos[5];
			String hourStr = infos[6];
			String param_idStr = infos[0];
			String valueStr = infos[7];
			String o3Value;

			//generate key for output
			String o3Key = param_idStr + "," + regionStr + "," + siteStr + "," + yearStr + "," + monthStr + "," + dayStr;

			//deal with the situation that some of the values have quote with them
			if (valueStr.contains("\""))
			{	
				String realValStr;
				int firstQuotev = valueStr.indexOf("\"");
				int lastQuotev = valueStr.lastIndexOf("\"");
				if (firstQuotev == lastQuotev)
					realValStr = valueStr;
				else
					realValStr = valueStr
							.substring(firstQuotev + 1, lastQuotev);
				o3Value = realValStr;
			}
			else 
				o3Value = valueStr;

			String hourAValue = hourStr + ":" + o3Value;

			// This if statement eliminates the entries that have been assigned
			// a pollutant level of 999999999.99 because of missing values
			if(Double.parseDouble(o3Value) < 999999999.99)//write output key, value
			context.write(new Text(o3Key), new Text(hourAValue));


		}
		catch (RuntimeException e){
			e.printStackTrace();
		}
	}
}
