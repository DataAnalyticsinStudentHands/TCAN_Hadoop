package OneHour;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OneHourMap extends Mapper<LongWritable, Text, Text, Text> {

	 public final static String sitesfile="sites.txt"; 
	 public ArrayList<String> HoustonSitesList = new ArrayList<String>();

	 public void setup(Context context) throws IOException { 
	 Scanner reader = new Scanner(new FileReader(sitesfile)); 

	 while (reader.hasNext())
	 {
		HoustonSitesList.add(reader.next());
	 }
		
	 reader.close(); 
	 } 

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
			String o3Key = param_idStr + "," + regionStr + "," + siteStr + "," + yearStr + "," + monthStr + "," + dayStr + "," + hourStr;

			//get the real flag inside the quote
			if (flagStr.contains("\""))
			{	int firstQuote = flagStr.indexOf("\"");
				int lastQuote = flagStr.lastIndexOf("\"");
				String realFlag = flagStr.substring(firstQuote+1, lastQuote);
				flagStr = realFlag;
			}
			



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
				valueStr = realValStr;
			}

			//let o3 value = "NA" if there is a flag information
			if (flagStr.equals(""))
				if (valueStr.equals("NULL"))
					o3Value = "NA";			
				else o3Value = valueStr;
			else
				o3Value = "NA";

			String minAValue = minStr + ":" + o3Value;
			
			// filtering only user defined sites 
			if(HoustonSitesList.contains(siteStr))
			{
				
				if(param_idStr.equals(param_arg))//write output key, value
				context.write(new Text(o3Key), new Text(minAValue));
			}

		}
		catch (RuntimeException e){
			e.printStackTrace();
		}
	}
}
