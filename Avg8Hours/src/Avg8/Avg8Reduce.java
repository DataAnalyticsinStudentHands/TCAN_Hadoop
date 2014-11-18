package Avg8;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Avg8Reduce extends Reducer<Text,Text,Text,Text> {
	public void reduce(Text key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {

	//declare variables
	double hourValue = 0.00;
	double totalValue = 0.00;
	double totalNumber = 0.00;
	double missing = 0.00;

	String[] valMap = new String[24];
	for (int i = 0; i<24; i++)
		valMap[i] = "999999999.99";

	for (Text val : values) {

		//String o3Value = null;
		String [] split = new String [2];
		split = val.toString().split(":");

		//when 0 is retrieved from hbase, it is treated as null, so convert it to 0
		if (split[0].equals("null") || split[0].equals(""))
			split[0] = "0";

		//generate an array  that contains all the values in one hour in timely order
		int index = Integer.parseInt(split[0]);

		valMap[index] = split[1];
	}
	
	//count missing values in array. If > 6 (75%) array is discharged
	for (int i = 0; i<24; i++)
		if (Double.parseDouble(valMap[i]) > 999999999)
			missing++;
	
	
	//put the timely ordered value in a list of 8 elements and compute avg
	ArrayList<String> list = new ArrayList <String>();
	int count = 0; // keeps track of number of elements in the list
	
	if (missing < 7) {
		for (String s : valMap) {
		
		hourValue = 0.00;
		totalValue = 0.00;
		totalNumber = 0.00;
		missing = 0.00;
		
	    list.add(s);
	    count++;

	 // if list reaches 8 elements, it is time to compute an average
	    if (count > 8) // remove oldest element of the list
	    	list.remove(0);
	    
	    if (count >= 8)
	    {
	    	Iterator<String> it2 = list.iterator();
	    	while (it2.hasNext()) 
	        {    	
	    		String o3Value = (String)it2.next();
	    		if (Double.parseDouble(o3Value) > 999999999) 
	    		{
	    			//count the missing value
	    			missing = missing + 1.00; 	  
	    			o3Value = "0.1"; // EPA minimum detectable value
	    		}
	    		else 
	    		{
	    			totalNumber = totalNumber + 1.00;    			
	    		}
	    		totalValue = totalValue + Double.parseDouble(o3Value);
	        }
	    	
	    	if (totalNumber > 5)
	    		hourValue = (totalValue - (missing * 0.1)) / totalNumber;
	    	else
	    		hourValue = totalValue / 8;
	    	
	    	String output = Integer.toString(count - 8) + "," + Double.toString(hourValue);
	    	
	    	context.write(key, new Text(output));
	    }
		}
	}
	}
}
