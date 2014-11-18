package Valid;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class ValidReduce extends Reducer<Text,Text,Text,Text> {
	public void reduce(Text key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {

	//declare variables
	double missing = 0.00;

	String[] valMap = new String[24];
	for (int i = 0; i<24; i++)
		valMap[i] = "999999999.99";

	String[] hourMap = new String[24];
	for (int i = 0; i<24; i++)
		hourMap[i] = "999999999.99";
	
	for (Text val : values) {

		//String o3Value = null;
		String [] split = new String [2];
		split = val.toString().split(":");

		//when 0 is retrieved from hbase, it is treated as null, so convert it to 0
		if (split[0].equals("null") || split[0].equals(""))
			split[0] = "0";

		//generate an array  that contains all the values in one hour in timely order
		int index = Integer.parseInt(split[0]);

		hourMap[index] = split[0];
		valMap[index] = split[1];
	}
	
	//count missing values in array. If > 6 (75%) array is discharged
	for (int i = 0; i<24; i++)
		if (Double.parseDouble(valMap[i]) > 999999999)
			missing++;
	
	if (missing < 7) {
		for (int i=0; i<24; i++) 
		{
			String output = hourMap[i] + "," + valMap[i];
			context.write(key,new Text(output));
		}
	    }
	}	
}

