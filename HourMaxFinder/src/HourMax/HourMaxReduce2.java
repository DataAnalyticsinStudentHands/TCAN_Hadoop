package HourMax;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class HourMaxReduce2 extends Reducer<Text,Text,Text,Text> {
	
	public void reduce(Text key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {

//declare variables
double hourMax = 0.00;
int hourTime = -1;


for (Text val : values) {

	//String o3Value = null;
	String [] split = new String [2];
	split = val.toString().split(":");

	//when 0 is retrieved from hbase, it is treated as null, so convert it to 0
	if (split[0].equals("null") || split[0].equals(""))
		split[0] = "0";
	 	
	if (Double.parseDouble(split[1]) > hourMax) {
  	   hourMax = Double.parseDouble(split[1]);
  	   hourTime = Integer.parseInt(split[0]);
    }
} // end for iteration

	String output = Integer.toString(hourTime) + "," + Double.toString(hourMax);
	
	// Days which max value is <0 are not written to output
	if (hourTime > -1)
	context.write(key, new Text(output));

}
}

