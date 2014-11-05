package HourMax;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class HourMaxReduce extends Reducer<Text,Text,Text,Text> {
	
	public void reduce(Text key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {

//declare variables
double hourMax = 0.00;
double hourTime = 0.00;
double totalValue = 0.00;
double totalNumber = 0.00;
double missing = 0.00;

ArrayList<String> list = new ArrayList <String>();

//since the values are in the Text format, they are sorted in lexicographical 
//generate hashmap using hour as key and o3 value as value
//since the hour is in the format of Integer, the hash map is automatically sorted in timely order
Map<Integer, String> valMap = new HashMap<Integer, String>();


for (Text val : values) {

	//String o3Value = null;
	String [] split = new String [2];
	split = val.toString().split(":");

	//when 0 is retrieved from hbase, it is treated as null, so convert it to 0
	if (split[0].equals("null") || split[0].equals(""))
		split[0] = "0";
	
	//generate a hash map  that contains all the values in one day in timely order
	valMap.put(Integer.parseInt(split[0]), split[1]);
}

//put the timely ordered value in a list
Iterator it = valMap.entrySet().iterator();
while (it.hasNext()) {
    Map.Entry hourAvalue = (Map.Entry)it.next();
    list.add((String)hourAvalue.getValue());
}

//iterate through the list and calculate hourly average
Iterator it2 = list.iterator();
while (it2.hasNext()) 
    {
	
	String o3Value = (String)it2.next();
  	
	if (Double.parseDouble(o3Value) > hourMax)
  	   hourMax = Double.parseDouble(o3Value);
    }

context.write(key, new Text(Double.toString(hourMax)));

}
}
