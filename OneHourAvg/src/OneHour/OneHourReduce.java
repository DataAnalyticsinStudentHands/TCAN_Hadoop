package OneHour;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class OneHourReduce extends Reducer<Text,Text,Text,Text> {
	public void reduce(Text key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {

//declare variables
double hourValue = 0.00;
double totalValue = 0.00;
double totalNumber = 0.00;
double missing = 0.00;

ArrayList<String> list = new ArrayList <String>();

//since the values are in the Text format, so they are sorted in lexicographical 
//generate hashmap using minute as key and o3 value as value
//since the minute is in the format of Integer, so the hash map is automatically sorted in timely order
Map<Integer, String> valMap = new HashMap<Integer, String>();

for (Text val : values) {

	//String o3Value = null;
	String [] split = new String [2];
	split = val.toString().split(":");

	//when 0 is retrieved from hbase, it is treated as null, so convert it to 0
	if (split[0].equals("null") || split[0].equals(""))
		split[0] = "0";
	
	//generate a hash map  that contains all the values in one hour in timely order
	valMap.put(Integer.parseInt(split[0]), split[1]);
}

//put the timely ordered value in a list
Iterator it = valMap.entrySet().iterator();
while (it.hasNext()) {
    Map.Entry minAvalue = (Map.Entry)it.next();
    list.add((String)minAvalue.getValue());
}

//iterate through the list and calculate hourly average
Iterator it2 = list.iterator();
while (it2.hasNext()) 
    {
	
	String o3Value = (String)it2.next();
	 if (o3Value.equals("NA")) {
  	  //count the missing value
  	  missing = missing + 1.00;
  	  int ind = list.indexOf(o3Value);
  	  
  	  //get previous value if the missing value is not at the beginning of the hour
  	  if (ind > 0)
  	  {		  
  		  o3Value = list.get(ind-1);
  		  totalNumber = totalNumber + 1.00;
  		  totalValue = totalValue + Double.parseDouble(o3Value);
  	  }

    }
    else 
    {
  	  totalNumber = totalNumber + 1.00;
  	  totalValue = totalValue + Double.parseDouble(o3Value);
    }
}

//the hourly value is calculated if the number of missing values is less than 5
//the hourly value is marked as 999999999.99 if there are 5 or more than 5 values are missing
if (missing < 5.00 && totalNumber>0) {
	hourValue = totalValue/totalNumber;

}
else 
	hourValue= 999999999.99;


context.write(key, new Text(Double.toString(hourValue)));

}
}

