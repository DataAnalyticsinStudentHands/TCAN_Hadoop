package DataCount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;

public class DataCountReduce extends Reducer<Text,Text,Text,Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (Text val : values) {
			sum ++;
		}
		
		context.write(key, new Text(Integer.toString(sum)));
	}
}
