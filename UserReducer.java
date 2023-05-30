package userlogs;

import java.util.*;
import java.io.IOException;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

public class UserReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>{
	public void reduce(Text t_key, Iterator<IntWritable> values, OutputCollector<Text,IntWritable> output, Reporter reporter) throws IOException {
		Text key = t_key;
		int freq = 0;
		
		while(values.hasNext()) {
			IntWritable value = (IntWritable) values.next();
			freq += value.get();
		}
		
		output.collect(key, new IntWritable(freq));
	}
}
