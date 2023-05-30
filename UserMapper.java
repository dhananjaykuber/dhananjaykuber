package userlogs;

import java.io.IOException;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;

public class UserMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>{
	private final static IntWritable one = new IntWritable(1);
	
	public void map(LongWritable key, Text values, OutputCollector<Text,IntWritable> output, Reporter reporter) throws IOException {
		String str_values = values.toString();
		String[] users = str_values.split("-");
		
		output.collect(new Text(users[0]), one);
	}
}
