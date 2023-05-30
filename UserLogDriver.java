package userlogs;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

public class UserLogDriver {
	public static void main(String args[]) {
		JobClient job = new JobClient();
		
		JobConf conf = new JobConf(UserLogDriver.class);
		
		conf.setJobName("UserLogJob");
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		conf.setMapperClass(UserMapper.class);
		conf.setReducerClass(UserReducer.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		job.setConf(conf);
		
		try {
			JobClient.runJob(conf);
			job.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
