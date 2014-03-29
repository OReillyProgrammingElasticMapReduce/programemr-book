package com.programemr.weblog_top_ten;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.programemr.weblog_top_ten.WebLogErrorFilterMapper;
import com.programemr.weblog_top_ten.WebLogErrorFilterReducer;

public class WebLogDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception 
	{		    
	    JobConf conf = new JobConf(getConf(), getClass());
	    conf.setJobName("Web Log Analyzer");

	    FileInputFormat.addInputPath(conf, new Path(args[0]));
	    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		    
	    conf.setOutputKeyClass(Text.class);
	    conf.setOutputValueClass(IntWritable.class);

	    conf.setMapperClass(WebLogErrorFilterMapper.class);
	    conf.setCombinerClass(WebLogErrorFilterReducer.class);
	    conf.setReducerClass(WebLogErrorFilterReducer.class);

	    JobClient.runJob(conf);
	    return 0;
	}
		  
	public static void main(String[] args) throws Exception {
	    int exitCode = ToolRunner.run(new WebLogDriver(), args);
	    System.exit(exitCode);
	}
	
}
