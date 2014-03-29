package com.programemr.weblog_top_ten;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class WebLogErrorFilterMapper extends MapReduceBase
implements Mapper<LongWritable, Text, Text, IntWritable> 
{

	// The number of fields that must be found
	public static final int NUM_FIELDS = 7;
	
	public void map( LongWritable key, // Offset into the file
			Text value,
			OutputCollector<Text, IntWritable> output,
			Reporter reporter) throws IOException
	{
		// Regular expression to parse Apache Web Log
		String logEntryPattern = "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\]" +
				" \"(.+?)\" (\\d{3}) (\\S+)";
		
		// Get the Apache Web Log record as a String
		String logEntryLine = value.toString();
		
		// Compile the regular expression for parsing input
		Pattern p = Pattern.compile(logEntryPattern);
		Matcher matcher = p.matcher(logEntryLine);
		
		// Validate we have a valid log record
		if (!matcher.matches() || NUM_FIELDS != matcher.groupCount())
		{
			System.err.println("Bad log entry:");
			System.err.println(logEntryLine);
			return;
		}
		
		// Get the HTTP request information from the log entry
		Integer httpCode = Integer.parseInt(matcher.group(6));
		
		// Filter any web requests that had a 300 HTTP return code or higher
		if ( httpCode >= 300 )
		{
			// Output the log line as the key and HTTPstatus as the value
			output.collect( value, new IntWritable(httpCode));
		}
	}
}