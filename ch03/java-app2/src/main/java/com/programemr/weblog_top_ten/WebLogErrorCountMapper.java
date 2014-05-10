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

public class WebLogErrorCountMapper extends MapReduceBase
implements Mapper<LongWritable, Text, Text, IntWritable> 
{

	// Value for summing in the reducer
	private final static IntWritable one = new IntWritable( 1 );
	
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
		Text httpRequest = new Text(matcher.group(5));
		
		// Filter any web requests that had a 300 HTTP return code or higher
		if ( httpCode >= 300 )
		{
			// Output the HTTP Error code and page requested and the number 1 as the value
			//  We will use the value in the reducer to sum the total occurrences
			//  of the same web request and error returned from the server.
			output.collect( new Text(httpRequest), one);
		}
	}
}