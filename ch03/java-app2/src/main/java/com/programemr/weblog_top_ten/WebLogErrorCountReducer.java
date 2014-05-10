package com.programemr.weblog_top_ten;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class WebLogErrorCountReducer extends MapReduceBase
  implements Reducer<Text, IntWritable, Text, IntWritable> 
{
	public void reduce( Text key, Iterator<IntWritable> values,
			OutputCollector<Text, IntWritable> output,
			Reporter reporter) throws IOException
	{		
		// Iterates over all of the values (counts of occurrences of the web requests)
		int count = 0;
		while( values.hasNext() )
		{			
			count += values.next().get();
		}

		// Output the web request with its count (wrapped in an IntWritable)
		output.collect( key, new IntWritable( count ) );
	}
}