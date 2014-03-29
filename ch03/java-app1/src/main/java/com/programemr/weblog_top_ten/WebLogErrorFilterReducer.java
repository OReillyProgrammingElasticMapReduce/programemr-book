package com.programemr.weblog_top_ten;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class WebLogErrorFilterReducer extends MapReduceBase
  implements Reducer<Text, IntWritable, Text, IntWritable> 
{
	public void reduce( Text key, Iterator<IntWritable> values,
			OutputCollector<Text, IntWritable> output,
			Reporter reporter) throws IOException
	{		
		// Iterates over all of the values and emits each key value pair
		while( values.hasNext() )
		{			
			output.collect( key, new IntWritable( values.next().get() ) );
		}
	}
}