package com.programemr;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class LogReducer extends MapReduceBase
  implements Reducer<Text, IntWritable, Text, IntWritable> 
{
    public void reduce( Text key, Iterator<IntWritable> values,
            OutputCollector<Text, IntWritable> output,
            Reporter reporter) throws IOException
    {
        // Output a log message
        System.err.println( "We are inside the reduce method" );

    	// Counts the occurrences of the date and time
    	int count = 0;
    	while( values.hasNext() )
    	{
            // Output a log message
            System.err.println( "Uh oh!  We are going to divide by zero!" );
    		
    		// Add the value to our count
    		count += ( values.next().get() / 0 );
    	}

    	// Output the date and time with its count
    	output.collect( key, new IntWritable( count ) );
    }
}
