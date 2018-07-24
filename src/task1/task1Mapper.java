package task1;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.*;

public class task1Mapper
  extends Mapper<LongWritable, Text, NullWritable, Text> {
  
  
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {   
	  
   // Here we are converting Text to String
	String content = value.toString();
	
	  String[] linesArray = content.split("  ");
	  
      for(String line : linesArray){
    	  
    	  //we are splitting line by pipe (|) 
          String[] word = line.split("\\|");

          //we are assigning company and product values from word
          Text company = new Text(word[0]);
          Text product = new Text(word[1]);
          
          // Remove lines which have company or product as "NA"
          if(!((company.equals(new Text("NA")))||(product.equals(new Text("NA")))))
          {
   
        	  // Here we are converting String to Test
        	  Text lineText = new Text(line);
        	  
        	  //we are writing only value as lineText and NullWritable.get() returns no key
		    context.write(NullWritable.get(),lineText); 
	  } 
	
	}
  }

}