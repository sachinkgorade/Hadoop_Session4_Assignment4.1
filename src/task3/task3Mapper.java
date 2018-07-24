package task3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.*;

public class task3Mapper
  extends Mapper<LongWritable, Text, Text, IntWritable> {
  
  private final static IntWritable one = new IntWritable(1);	 

  
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {   
	  
  
	String content = value.toString();
	
	  String[] linesArray = content.split("  ");
	  
      for(String line : linesArray){
    	  
    	     	  
          String[] word = line.split("\\|");
          /*for(String s: lineArray){
              System.out.println(s);} -- for debuggin purpose */
          Text company = new Text(word[0]);
          Text product = new Text(word[1]);
          Text state = new Text(word[3]);
          // Remove lines which have company or product as "NA"
          if(!((company.equals(new Text("NA")))||(product.equals(new Text("NA")))))
        	  if(company.equals(new Text("Onida")))
        	  {
               
        	 // String wordString = Arrays.toString(word);
        	 // Text lineText = new Text(wordString);
        	 // Text lineText = new Text(line);
		    context.write(state,one); 
	           }  
          	  
	}
  }

}