
import java.io.IOException;
import java.util.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PeakAnalysisReply{
	
	public static class Data{
        String key;
        int sum;
        public static class OrderByReply implements Comparator<Data> {

            @Override
            public int compare(Data o1, Data o2) {
                return o1.sum < o2.sum ? 1 : (o1.sum > o2.sum ? -1 : 0);
            }
        }

        public String getKey() {return key; }
        public void setKey(String k) {this.key = k;}

        public int getValue() {return sum;}
        public void setValue(int val) {this.sum = val;}
   
        /* implementing toString method to print key of Data
         */
        @Override
        public String toString(){
            return String.valueOf(key);
        }
    }//Data Represented as key value pair
	
 	public static class Map extends   Mapper<LongWritable,Text,Text,IntWritable>
	{
		 private final static IntWritable one=new IntWritable(1);
		 private final static IntWritable zero=new IntWritable(0);
		 
		 private Text word=new Text();
		//map function
		 public void map(LongWritable key,Text value,Context context) throws      IOException, InterruptedException{
	
			 String line=value.toString();
			 
			 
			// context.write(word, one);
			 int i=line.indexOf("[");
			 if(i!=-1){
				 int j=line.indexOf("]");
				 if(j!=-1){
					 String delims=" ";
					 String[] tokens=line.split(delims);
					 if(tokens.length>=9)
					 {
					 String str = tokens[3];
					 str=str.substring(1, str.length()-6);
					 word.set(str);	
					 delims="\" ";
					 tokens=line.split(delims);
					 str=tokens[1];
					 
					 delims=" ";
					 tokens=str.split(delims);
					 if(tokens.length==2){
						 str=tokens[1];
					 if(str.equals("-")){
						 context.write(word, zero);
					 }
					 else{
						 
						 int p=Integer.parseInt(tokens[1]);
						 IntWritable size=new IntWritable(p);
					 context.write(word, size);
					 }
					  
					 }
					 }
					 
				 }
				 
			 
			 }
		
			  
 		 }
		
	}//end of class Map	
	
 	
	public static class Reduce extends  Reducer<Text,IntWritable,Text,IntWritable>
	{	
		 List<Data> orders = new ArrayList<Data>();
	     
		public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException
		{
	
			int sum=0;
			for(IntWritable val : values)
			{
				sum+=val.get();
			}
			Data to=new Data();
			String key1=key+"";
			Data dat=new Data();
		     dat.key=key1;
		     dat.sum=sum;		     
		     orders.add(dat);
		}
		
		public  void cleanup(Context context) throws IOException,InterruptedException {
			Collections.sort(orders, new Data.OrderByReply());
            
            for(int i=0;i<orders.size();i++)
            {
                String key1=orders.get(i).getKey();
                Text key=new Text();
                key.set(key1);
                context.write(key, new IntWritable(orders.get(i).getValue()));
            }
      }
		
	}//end of class Reduce	
	
	
	public static void main(String[] args) throws Exception
	{	
			Configuration conf=new Configuration();
			Job job = new Job(conf, "PeakAnalysisReply");
	       
	       job.setOutputKeyClass(Text.class);
	       job.setOutputValueClass(IntWritable.class);
	           
	       job.setMapperClass(Map.class);
	       job.setReducerClass(Reduce.class);
	           
	       job.setInputFormatClass(TextInputFormat.class);
	       job.setOutputFormatClass(TextOutputFormat.class);
	           
	       FileInputFormat.addInputPath(job, new Path(args[0]));
	       FileOutputFormat.setOutputPath(job, new Path(args[1]));
	           
	       job.waitForCompletion(true);
	}//end of main	

}//end of PeakAnalysisReply class

