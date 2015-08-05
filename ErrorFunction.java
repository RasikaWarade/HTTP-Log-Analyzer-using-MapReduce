import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;






         
   public class ErrorFunction {
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
	 
         
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
    	private final static IntWritable one=new IntWritable(1);
    	private final static IntWritable zero=new IntWritable(0);
       private Text word = new Text();
          
       public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	   
    	   String line = value.toString();
    	   String serv;
    	   int i=line.indexOf("\"");
			 if(i!=-1){
				 int j=line.indexOf("\"");
				 if(j!=-1){
					 String delims="\"";
					 String[] tokens=line.split(delims);
					 if(tokens.length>=3)
					 {
					 String str = tokens[1];
					 serv=tokens[0];
					 delims=" ";
					 tokens=str.split(delims);
					 if(tokens.length==3){
					 str=tokens[1];
					
					 word.set(str.trim());	
					 delims="\" ";
					 tokens=line.split(delims);
					 str=tokens[1];
					 
					 delims=" ";
					 tokens=str.split(delims);
					 if(tokens.length==2){
						 str=tokens[0];
					 if(!str.equals("200")){
						 delims=" ";
						 tokens=serv.split(delims);
						 Text ret=new Text();
						 String d=tokens[0]+"";
						 ret.set(d);
						 
						 context.write(word,ret);
					 }
					 
					  
					 }
					 }
					 }
				 }
				 
			 
			 }
          
       }
    }
         
    public static class Reduce extends Reducer<Text, Text, Text, IntWritable> {
    	 List<Data> orders = new ArrayList<Data>();
    	 List<Object> uniqueList = new ArrayList<Object>(); 
        public void reduce(Text key, Iterable<Text> values, Context context)
         throws IOException, InterruptedException {
           int sum = 0;
           for(Text val:values)
           {
               String uri = val.toString();
               
               if(!uniqueList.contains(uri))
               	{
            	   uniqueList.add(uri);
                   sum=sum+1;
               	}
           }
           String key1=key+"--->";
           Data dat=new Data();
		   dat.key=key1;
		   dat.sum=sum;		     
		   orders.add(dat);
		   uniqueList.clear();
           
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
                       
    }
          
    public static void main(String[] args) throws Exception {
       Configuration conf = new Configuration();
          
       Job job = new Job(conf, "errorAnalysis");
      
       job.setOutputKeyClass(Text.class);
       job.setOutputValueClass(IntWritable.class);
          
       job.setMapperClass(Map.class);
       job.setReducerClass(Reduce.class);
       job.setMapOutputKeyClass(Text.class);
       job.setMapOutputValueClass(Text.class);
       job.setInputFormatClass(TextInputFormat.class);
       job.setOutputFormatClass(TextOutputFormat.class);
          
       FileInputFormat.addInputPath(job, new Path(args[0]));
       FileOutputFormat.setOutputPath(job, new Path(args[1]));
          
       job.waitForCompletion(true);
    }
   }
