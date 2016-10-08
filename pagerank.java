package org.apache.hadoop;
import java.io.IOException;


import java.util.StringTokenizer;
//import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class pagerank {
	public static class InitialMapper 
    extends Mapper<Object, Text, Text, Text>{
		//public int total_node = 0;//the total number of parentnodes
		//private int min = 0;//the number of nodes without outlinks
		//public int max=0;//the number of nodes with 5 outlinks
		
 public void map(Object key, Text value, Context context
         ) throws IOException, InterruptedException {
     
   String line = value.toString();
   String Line = line.trim();
   String[] test = Line.split(" ");
  //////////////////////////////////////////print out property
  // StringTokenizer str = new StringTokenizer(line);
  // int num = str.countTokens();
  // String leadnode = str.nextToken();
  // System.out.println(leadnode+" "+(num-1));
  // total_node += 1;
  //if ((num-1)==0){ min +=1;}
  //else {max +=1;}
   
  //double avg = max*5/total_node;
  // System.out.println(max+" "+ min + " "+total_node+" "+ avg);
  ///////////////////////////////////////////

   if (test.length > 1)// discard those without outlinks.
   {
   String[] parts = Line.split(" ",2);
   String keylink = parts[0]; //lead
   String outlinks = parts[1]; //outlink

   //n is the number of outlinks.
   StringTokenizer st = new StringTokenizer(outlinks);
   int n = st.countTokens();
   //System.out.println(keylink+" "+n);//test n
   
   double rank=0.85;//initial pr value
   String[] keypart = keylink.split(",");
   //keypart[0] is the lead link
   if (keypart.length > 1) {
	 rank = Double.parseDouble(keypart[1]);
		// if has rank, use it; no rank, use 0.85
	}
   for (String s: outlinks.split(" ")){
	   context.write(new Text(s), new Text(keypart[0] + ";" + rank +";"+ n ));
   }
   context.write(new Text(keypart[0]), new Text (outlinks));
  }
   else{}

  }
 }
	
public static class InitialReducer 
    extends Reducer<Text,Text,Text,Text> { 
  
public void reduce(Text key, Iterable<Text> values, 
                   Context context
                   ) throws IOException, InterruptedException {   
	  double rank=0.15;
	  double factor = 0.85;
		String[] str;
		Text outLinks = new Text();// outlinks
		
		for (Text t : values) {
			str = t.toString().split(";");
			if (str.length == 3) {
				rank += Double.parseDouble(str[1]) / Integer.parseInt(str[2])* factor;
			} else {
				// record outlinks
				outLinks.set(t.toString());
			}
		}
		context.write(new Text(key.toString() + "," + rank +" "+ outLinks ),new Text());
	}
}
  /////////////////////////////////////////////////////////////////////////
  public static class SequenceMapper 
  extends Mapper<Object, Text, DoubleWritable, Text>{
	  
	  private DoubleWritable num = new DoubleWritable();
	  
  public void map(Object key, Text value, Context context
               ) throws IOException, InterruptedException {
  //second map
	   String line = value.toString();
	   String Line = line.trim();

	   String[] parts = Line.split(" ");
	   String keypart = parts[0]; //lead
	  
	   String[] parent = keypart.split(",");
	   String parentnode = parent [0];  //parent node
	   String parentpr = parent [1];  //pr value, convert to key
	   
	   num.set(-Double.parseDouble(parentpr));
	   
	   //System.out.println(num+ " "+parentnode);
     context.write(num, new Text(parentnode));
	
   }
} 
  
  public static class SequenceReducer 
  extends Reducer<DoubleWritable,Text,Text,DoubleWritable> {
	  private Text result = new Text();
	  private DoubleWritable pr = new DoubleWritable ();
  public void reduce(DoubleWritable key, Iterable<Text> values, 
                  Context context
                  ) throws IOException, InterruptedException {    
  //second reduce
	//System.out.println("the following is reducer");
	String parentnode = "";
	 for (Text val : values) {
		 parentnode = parentnode+val.toString();
	}
	 result.set(parentnode);
	 pr.set(0-key.get());
	 //System.out.println(result+" "+pr);
	 context.write(result,pr);
    }
}
  
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: pagerank <in> <out>");
      System.exit(2);
    }
   
    Job job1 = new Job(conf, "part1");
    job1.setJarByClass(pagerank.class);
    job1.setMapperClass(InitialMapper.class);
    
    job1.setReducerClass(InitialReducer.class);
   
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);
    
    FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job1, new Path("txt1"));
    
    job1.waitForCompletion(true);
    
   int i=0;
   while (i<9)
    {
    	i++ ;
    	String In = Integer.toString(i);
    	String Out = Integer.toString(i+1);
      Job job2 = new Job(conf, "part2");
      job2.setJarByClass(pagerank.class);
      job2.setMapperClass(InitialMapper.class);
      
      job2.setReducerClass(InitialReducer.class);
     
      job2.setOutputKeyClass(Text.class);
      job2.setOutputValueClass(Text.class);
      
      FileInputFormat.addInputPath(job2, new Path("txt"+In));
      FileOutputFormat.setOutputPath(job2, new Path("txt"+Out));
     
      job2.waitForCompletion(true);
    }
 
   Job job3 = new Job(conf, "part3");
   job3.setJarByClass(pagerank.class);
   job3.setMapperClass(SequenceMapper.class);
   
   job3.setReducerClass(SequenceReducer.class);
  
   job3.setOutputKeyClass(DoubleWritable.class);
   job3.setOutputValueClass(Text.class);
   
   FileInputFormat.addInputPath(job3, new Path("txt10"));
   FileOutputFormat.setOutputPath(job3, new Path(otherArgs[1]));
   
   job3.waitForCompletion(true);

   System.exit(0);
  }
}
