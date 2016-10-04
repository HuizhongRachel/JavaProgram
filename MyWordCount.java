    package org.apache.hadoop.examples;
    import java.io.IOException;
    import java.util.StringTokenizer;
    import org.apache.hadoop.conf.Configuration;
    import org.apache.hadoop.fs.Path;
    import org.apache.hadoop.io.IntWritable;
    import org.apache.hadoop.io.Text;
    import org.apache.hadoop.mapreduce.Job;
    import org.apache.hadoop.mapreduce.Mapper;
    import org.apache.hadoop.mapreduce.Reducer;
    import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
    import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
    import org.apache.hadoop.util.GenericOptionsParser;
    public class MyWordCount {
      public static class TokenizerMapper 
           extends Mapper<Object, Text, Text, IntWritable>{
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
       
        public void map(Object key, Text value, Context context
                        ) throws IOException, InterruptedException {
          StringTokenizer chz1 = new StringTokenizer(value.toString());
          StringTokenizer chz2 = new StringTokenizer(value.toString());
          if (chz2.hasMoreTokens()){
        	  chz2.nextToken();
              while (chz2.hasMoreTokens()) {
            	  String word1 = chz1.nextToken();
            	  String word2 = chz2.nextToken();
            	  String word3 = word1 + " "+word2;
                  word.set(word3);
                  context.write(word, one);
          }
        }
      }
      }
      public static class IntSumReducer 
           extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, 
                           Context context
                           ) throws IOException, InterruptedException {
          int sum = 0;
          for (IntWritable val : values) {
            sum += val.get();
          }
          result.set(sum);
          context.write(key, result);
        }
      }
      public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        String[] otherArgs=new String[]{"input","output"};
        if (otherArgs.length != 2) {
          System.err.println("Usage: wordcount <in> <out>");
          System.exit(2);
        }
        Job job = new Job(conf, "word count");
        job.setJarByClass(MyWordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
      }
    }