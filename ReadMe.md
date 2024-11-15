import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; import org.apache.hadoop.mapreduce.Job; 
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.mapreduce.Mapper; 
import org.apache.hadoop.mapreduce.Reducer; 
import java.io.IOException; 
public class WordCount_2 { 
public static class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> 
{ 
@Override 
public void map(LongWritable key,Text value,Context context) throws IOException, 
InterruptedException{ 
/*Listing karna hai ek integer no add karna hai 
* Convert the line text object to a string*/ 
String line=value.toString(); 
/*the line .split("\\W+") call users regular expressions */ 
for(String word:line.split("\\W+")){ 
if(word.length()>0){ 
context.write(new Text(word),new IntWritable(1)); 
} 
} 
}
} 
public static class SumReducer extends 
Reducer<Text,IntWritable,Text,IntWritable>{ 
@Override 
public void reduce(Text key,Iterable<IntWritable>values,Context context) throws IOException, 
InterruptedException{ 
int wordCount=0; 
for (IntWritable value:values){ 
wordCount +=value.get(); 
} 
context.write(key,new IntWritable(wordCount)); 
} 
} 
public static void main(String [] args)throws Exception{ 
if(args.length !=2){ 
System.out.printf( 
"Usage:WordCount<input.dir><output dir>\n"); 
System.exit(-1); 
} 
Job job = new Job(); 
job.setJarByClass(WordCount_2.class); 
job.setJobName("Word Count"); 
FileInputFormat.setInputPaths(job,new Path(args[0])); 
FileOutputFormat.setOutputPath(job,new Path(args[1])); 
job.setMapperClass(WordMapper.class); 
job.setReducerClass(SumReducer.class); 
job.setOutputKeyClass(Text.class); 
job.setOutputValueClass(IntWritable.class);
boolean success = job.waitForCompletion(true); 
System.exit(success ? 0:1); 
} 
}
