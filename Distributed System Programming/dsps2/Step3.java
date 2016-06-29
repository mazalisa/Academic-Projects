package dsps2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// Step 3 - unites the values extracted from step 2 to a pair data.

public class Step3 {

	public static class Mapper3 extends Mapper<LongWritable, Text, ExtractedPair, ExtractedPairData>{
		// write <pair,data> to context with no changes
		
		@Override
	    public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
			String[] value_after_parsing = value.toString().split("\t");
			String[] pairTmp = value_after_parsing[0].split(",");
			Text word1 = new Text(pairTmp[0]);
			Text word2 = new Text(pairTmp[1]);
			ExtractedPair finalPair = new ExtractedPair(word1,word2);
			String[] pairData = value_after_parsing[1].split(",");

			// ExtractedPairData (finalPair, decade, cw_1w_2, cw_1, cw_2, N)
			ExtractedPairData data = new ExtractedPairData(finalPair, Integer.parseInt(pairData[2]), Long.parseLong(pairData[3]), Long.parseLong(pairData[4]), Long.parseLong(pairData[5]), Long.parseLong(pairData[6]));
			context.write(finalPair, data);
		}
	}
	
	public static class Reducer3 extends Reducer<ExtractedPair,ExtractedPairData,ExtractedPair,ExtractedPairData> {
		
		// we have two data values for every pair: data-1 that has c(word1) and data-2 that has c(word2). we want to merge c(word1) and c(word2) values. now they have an updated data. Now we need to write the pair to the context with the new data
		
		@Override
	    public void reduce(ExtractedPair key, Iterable<ExtractedPairData> values, Context context) throws IOException, InterruptedException {

			long cw_1 = 0;
			long cw_2 = 0;
			long cw_1w_2 = 0;
			long N = 0;
			int decade = 0;
			
			for (ExtractedPairData val : values){
				decade = val.decade.get();
				cw_1 = cw_1 + val.firstWordCount.get();
				cw_2 = cw_2 + val.secondWordCount.get();
				N = val.N.get();
				cw_1w_2 = val.pairCount.get();
			}
			ExtractedPairData data = new ExtractedPairData(key,decade,cw_1w_2,cw_1,cw_2,N);
			context.write(key, data);
		}
	}
	
	public static class Partitioner3 extends Partitioner<ExtractedPair, ExtractedPairData> {
		
		// part the keys by the decade
		@Override
	    public int getPartition(ExtractedPair key, ExtractedPairData value, int numPartitions) {
			int curDec = value.decade.get();
			int partitionNum = (curDec % 100)/10;
			if (curDec == 2000 || curDec == 2010){
				partitionNum = partitionNum + 10;
			}
			return partitionNum;
	    }
	}
	
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Step3");
	    job.setJarByClass(Step3.class);
	    job.setMapperClass(Mapper3.class);
	    job.setPartitionerClass(Partitioner3.class);
	    job.setReducerClass(Reducer3.class);
	    job.setNumReduceTasks(12);
	    job.setOutputKeyClass(ExtractedPair.class);
	    job.setOutputValueClass(ExtractedPairData.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}