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

// Step 2 - updates values c(w1) in each pair w1 is in, and same for c(w2). 

public class Step2 {

	public static class Mapper2 extends Mapper<LongWritable, Text, ExtractedPair, ExtractedPairData>{

		//for every pair <w1,w2> creates 4 pairs: <w1,*>, <w1,a>, <w2,*>, <w2,a>
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
			String[] parsedValue = value.toString().split("\t");
			String[] pair = parsedValue[0].split(",");
			Text word1 = new Text(pair[0]);
			Text word2 = new Text(pair[1]);
			String[] pairOccurences = parsedValue[1].split(",");
			int decade = Integer.parseInt(pairOccurences[0]);
			long occurences = Long.parseLong(pairOccurences[1]);
			long N = Long.parseLong(pairOccurences[2]);
			
			String firstPair = pair[0];
			
			if (!firstPair.equals("*")){
				
				ExtractedPair keyPair = new ExtractedPair(word1,new Text("*"));
				ExtractedPairData data = new ExtractedPairData(keyPair, decade, occurences, 0, 0, N);
				context.write(keyPair, data);
					
				keyPair.set(word1,new Text ("a"));
				data.setPair(word1,word2);
				context.write(keyPair, data);
					
				keyPair.set(word2, new Text("*"));
				data.setPair (word2, new Text("*"));
				context.write(keyPair, data);
				
				keyPair.set(word2, new Text("a"));
				data.setPair(word1,word2);
				context.write(keyPair, data);
			}
		}
	}

	public static class Reducer2 extends Reducer<ExtractedPair,ExtractedPairData,ExtractedPair,ExtractedPairData> {

		/* for <w,*> , calculate c(w) by summing the number of occurrences of all the pairs containing w
		   for <w,a> , update c(w) in every pair containing w, according to its location in the pair,
		   and write the pair to the context with the updated data */
		
		LongWritable cw;
		
		@Override
		public void reduce(ExtractedPair key, Iterable<ExtractedPairData> values, Context context) throws IOException, InterruptedException {
			
			String w = key.w1.toString();
			String case1 = key.w2.toString();
			
			// if key = <w,*>, calculate c(w)
			if (case1.equals("*")){
				long sum = 0;
				for (ExtractedPairData value : values){
					sum += value.pairCount.get();
				}
				cw = new LongWritable(sum);
			} 
			// if key = <w,a>, update c(w) in the pairs that contain w
			else {
				for (ExtractedPairData value : values){
					String w1 = value.pair.w1.toString();
					// w is the first word in the pair
					if (w.equals(w1)){
						value.setCw1(cw.get());
					}
					// w is the second word in the pair
					else {
						value.setCw2(cw.get());
					}
					context.write(value.pair, value);
				}
			}
		}
	}

	public static class Partitioner2 extends Partitioner<ExtractedPair, ExtractedPairData> {
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
		Job job = Job.getInstance(conf, "Step2");
		job.setJarByClass(Step2.class);
		job.setMapperClass(Mapper2.class);
		job.setPartitionerClass(Partitioner2.class);
		job.setReducerClass(Reducer2.class);
		job.setNumReduceTasks(12);
		job.setOutputKeyClass(ExtractedPair.class);
		job.setOutputValueClass(ExtractedPairData.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}