package dsps2;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

// Step 4 - Calculates the 3 measures and returns the K pairs with the highest score for each measure.

public class Step4 {

	public static class Mapper4 extends Mapper<LongWritable, Text, FinalData, ExtractedPairData>{
		// for every pair <w1,w2> calculate the 3 measures using the data
		
		@Override
	    public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
			String[] parsedValue = value.toString().split("\t");
			String[] pairData = parsedValue[1].split(",");
			Text word1 = new Text(pairData[0]);
			Text word2 = new Text(pairData[1]);
			int decade = Integer.parseInt(pairData[2]);
			double cw_1w_2 = Double.parseDouble(pairData[3]);
			double cw_1 = Double.parseDouble(pairData[4]);
			double cw_2 = Double.parseDouble(pairData[5]);
			double N = Double.parseDouble(pairData[6]);
			ExtractedPair pair = new ExtractedPair(word1,word2);
			ExtractedPairData data = new ExtractedPairData(pair, decade, (long)cw_1w_2, (long)cw_1, (long)cw_2, (long) N);
			double joint_prob = cw_1w_2 / N;
			double dice_coef = (2 * cw_1w_2) / (cw_1 + cw_2);
			double geometricMean = Math.sqrt(joint_prob*dice_coef);
			//Writing to context the result of the calc
			FinalData result = new FinalData(1,joint_prob);
			context.write(result, data);
			result.set(2, dice_coef);
			context.write(result, data);
			result.set(3, geometricMean);
			context.write(result, data);
		}
	}

	public static class Reducer4 extends Reducer<FinalData,ExtractedPairData,FinalData,ExtractedPair> {
		// writes the first k pairs for each measure
		@Override
	    public void reduce(FinalData key, Iterable<ExtractedPairData> values, Context context) throws IOException, InterruptedException {
			int k = Integer.parseInt(context.getConfiguration().get("k","-1"));
			Iterator<ExtractedPairData> itr = values.iterator();
			for (int i = 0; i < k && itr.hasNext(); i++){
				ExtractedPairData data = (ExtractedPairData) itr.next();
				context.write(key, data.pair);
			}
		}
	}
	
	public static class Partitioner4 extends Partitioner<FinalData, ExtractedPairData> {
		// part the keys by the decade
		@Override
	    public int getPartition(FinalData key, ExtractedPairData value, int numPartitions) {
			int curDec = value.decade.get();
			int partitionNum = (curDec % 100)/10;
			if (curDec == 2000 || curDec == 2010){
				partitionNum = partitionNum + 10;
			}
			return partitionNum;
		}
	}
	
	public static class NaturalKeyGroupingComparator extends WritableComparator {

		protected NaturalKeyGroupingComparator() {
			super(FinalData.class, true);
		}
		
		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable d1, WritableComparable d2) {
			FinalData data1 = (FinalData)d1;
			FinalData data2 = (FinalData)d2;
			int result = data1.measure.get() - data2.measure.get();
			return result;
		}
	}
	
	public static class CompositeKeyComparator extends WritableComparator {
		
		protected CompositeKeyComparator() {
			super(FinalData.class, true);
		}
		
		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable d1, WritableComparable d2) {
			FinalData data1 = (FinalData)d1;
			FinalData data2 = (FinalData)d2;
			
			int result = data1.measure.get() - data2.measure.get();
			if(result == 0) {
				double tmp = data2.num.get() - data1.num.get();
				if (tmp < 0){
					result = -1;
				} else if (tmp > 0){
					result = 1;
				} else {
					result = 0;
				}
			}
			return result;
		}
	}

	
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    conf.set("k", args[2]);
		Job job = Job.getInstance(conf, "Step4");
	    job.setJarByClass(Step4.class);
	    job.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);
		job.setSortComparatorClass(CompositeKeyComparator.class);
	    job.setMapperClass(Mapper4.class);
	    job.setPartitionerClass(Partitioner4.class);
	    job.setReducerClass(Reducer4.class);
	    job.setNumReduceTasks(12);
	    job.setOutputKeyClass(FinalData.class);
	    job.setOutputValueClass(ExtractedPairData.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}