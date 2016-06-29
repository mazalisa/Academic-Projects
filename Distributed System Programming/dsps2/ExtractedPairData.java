package dsps2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class ExtractedPairData implements WritableComparable<ExtractedPairData> {

	public ExtractedPair pair;
	public IntWritable decade;
	public LongWritable pairCount;
	public LongWritable firstWordCount;
	public LongWritable secondWordCount;
	public LongWritable N;
	
	public ExtractedPairData(){
		pair = new ExtractedPair();
		decade = new IntWritable(0);
		pairCount = new LongWritable(0);
		firstWordCount = new LongWritable(0);
		secondWordCount = new LongWritable(0);
		N = new LongWritable(0);
	}
	
	public ExtractedPairData(ExtractedPair p, int d, long pc, long c1, long c2, long n){
		pair = new ExtractedPair(p.w1,p.w2);
		decade = new IntWritable(d);
		pairCount = new LongWritable(pc);
		firstWordCount = new LongWritable(c1);
		secondWordCount = new LongWritable(c2);
		N = new LongWritable(n);
	}
	
	public void setPair(Text w1,Text w2){
		pair.set(w1, w2);
	}
	
	public void setCw1(long num){
		firstWordCount.set(num);
	}
	
	public void setCw2(long num){
		secondWordCount = new LongWritable(num);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		pair.readFields(in);
		decade.readFields(in);
		pairCount.readFields(in);
		firstWordCount.readFields(in);
		secondWordCount.readFields(in);
		N.readFields(in);
	}
	@Override
	public void write(DataOutput out) throws IOException {
		pair.write(out);
		decade.write(out);
		pairCount.write(out);
		firstWordCount.write(out);
		secondWordCount.write(out);
		N.write(out);
	}
	@Override
	public int compareTo(ExtractedPairData other) {
		int comparedPair = pair.compareTo(other.pair);
		if (comparedPair == 0){
			return decade.compareTo(other.decade);
		}
		return comparedPair;
	}
	
	public String toString(){
		return pair.w1.toString()+","+pair.w2.toString()+","+decade.toString()+
				","+pairCount.toString()+","+firstWordCount.toString()+","+secondWordCount.toString()+","+N.toString();
	}
	
	
}
