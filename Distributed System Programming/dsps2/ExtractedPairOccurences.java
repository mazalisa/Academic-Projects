package dsps2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

public class ExtractedPairOccurences implements WritableComparable<ExtractedPairOccurences> {

	public IntWritable decade;
	public LongWritable occurences; // c(w1,w2)
	public LongWritable N;
	
	public ExtractedPairOccurences(){
		decade = new IntWritable(0);
		occurences = new LongWritable(0);
		N = new LongWritable(0);
	}
	
	public ExtractedPairOccurences(int d, long o, long n){
		set(d,o,n);
	}
	
	public void set(int d, long o, long n){
		decade = new IntWritable(d);
		occurences = new LongWritable(o);
		N = new LongWritable(n);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		decade.readFields(in);
		occurences.readFields(in);
		N.readFields(in);
	}
	@Override
	public void write(DataOutput out) throws IOException {
		decade.write(out);
		occurences.write(out);
		N.write(out);
	}
	
	@Override
	public int compareTo(ExtractedPairOccurences other) {
		return decade.compareTo(other.decade);
	}
	
	public String toString(){
		return decade.toString() + "," + occurences.toString() + "," + N.toString();
	}
	
}
