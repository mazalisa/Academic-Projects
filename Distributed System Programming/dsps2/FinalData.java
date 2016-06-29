package dsps2;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class FinalData implements WritableComparable<FinalData>{

	public IntWritable measure;
	public DoubleWritable num;
	
	public FinalData(){
		measure = new IntWritable(0);
		num = new DoubleWritable(0);
	}
	
	public FinalData(int m,double n){
		measure = new IntWritable(m);
		num = new DoubleWritable(n);
	}
	
	public void set(int m,double n){
		measure.set(m);
		num.set(n);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		measure.readFields(in);
		num.readFields(in);
	}
	@Override
	public void write(DataOutput out) throws IOException {
		measure.write(out);
		num.write(out);
	}
	
	@Override
	public int compareTo(FinalData other) {
		int first = measure.compareTo(other.measure);
		if (first == 0){
			return other.num.compareTo(num);
		}
		return first;
	}
	
	public String toString(){
		return measure.toString()+","+num.toString();
	}
	
}