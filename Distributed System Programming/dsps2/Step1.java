package dsps2;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Step1 {

	public static class Mapper1 extends Mapper<LongWritable, Text, ExtractedPair, ExtractedPairOccurences>{

		private ExtractedPair extractedPair = new ExtractedPair(); // this will be a couple: <w1,w2>
		private ExtractedPairOccurences pairOccur = new ExtractedPairOccurences(); // this will be a triplet: <decade, occurrences, N>
		private HashMap<String,String> ourStopWords = allOfOurStopWords();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {

			// parse the n-gram - in our case: 5-gram
			String[] sentences = value.toString().split("\t");
			String[] sentence = sentences[0].split(" ");
			
			if(sentences.length > 1){
				
				int year = Integer.parseInt(sentences[1]);
				int decade = year-(year%10);
				long occurences = Long.parseLong(sentences[2]);
				pairOccur.set(decade,occurences,0);

				boolean goodYear = (year >= 1900) && sentence.length>1;
				
				// middleWordOfTheN_Gram is the middle word of the current sentence
				String middleWordOfTheN_Gram = cleanWord(sentence[sentence.length/2]);

				// if the year is in a relevant decade (not less then then 1900) then check if the middle word is not a stop word and if the n-gram is containing more that one word
				if (goodYear && !ourStopWords.containsKey(middleWordOfTheN_Gram)){
					for (int i=0 ; i<sentence.length ; i++){
						String currentWord = cleanWord(sentence[i]);
						// create a pair only in terms that currentWord is not a stop word and is not the same as middleWordOfTheN_Gram
						if (currentWord != null && middleWordOfTheN_Gram != null &&
									!ourStopWords.containsKey(currentWord) && !(currentWord.equals(middleWordOfTheN_Gram))){
							// arrange the words in an alphabetic order
							if (currentWord.compareTo(middleWordOfTheN_Gram)<0){
								extractedPair.set(new Text(currentWord),new Text(middleWordOfTheN_Gram));
							}
							else{
								if(currentWord.compareTo(middleWordOfTheN_Gram)>0){
								extractedPair.set(new Text(middleWordOfTheN_Gram),new Text(currentWord));
								}
							}

							context.write(extractedPair, pairOccur);
							
							// this is for calculating the N in the reducer
							context.write(new ExtractedPair(new Text("*"),new Text("*")), new ExtractedPairOccurences(decade,occurences,0));
						}
					}
				}
			}
		}
	
	
		private String cleanWord(String word){
			String notLetters = ".,/;:'\"!@#$%^&*()~`-—{}[]+=_?<>1234567890°";
			String res = "";
			for (int i=0 ; i<word.length() ; i++){
				char currentChar = word.charAt(i);
				if (notLetters.indexOf(currentChar)<0){ // if the char is a letter, keep it
					res = res+currentChar;
				}
			}
			res = res.toLowerCase();
			return res;
		}
	}
	
	public static class Reducer1 extends Reducer<ExtractedPair, ExtractedPairOccurences, ExtractedPair, ExtractedPairOccurences> {
	
		LongWritable N;
	
		@Override
		public void reduce(ExtractedPair key, Iterable<ExtractedPairOccurences> values, Context context) throws IOException, InterruptedException {
	
			// calculating N
			String word1 = key.w1.toString();
			if (word1.equals("*")){
				long n = 0;
				int decade = 0;
				for (ExtractedPairOccurences value : values){
					n = n + value.occurences.get();
					decade = value.decade.get();
				}
				N = new LongWritable(n);
				context.write(key, new ExtractedPairOccurences(decade,N.get(),N.get()));
			} 
			// calculating c(word1,word2) for all pairs, and add N to the value
			else {
				int decade = 0;
				long sum = 0;
				for (ExtractedPairOccurences value : values){
					sum = sum + value.occurences.get();
					decade = value.decade.get();
				}
				context.write(key, new ExtractedPairOccurences(decade,sum,N.get()));
			}
		}
	}
	
	public static class Partitioner1 extends Partitioner<ExtractedPair, ExtractedPairOccurences> {
	
		// part the keys by the decade
		@Override
		public int getPartition(ExtractedPair key, ExtractedPairOccurences value, int numPartitions) {
			int currentDecade = (value.decade.get() % 100)/10;
			if (value.decade.get() == 2000 || value.decade.get() == 2010){
				currentDecade = currentDecade + 10;
			}
			return currentDecade;
		}
	}
	
			private static HashMap<String,String> allOfOurStopWords(){
			String[] stpWrdsArr = {null,"","a","about","above","after","again","against","all","am","an","and","any","are","aren't","as",
					"at","be","because","been","before","being","below","between","both","but","by","can't","cannot","could","couldn't","did",
					"didn't","do","does","doesn't","doing","don't","down","during","each","few","for","from","further","had","hadn't","has",
					"hasn't","have","haven't","having","he","he'd","he'll","he's","her","here","here's","hers","herself","him","himself","his",
					"how","how's","i","i'd","i'll","i'm","i've","if","in","into","is","isn't","it","it's","its","itself","let's","me","more",
					"most","mustn't","my","myself","no","nor","not","of","off","on","once","only","or","other","ought","our","ours","ourselves",
					"out","over","own","same","shan't","she","she'd","she'll","she's","should","shouldn't","so","some","such","than","that",
					"that's","the","their","theirs","them","themselves","then","there","there's","these","they","they'd","they'll","they're",
					"they've","this","those","through","to","too","under","until","up","very","was","wasn't","we","we'd","we'll","we're","we've",
					"were","weren't","what","what's","when","when's","where","where's","which","while","who","who's","whom","why","why's","with",
					"won't","would","wouldn't","you","you'd","you'll","you're","you've","your","yours","yourself","yourselves","b","c","e","f",
					"g","h","i","j","k","l","m","n","o","p","q","r","s","t","u","v","w","x","y","z"};
			HashMap<String,String> stpWrds = new HashMap<String,String>();
			for (int i = 0 ; i < stpWrdsArr.length ; i++){
				stpWrds.put(stpWrdsArr[i],"");
			}
			return stpWrds;
		}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Step1");
		job.setJarByClass(Step1.class);
		job.setMapperClass(Mapper1.class);
		job.setPartitionerClass(Partitioner1.class);
		job.setReducerClass(Reducer1.class);
		job.setNumReduceTasks(12);
		job.setOutputKeyClass(ExtractedPair.class);
		job.setOutputValueClass(ExtractedPairOccurences.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}