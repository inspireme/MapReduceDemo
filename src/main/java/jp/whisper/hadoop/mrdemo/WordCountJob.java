package jp.whisper.hadoop.mrdemo;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * ワード数の統計
 * @author whisper
 *
 */
public class WordCountJob extends Configured implements Tool {

	public static class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
		private Text word = new Text();
		private LongWritable lw = new LongWritable(1);

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			String[] words = StringUtils.split(value.toString(), ' ');
			for (String w : words) {
				word.set(w);
				context.write(word, new LongWritable(1));
			}

		}

	}

	public static class WordCountReduce extends Reducer<Text, LongWritable, Text, LongWritable> {

		private LongWritable result = new LongWritable();

		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,
				Reducer<Text, LongWritable, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {

			long sum = 0L;
			for (LongWritable v : values) {
				sum += v.get();
			}
			result.set(sum);
			context.write(key, result);

		}

	}

	public int run(String[] args) throws Exception {
		if (args.length < 2) {
			System.out.println("<input dir> <output dir>");
			return 2;
		}

		// テスト環境の整備
		Path output = new Path(args[1]);
		FileSystem fs = FileSystem.get(getConf());
		if (fs.exists(output)) {
			fs.delete(output,true);
			System.out.println(output.getName()+" is deleted");
		}

		Job job = Job.getInstance(getConf(), "wordcountjob");
		job.setJarByClass(WordCountJob.class);
		job.setMapperClass(WordCountMapper.class);
		// job.setCombinerClass(WordCountReduce.class);
		job.setReducerClass(WordCountReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));//単一インプットパス
		FileOutputFormat.setOutputPath(job, output);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	/**
	 * 
	 * @param args /home/whisper/workspace/tmp/input /home/whisper/workspace/tmp/output
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new WordCountJob(), args);
		System.exit(result);

	}

}
