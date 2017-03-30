package jp.whisper.hadoop.mrdemo;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * グローバルソートを実現する。REDUCEうちでソート順を確保できるけれど、複数REDUCEを指定するにしたがって、パティションが複数であり、バティション間で
 * ソート順を確保することができず、MAPPER処理である規則に従い、データを適切なパティションを指定する必要がある →複数REDUCE
 * データが均等的に各パティションに文されていない恐れがあるため、実際のデータ状況を分析して、パティション化するアルゴリズムを指定すること。
 * @author whisper
 *
 */
public class GlobalSortJob2 extends Configured implements Tool {

	public static class GlobalSortMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
		private LongWritable sortKey = new LongWritable();

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, Text>.Context context)
				throws IOException, InterruptedException {
			String[] fields = StringUtils.split(value.toString(), ',');
			if (fields == null || fields.length != 2) {
				return;
			}

			sortKey.set(Long.parseLong(fields[1]));
			context.write(sortKey, value);

		}

	}

	public static class GlobalSortReduce extends Reducer<LongWritable, Text, Text, NullWritable> {

		private LongWritable result = new LongWritable();

		@Override
		protected void reduce(LongWritable key, Iterable<Text> values,
				Reducer<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {

			for (Text text : values) {
				context.write(text, NullWritable.get());
			}

		}

	}

	public static class LongValueDescComparator extends WritableComparator {

		public LongValueDescComparator() {
			super(LongWritable.class, true);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return -(super.compare(b1, s1, l1, b2, s2, l2));
		}

	}

	public static class RangePatitioner extends Partitioner<LongWritable, Text> {
		private long maxValue = 100L;

		@Override
		public int getPartition(LongWritable key, Text value, int numPartitions) {
			if (numPartitions <= 1) {
				return 0;// 単一パティション
			}
			long k = key.get();
			if (k >= maxValue) {
				return numPartitions - 1;
			}
			long section = maxValue / numPartitions + 1;

			return (int) (k / section);
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
			fs.delete(output, true);
			System.out.println(output.getName() + " is deleted");
		}

		Job job = Job.getInstance(getConf(), "GlobalSortjob");
		job.setJarByClass(GlobalSortJob2.class);
		job.setMapperClass(GlobalSortMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(GlobalSortReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setSortComparatorClass(LongValueDescComparator.class);
		job.setNumReduceTasks(3);
		job.setPartitionerClass(RangePatitioner.class);
	

		FileInputFormat.addInputPath(job, new Path(args[0]));// 単一インプットパス
		FileOutputFormat.setOutputPath(job, output);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	/**
	 * 
	 * @param args
	 *            /home/whisper/workspace/tmp/input
	 *            /home/whisper/workspace/tmp/output
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new GlobalSortJob2(), args);
		System.exit(result);

	}

}
