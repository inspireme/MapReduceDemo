package jp.whisper.hadoop.mrdemo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * データファイルを結合し、統計データを生成する
 * 補助：インプットファイルパスをコンマで区切って一つストリングとして指定する
 * 
 * @author whisper
 *
 */
public class JoinJob extends Configured implements Tool {

	public static class JoinMapper extends Mapper<LongWritable, Text, Text, JoinValueWritable> {
		private Text stuNo = new Text();
		private JoinValueWritable joinVal = new JoinValueWritable();

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, JoinValueWritable>.Context context)
				throws IOException, InterruptedException {
			FileSplit split = (FileSplit) context.getInputSplit();

			if (split.getPath().toString().indexOf("stu") >= 0) {// stu data
				joinVal.setTag((byte) 1);
			} else {// record data
				joinVal.setTag((byte) 2);
			}
			String[] fields = StringUtils.split(value.toString(), ',');
			stuNo.set(fields[0]);
			joinVal.setContent(value);

			context.write(stuNo, joinVal);

		}

	}

	public static class JoinReduce extends Reducer<Text, JoinValueWritable, Text, NullWritable> {

		private List<String> stuList = new ArrayList<String>();
		private List<String> recordList = new ArrayList<String>();
		private Text result = new Text();

		@Override
		protected void reduce(Text key, Iterable<JoinValueWritable> values,
				Reducer<Text, JoinValueWritable, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			stuList.clear();
			recordList.clear();
			// データを分けて一時保存する
			for (JoinValueWritable jvw : values) {
				if (jvw.getTag() == (byte) 1) {
					stuList.add(jvw.getContent().toString());
				} else {
					recordList.add(jvw.getContent().toString());
				}
			}

			// データを出力する
			for (String s : stuList) {
				for (String rcd : recordList) {
					result.set(s + "\t" + rcd);
					context.write(result, NullWritable.get());
				}
			}

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

		Job job = Job.getInstance(getConf(), "joinjob");
		job.setJarByClass(JoinJob.class);
		job.setMapperClass(JoinMapper.class);
		// job.setCombinerClass(WordCountReduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(JoinValueWritable.class);
		job.setReducerClass(JoinReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		// FileInputFormat.addInputPath(job, new Path(args[0]));//単一インプットパス
		FileInputFormat.addInputPaths(job, args[0]); // 「，」で区切られた複数パス
		FileOutputFormat.setOutputPath(job, output);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	/**
	 * 
	 * @param args /home/whisper/workspace/tmp/input/student,/home/whisper/workspace/tmp/input/record  /home/whisper/workspace/tmp/output
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new JoinJob(), args);
		System.exit(result);

	}

}
