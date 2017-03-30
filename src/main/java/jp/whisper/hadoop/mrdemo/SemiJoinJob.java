package jp.whisper.hadoop.mrdemo;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * データ量が少ないデータをキャッシュ化し、ビグファイルと結合する
 * @author whisper
 *
 */
public class SemiJoinJob extends Configured implements Tool {

	public static class SemiJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
		private Map<String, String> hp = new HashMap<String, String>();
		private Text text = new Text();

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {

			String[] fields = StringUtils.split(value.toString(), ',');
			String name = hp.get(fields[0]);
			if (StringUtils.isBlank(name)) {
				return;
			}

			StringBuilder sb = new StringBuilder();
			sb.append(fields[0]).append(',');
			sb.append(name).append(',');
			sb.append(fields[1]).append(',');
			sb.append(fields[2]);
			text.set(sb.toString());	

			context.write(text, NullWritable.get());
		}

		@Override
		protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// コマンドラインから渡したキャッシュファイルを読み込む
			// context.getCacheArchives();//圧縮したファイルも利用可能
			URI[] paths = context.getCacheFiles();
			if (paths == null || paths.length == 0) {
				return;
			}
			try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("student.dat")))) {
				String data;
				String[] fields;
				while ((data = br.readLine()) != null) {
					fields = StringUtils.split(data, ',');

					if (fields == null || fields.length < 2) {
						continue;
					}
					hp.put(fields[0], fields[1]);
				}
			}

		}

	}

	public int run(String[] args) throws Exception {
		if (args.length < 2) {
			System.out.println("<input dir> <output dir>");
			ToolRunner.printGenericCommandUsage(System.out);
			return 2;
		}

		// テスト環境の整備
		Path output = new Path(args[1]);
		FileSystem fs = FileSystem.get(getConf());
		if (fs.exists(output)) {
			fs.delete(output, true);
			System.out.println(output.getName() + " is deleted");
		}

		Job job = Job.getInstance(getConf(), "semijoinjob");
		job.setJarByClass(SemiJoinJob.class);
		job.setMapperClass(SemiJoinMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));// 単一インプットパス
		FileOutputFormat.setOutputPath(job, output);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	/**
	 * 
	 * @param args  -Dmapreduce.job.cache.files=/home/whisper/workspace/tmp/input/student/stu_list#student.dat /home/whisper/workspace/tmp/input/record /home/whisper/workspace/tmp/output
     *クラウド環境では、-files  hdfs:///tmp/input/stu_list#student.dat というような形で使う
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new SemiJoinJob(), args);
		System.exit(result);

	}

}
