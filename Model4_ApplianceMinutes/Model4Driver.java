package model4_minutes;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class Model4Driver extends Configured implements Tool{

	@Override
	public int run(String[] arg0) throws Exception {
		FileSystem fs = null;
		String uri = arg0[0];
		
		try
		{
			 Configuration conf = new Configuration();
			 Job job = Job.getInstance(conf, "Model4");
			 
			job.setJarByClass(Model4Driver.class);
		
			fs = FileSystem.get(URI.create(uri), conf);
			if(fs.exists(new Path(arg0[0]+"/MinutesResults/op6")))
				fs.delete(new Path(arg0[0]+"/MinutesResults/op6"));
			
			job.setMapperClass(Model4Mapper.class);
			job.setReducerClass(Model4Reducer.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Consumption.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setNumReduceTasks(1);
			
			FileInputFormat.addInputPath(job, new Path(arg0[0]+"/electricityconsumption/inputMinutes"));
			FileOutputFormat.setOutputPath(job, new Path(arg0[0]+"/electricityconsumption/MinutesResults/op6"));
			
			return job.waitForCompletion(true)?1:0;
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
			return 0;
		}
		finally
		{
			if(fs!=null)
				fs.close();
		}
	}
	
	public static void main(String[] args) {
		try
		{
			ToolRunner.run(new Configuration(),new Model4Driver(), args);
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
	}
	


}
