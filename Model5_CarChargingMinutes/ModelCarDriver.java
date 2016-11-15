package carmodel;

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

public class ModelCarDriver extends Configured implements Tool{

	@Override
	public int run(String[] arg0) throws Exception {
		FileSystem fs = null;
		String uri = arg0[0];
		try
		{
			Job job = new Job(getConf());
			job.setJobName("Model #Car");
			job.setJarByClass(ModelCarDriver.class);
			
			Configuration conf = job.getConfiguration();
			/*conf.addResource(new Path("core-site.xml"));
			conf.addResource(new Path("hdfs-site.xml"));
			*/
			fs = FileSystem.get(URI.create(uri), conf);
			if(fs.exists(new Path(arg0[0]+"/electricityconsumption/Yan/CarOutputModel")))
				fs.delete(new Path(arg0[0]+"/electricityconsumption/Yan/CarOutputModel"));
			
			job.setMapperClass(ModelCarMapper.class);
			job.setReducerClass(ModelCarReducer.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Consumption.class);
			job.setNumReduceTasks(1);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			
			FileInputFormat.setInputPaths(job, new Path(arg0[0]+"/electricityconsumption/input"));
			FileOutputFormat.setOutputPath(job, new Path(arg0[0]+"/electricityconsumption/Yan/CarOutputModel"));
			
			return job.waitForCompletion(true)?1:0;
			
		}
		catch(Exception ex)
		{
			ex.printStackTrace();return 0;
		}
		finally
		{
			if(fs!=null)fs.close();
		}
	}

	
	public static void main(String[] args) {
		try
		{
			ToolRunner.run(new Configuration(),  new ModelCarDriver(), args);
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
	}
}
