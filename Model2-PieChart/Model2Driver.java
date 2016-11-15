package secondModel;

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


public class Model1Driver extends Configured implements Tool{

	@Override
	public int run(String[] arg0) throws Exception {
		FileSystem fs = null;
		try
		{
			Job job = new Job(getConf());
			job.setJobName("Model 1");
			job.setJarByClass(Model1Driver.class);
			
			Configuration conf  = job.getConfiguration();
			/*conf.addResource(new Path("core-site.xml"));
			conf.addResource(new Path("hdfs-site.xml"));*/
			
			fs = FileSystem.get(URI.create(arg0[0]+"/electricityconsumption/output3"), conf);
			if(fs.exists(new Path(arg0[0]+"/electricityconsumption/output3")))
				fs.delete(new Path(arg0[0]+"/electricityconsumption/output3"));
			
			job.setMapperClass(Model1Mapper.class);
			job.setReducerClass(Model1Reducer.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Consumption.class);
			
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			
			//job.setInputFormatClass(NLineInputFormat.class);
			
			FileInputFormat.addInputPath(job, new Path(arg0[0]+"/electricityconsumption/input"));
			FileOutputFormat.setOutputPath(job, new Path(arg0[0]+"/electricityconsumption/output3"));
			
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
			ToolRunner.run(new Configuration(),new Model1Driver(), args);
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
	}
}