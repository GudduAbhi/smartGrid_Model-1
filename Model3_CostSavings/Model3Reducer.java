package ThirdModel;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Model2Reducer extends Reducer<Text	, Consumption, NullWritable, Text>
{
	private int applianceId;
	private int percent;
	
	@Override
	protected void setup(org.apache.hadoop.mapreduce.Reducer.Context context)
			throws IOException, InterruptedException {
		applianceId = Integer.parseInt(context.getConfiguration().get("APPLIANCEID"));
		percent = Integer.parseInt(context.getConfiguration().get("PERCENT"));
	}
	
	@Override
	protected void reduce(Text arg0, Iterable<Consumption> arg1,
			org.apache.hadoop.mapreduce.Reducer<Text, Consumption, NullWritable, Text>.Context arg2)
			throws IOException, InterruptedException {
		try
		{
			double aird=0.0d,air=0.0d;
			double washerd=0.0d,washer=0.0d;
			double dishd=0.0d,dish=0.0d;
			double dryerd=0.0d,dryer=0.0d;
			double furnaced=0.0d,furnace=0.0d;
			double kitchend=0.0d,kitchen=0.0d;
			double lightsd=0.0d,lights=0.0d;
			double livingd=0.0d,living=0.0d;
			double microd=0.0d,micro=0.0d;
			double totald =0.0d,total=0.0d;
			String total_s;
			String air_s,washer_s,dish_s,dryer_s,furnace_s,kitchen_s,lights_s,living_s,micro_s;
			DecimalFormat df = new DecimalFormat("#0.00");
			
			for(Consumption c : arg1)
			{
				
				totald += Double.parseDouble(c.use.toString());
				aird += Double.parseDouble(c.air.toString());
				washerd += Double.parseDouble(c.washer.toString());
				dishd += Double.parseDouble(c.dish.toString());
				dryerd += Double.parseDouble(c.dryer.toString());
				furnaced += Double.parseDouble(c.furnace.toString());
				kitchend += Double.parseDouble(c.kitchen.toString());
				lightsd += Double.parseDouble(c.lights.toString());
				livingd += Double.parseDouble(c.living.toString());
				microd += Double.parseDouble(c.micro.toString());
				
				total_s = df.format(totald);
				air_s = df.format(aird);
				washer_s= df.format(washerd);
				dish_s=df.format(dishd);
				dryer_s= df.format(dryerd);
				furnace_s= df.format(furnaced);
				kitchen_s= df.format(kitchend);
				lights_s= df.format(lightsd);
				living_s= df.format(livingd);
				micro_s= df.format(microd);
			
				total = Double.parseDouble(total_s);
				air = Double.parseDouble(air_s);
				washer = Double.parseDouble(washer_s);
				dish = Double.parseDouble(dish_s);
				dryer = Double.parseDouble(dryer_s);
				furnace = Double.parseDouble(furnace_s);
				lights = Double.parseDouble(lights_s);
				living = Double.parseDouble(living_s);
				micro = Double.parseDouble(micro_s);
				
			
			}
			
			double percentChangeReqd  = 0;
			
			if(applianceId==1)
			{
				percentChangeReqd = total*(100-percent)/100/(washer+dish+dryer+furnace+kitchen+lights+living+micro);
				arg2.write(NullWritable.get(), new Text(arg0.toString()+","+air+","+washer+","+dish+","+dryer+","+furnace+","+kitchen+","+lights+","+living+","+micro+","+air+","+(washer-(washer*percentChangeReqd/100))+","+(dish-(dish*percentChangeReqd/100))+","+(dryer-(dryer*percentChangeReqd/100))+","+(furnace-(furnace*percentChangeReqd/100))+","+(kitchen-(kitchen*percentChangeReqd/100))+","+(lights-(lights*percentChangeReqd/100))));
				//arg2.write(NullWritable.get(), new Text(arg0.toString()+","+air+","+(washer-(washer*percentChangeReqd/100))+","+(dish-(dish*percentChangeReqd/100))+","+(dryer-(dryer*percentChangeReqd/100))+","+(furnace-(furnace*percentChangeReqd/100))+","+(kitchen-(kitchen*percentChangeReqd/100))+","+(lights-(lights*percentChangeReqd/100))+","+(living-(living*percentChangeReqd/100))+","+(micro-(micro*percentChangeReqd/100))));
			}
			else if(applianceId==2)
			{
				percentChangeReqd = total*(100-percent)/100/(air+dish+dryer+furnace+kitchen+lights+living+micro);
				arg2.write(NullWritable.get(), new Text(arg0.toString()+","+air+","+washer+","+dish+","+dryer+","+furnace+","+kitchen+","+lights+","+living+","+micro+","+(air-(air*percentChangeReqd/100))+","+washer+","+(dish-(dish*percentChangeReqd/100))+","+(dryer-(dryer*percentChangeReqd/100))+","+(furnace-(furnace*percentChangeReqd/100))+","+(kitchen-(kitchen*percentChangeReqd/100))+","+(lights-(lights*percentChangeReqd/100))+","+(living-(living*percentChangeReqd/100))+","+(micro-(micro*percentChangeReqd/100))));
				//arg2.write(NullWritable.get(), new Text(arg0.toString()+","+(air-(air*percentChangeReqd/100))+","+washer+","+(dish-(dish*percentChangeReqd/100))+","+(dryer-(dryer*percentChangeReqd/100))+","+(furnace-(furnace*percentChangeReqd/100))+","+(kitchen-(kitchen*percentChangeReqd/100))+","+(lights-(lights*percentChangeReqd/100))+","+(living-(living*percentChangeReqd/100))+","+(micro-(micro*percentChangeReqd/100))));
						
			}
			else if(applianceId==3)
			{
				percentChangeReqd = total*(100-percent)/100/(washer+air+dryer+furnace+kitchen+lights+living+micro);
				arg2.write(NullWritable.get(), new Text(arg0.toString()+","+air+","+washer+","+dish+","+dryer+","+furnace+","+kitchen+","+lights+","+living+","+micro+","+(air-(air*percentChangeReqd/100))+","+(washer-(washer*percentChangeReqd/100))+","+dish+","+(dryer-(dryer*percentChangeReqd/100))+","+(furnace-(furnace*percentChangeReqd/100))+","+(kitchen-(kitchen*percentChangeReqd/100))+","+(lights-(lights*percentChangeReqd/100))+","+(living-(living*percentChangeReqd/100))+","+(micro-(micro*percentChangeReqd/100))));
				//arg2.write(NullWritable.get(), new Text(arg0.toString()+","+(air-(air*percentChangeReqd/100))+","+(washer-(washer*percentChangeReqd/100))+","+dish+","+(dryer-(dryer*percentChangeReqd/100))+","+(furnace-(furnace*percentChangeReqd/100))+","+(kitchen-(kitchen*percentChangeReqd/100))+","+(lights-(lights*percentChangeReqd/100))+","+(living-(living*percentChangeReqd/100))+","+(micro-(micro*percentChangeReqd/100))));
			}
			else if (applianceId==4)
			{
				percentChangeReqd = total*(100-percent)/100/(washer+dish+air+furnace+kitchen+lights+living+micro);
				arg2.write(NullWritable.get(), new Text(arg0.toString()+","+air+","+washer+","+dish+","+dryer+","+furnace+","+kitchen+","+lights+","+living+","+micro+","+(air-(air*percentChangeReqd/100))+","+(washer-(washer*percentChangeReqd/100))+","+(dish-(dish*percentChangeReqd/100))+","+dryer+","+(furnace-(furnace*percentChangeReqd/100))+","+(kitchen-(kitchen*percentChangeReqd/100))+","+(lights-(lights*percentChangeReqd/100))+","+(living-(living*percentChangeReqd/100))+","+(micro-(micro*percentChangeReqd/100))));
				//arg2.write(NullWritable.get(), new Text(arg0.toString()+","+(air-(air*percentChangeReqd/100))+","+(washer-(washer*percentChangeReqd/100))+","+(dish-(dish*percentChangeReqd/100))+","+dryer+","+(furnace-(furnace*percentChangeReqd/100))+","+(kitchen-(kitchen*percentChangeReqd/100))+","+(lights-(lights*percentChangeReqd/100))+","+(living-(living*percentChangeReqd/100))+","+(micro-(micro*percentChangeReqd/100))));
			}
			else if(applianceId==5)
			{
				percentChangeReqd = total*(100-percent)/100/(washer+dish+dryer+air+kitchen+lights+living+micro);
				arg2.write(NullWritable.get(), new Text(arg0.toString()+","+","+air+","+washer+","+dish+","+dryer+","+furnace+","+kitchen+","+lights+","+living+","+micro+","+(air-(air*percentChangeReqd/100))+","+(washer-(washer*percentChangeReqd/100))+","+(dish-(dish*percentChangeReqd/100))+","+(dryer-(dryer*percentChangeReqd/100))+","+furnace+","+(kitchen-(kitchen*percentChangeReqd/100))+","+(lights-(lights*percentChangeReqd/100))+","+(living-(living*percentChangeReqd/100))+","+(micro-(micro*percentChangeReqd/100))));
				//arg2.write(NullWritable.get(), new Text(arg0.toString()+","+(air-(air*percentChangeReqd/100))+","+(washer-(washer*percentChangeReqd/100))+","+(dish-(dish*percentChangeReqd/100))+","+(dryer-(dryer*percentChangeReqd/100))+","+furnace+","+(kitchen-(kitchen*percentChangeReqd/100))+","+(lights-(lights*percentChangeReqd/100))+","+(living-(living*percentChangeReqd/100))+","+(micro-(micro*percentChangeReqd/100))));
			}
			else if(applianceId==6)
			{
				percentChangeReqd = total*(100-percent)/100/(washer+dish+dryer+furnace+air+lights+living+micro);
				arg2.write(NullWritable.get(), new Text(arg0.toString()+","+","+air+","+washer+","+dish+","+dryer+","+furnace+","+kitchen+","+lights+","+living+","+micro+","+(air-(air*percentChangeReqd/100))+","+(washer-(washer*percentChangeReqd/100))+","+(dish-(dish*percentChangeReqd/100))+","+(dryer-(dryer*percentChangeReqd/100))+","+(furnace-(furnace*percentChangeReqd/100))+","+kitchen+","+(lights-(lights*percentChangeReqd/100))+","+(living-(living*percentChangeReqd/100))+","+(micro-(micro*percentChangeReqd/100))));
				//arg2.write(NullWritable.get(), new Text(arg0.toString()+","+(air-(air*percentChangeReqd/100))+","+(washer-(washer*percentChangeReqd/100))+","+(dish-(dish*percentChangeReqd/100))+","+(dryer-(dryer*percentChangeReqd/100))+","+(furnace-(furnace*percentChangeReqd/100))+","+kitchen+","+(lights-(lights*percentChangeReqd/100))+","+(living-(living*percentChangeReqd/100))+","+(micro-(micro*percentChangeReqd/100))));
			}
			else if(applianceId==7)
			{
				percentChangeReqd = total*(100-percent)/100/(washer+dish+dryer+furnace+kitchen+air+living+micro);
				arg2.write(NullWritable.get(), new Text(arg0.toString()+","+","+air+","+washer+","+dish+","+dryer+","+furnace+","+kitchen+","+lights+","+living+","+micro+","+(air-(air*percentChangeReqd/100))+","+(washer-(washer*percentChangeReqd/100))+","+(dish-(dish*percentChangeReqd/100))+","+(dryer-(dryer*percentChangeReqd/100))+","+(furnace-(furnace*percentChangeReqd/100))+","+(kitchen-(kitchen*percentChangeReqd/100))+","+lights+","+(living-(living*percentChangeReqd/100))+","+(micro-(micro*percentChangeReqd/100))));
				//arg2.write(NullWritable.get(), new Text(arg0.toString()+","+(air-(air*percentChangeReqd/100))+","+(washer-(washer*percentChangeReqd/100))+","+(dish-(dish*percentChangeReqd/100))+","+(dryer-(dryer*percentChangeReqd/100))+","+(furnace-(furnace*percentChangeReqd/100))+","+(kitchen-(kitchen*percentChangeReqd/100))+","+lights+","+(living-(living*percentChangeReqd/100))+","+(micro-(micro*percentChangeReqd/100))));
			}
			else if(applianceId==8)
			{
				percentChangeReqd = total*(100-percent)/100/(washer+dish+dryer+furnace+kitchen+lights+air+micro);
				arg2.write(NullWritable.get(), new Text(arg0.toString()+","+","+air+","+washer+","+dish+","+dryer+","+furnace+","+kitchen+","+lights+","+living+","+micro+","+(air-(air*percentChangeReqd/100))+","+(washer-(washer*percentChangeReqd/100))+","+(dish-(dish*percentChangeReqd/100))+","+(dryer-(dryer*percentChangeReqd/100))+","+(furnace-(furnace*percentChangeReqd/100))+","+(kitchen-(kitchen*percentChangeReqd/100))+","+(lights-(lights*percentChangeReqd/100))+","+living+","+(micro-(micro*percentChangeReqd/100))));
				//arg2.write(NullWritable.get(), new Text(arg0.toString()+","+(air-(air*percentChangeReqd/100))+","+(washer-(washer*percentChangeReqd/100))+","+(dish-(dish*percentChangeReqd/100))+","+(dryer-(dryer*percentChangeReqd/100))+","+(furnace-(furnace*percentChangeReqd/100))+","+(kitchen-(kitchen*percentChangeReqd/100))+","+(lights-(lights*percentChangeReqd/100))+","+living+","+(micro-(micro*percentChangeReqd/100))));
			}
			else if(applianceId==9)
			{
				percentChangeReqd = total*(100-percent)/100/(washer+dish+dryer+furnace+kitchen+lights+living+air);
				arg2.write(NullWritable.get(), new Text(arg0.toString()+","+","+air+","+washer+","+dish+","+dryer+","+furnace+","+kitchen+","+lights+","+living+","+micro+","+(air-(air*percentChangeReqd/100))+","+(washer-(washer*percentChangeReqd/100))+","+(dish-(dish*percentChangeReqd/100))+","+(dryer-(dryer*percentChangeReqd/100))+","+(furnace-(furnace*percentChangeReqd/100))+","+(kitchen-(kitchen*percentChangeReqd/100))+","+(lights-(lights*percentChangeReqd/100))+","+(living-(living*percentChangeReqd/100))+","+micro));
				//arg2.write(NullWritable.get(), new Text(arg0.toString()+","+(air-(air*percentChangeReqd/100))+","+(washer-(washer*percentChangeReqd/100))+","+(dish-(dish*percentChangeReqd/100))+","+(dryer-(dryer*percentChangeReqd/100))+","+(furnace-(furnace*percentChangeReqd/100))+","+(kitchen-(kitchen*percentChangeReqd/100))+","+(lights-(lights*percentChangeReqd/100))+","+(living-(living*percentChangeReqd/100))+","+micro));
			}
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
	}
}
