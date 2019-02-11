import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.lib.input.*;


import java.io.*;
import java.net.URI;
import java.util.*;


class Point implements WritableComparable<Point> {
	public double x;
	public double y;
	
	public Point() {}

	public Point(double x, double y)
	{
		this.x=x;
		this.y=y;
	}

	public String toString()
	{
		return this.x + " " + this.y;
	}
	
	public void readFields(DataInput input) throws IOException
	{
		x=input.readDouble();
		y=input.readDouble();
	}
	
	public void write(DataOutput output ) throws IOException
	{
		output.writeDouble(x);
		output.writeDouble(y);
	}
	
	public int compareTo(Point c)
	{
		int compairing  = Double.valueOf(this.x).compareTo(Double.valueOf(c.x));
		
		if(compairing == 0)
		{
			 compairing = Double.valueOf(this.y).compareTo(Double.valueOf(c.y));
		}
		return compairing;
	}

}

 class Avg implements Writable{
	public double sumX;
	public double sumY;
	public long count;

	public Avg() {}

	public Avg(double sumX, double sumY, long count)
	{
		this.sumX=sumX;
		this.sumY=sumY;
		this.count=count;
	}

	public void readFields(DataInput input) throws IOException
	{
		sumX=input.readDouble();
		sumY=input.readDouble();
		count= input.readLong();
	}

	public void write(DataOutput output) throws IOException
	{
		output.writeDouble(sumX);
		output.writeDouble(sumY);
		output.writeLong(count);
	}
}



public class KMeans {

	static Vector<Point> centroids = new Vector(100);
	static Hashtable<Point,Avg> table;

	public static class AvgMapper extends Mapper<Object,Text,Point,Avg> {
		@Override

		protected void setup(Context context) throws IOException
		{
			table= new Hashtable<Point, Avg>();
			URI[] paths = context.getCacheFiles();
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
			BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(paths[0]))));
			String st;
			while ((st = reader.readLine()) != null) {
				List<String> coordinates = Arrays.asList(st.split(","));
				double p1= Double.parseDouble(coordinates.get(0));
				//System.out.println(p1);
				double p2= Double.parseDouble(coordinates.get(1));
				Point p = new Point(p1,p2);
				centroids.add(p);

			}
			for(Point p : centroids)
			{
				System.out.println(p.toString());
				table.put(p,new Avg(0,0,0));
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			Set<Point> set = table.keySet(); 
			for(Point key: set)
			{	
				context.write(key, table.get(key));
			}
		}


		@Override
		public void map (Object key,Text line, Context context)
		{			
			String a = line.toString();
			String[] line_split = a.split(",");
			double xcoord = Double.parseDouble(line_split[0]);
			double ycoord = Double.parseDouble(line_split[1]);
			Point p = new Point(xcoord, ycoord);
			int count= 0;
			double minimum_distance = 10000.00;
			double euclidean_dist;
			int minimum_index = 10000;
			for(Point pt : centroids){
			double xcoord_sq= (xcoord-pt.x)*(xcoord-pt.x);
			double ycoord_sq= (ycoord-pt.y)*(ycoord-pt.y);
			euclidean_dist = Math.sqrt((xcoord_sq)+(ycoord_sq));
			if (euclidean_dist <= minimum_distance)
			{
				minimum_distance = euclidean_dist;
				minimum_index = count;
			}
			
			count++;
			
			}
			try
			{
			//context.write(centroids.get(minimum_index),p);
			//Point i= table.out(minimum_index);
			//Avg a = table.get(centroids.get(minimum_index));
			//Integer ct= centroids.get(count.valueOf());	

			Point i = centroids.get(minimum_index);
			Avg avg = table.get(i);
				if(avg.count==0)
			{
				
				table.put(i,new Avg(xcoord,ycoord,1));
			}
				else
			{
				
				table.put(i,new Avg ((avg.sumX+xcoord), (avg.sumY+ycoord), (avg.count+1)));	
			}
			}
			catch (Exception exception)
			{
			System.out.println("Exception occurred.");
			}

			
			
			}
    
	
}


public static class AvgReducer extends Reducer<Point,Avg,Text,Object> {

	@Override
	public void reduce(Point c, Iterable<Avg> points, Context context) {
		int count = 0;
		double datax = 0.0, datay = 0.0;
		for (Avg pt : points) {
			count+= pt.count;
			datax += pt.sumX;
			datay += pt.sumY;
		}

		c.x = datax/(double)count;
		c.y = datay/(double)count;
		try

		{
			context.write(new Text(c.toString()), NullWritable.get());
		}
		catch (Exception exception)

		{
			System.out.println("Exception");

		}


	}

}






	public static void main ( String[] args ) throws Exception {
		Job job = Job.getInstance();
		job.setJobName("KMeans");

		job.setJarByClass(KMeans.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		job.setMapOutputKeyClass(Point.class);
		//job.setMapOutputValueClass(Point.class);
		job.setMapOutputValueClass(Avg.class);


		job.setMapperClass(AvgMapper.class);
		job.setReducerClass(AvgReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[2]));


		job.addCacheFile(new URI (args[1]));

		job.waitForCompletion(true);


	}
}



