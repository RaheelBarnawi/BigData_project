import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.ArrayList;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Mapper.Context;

public class Kprototype_Mapper extends Mapper {
	
	public final Log logR = LogFactory.getLog(Kprototype_Mapper.class);
	private ArrayList<ClusterSummuray> clusters = new ArrayList<ClusterSummuray>(); 
	private static int number_counter = 0;
	//private static int string_counter=0;
																					

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		logR.info("setup method  ");
		// Read the file containing centroids
		URI centroidURI = Job.getInstance(context.getConfiguration()).getCacheFiles()[0];
		Path centroidsPath = new Path(centroidURI.getPath());
		BufferedReader br = new BufferedReader(new FileReader(centroidsPath.getName().toString()));
		BufferedReader br1 = new BufferedReader(new FileReader(centroidsPath.getName().toString()));
		ArrayList<Double> center_num;// a list for numeric features
		ArrayList<String> center_cate;// a list for categorical features
		String centroid = null;
		int cluster_id = 0;
		ClusterSummuray object;
		NumberFormat f = NumberFormat.getInstance();
		String mix_line = br1.readLine();
		number_counter = get_num_dim(mix_line, 1);
		//string_counter = get_num_dim(mix_line, 2);
		
		while ((centroid = br.readLine()) != null) // read centroids
		{
			logR.info("centerod " + centroid);
			String[] splits = centroid.split("\t")[1].split(" ");
			// each cluster has a centeriod and a clusteriod 
			center_num = new ArrayList<Double>();
			center_cate = new ArrayList<String>();
			for (int k = 0; k < splits.length; k++) {
				if (k < number_counter) {
					try 
					{center_num.add(f.parse(splits[k]).doubleValue()); } 
					catch (ParseException e) 
					{e.printStackTrace();}
					
				    } // end try
				else { // categorical values 
					center_cate.add(splits[k]);
					}
			}
			// set up cluster representations
			object = new ClusterSummuray();
			object.set_center_num(center_num);
			object.set_center_cate(center_cate);
			object.setCluster_id(cluster_id);
			clusters.add(object);
			cluster_id += 1;
		}
	}

	/* this method is used to compute the distance between numeric attribute values of
	  an object and a certriod*/
	
	public double compute_EculdeanDistance(ArrayList<Double> center, ArrayList<Double> datapoint) {
		double differnece = 0.0;
		double square = 0.0;
		double distance = 0.0;
		for (int i = 0; i < datapoint.size(); i++) 
		{
			differnece = datapoint.get(i) - center.get(i);
			// square += Math.pow(difference, 2);
			square += differnece * differnece;
		}
		distance = Math.sqrt(square);
		return distance;
	}

	/*
	 * find the similarity between clusteriod and categorical values. Two values
	 * x and y have similarity value equal to one if x=y otherwise it's zero
	 * then distance= 1- similarity
	 */
	public int compute_MisMatch_distance(ArrayList<String> cateData, ArrayList<String> clusteroid) {
		logR.info("Mismatch ");
		int total_misMatch = 0;
		for (int i = 0; i < cateData.size(); i++) {
			if (cateData.get(i) == clusteroid.get(i))
				total_misMatch += 1;
		}

		return total_misMatch;
	}
// this method used to calculate the number of numeric and categorical features
	public int get_num_dim(String line, int data_type) {
		int number_counter = 0;
		int string_counter = 0;
		boolean isnumber;
		String[] splits = line.split("\t")[1].split(" ");
		for (int i = 0; i < splits.length; i++) {
			isnumber = NumberUtils.isNumber(splits[i]);
			if (isnumber)
				number_counter++;
			else
				string_counter++;
		}
		if (data_type == 1) 
			return number_counter;
		else
			return string_counter;

	}

	// @Override 
	//Emit a cluster id as a key and the data point as a value
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] splits = value.toString().split(" ");
		ArrayList<String> cate_values = new ArrayList<String>();
		ArrayList<Double> num_values = new ArrayList<Double>();
		ClusterSummuray object;
		double mixed_diatance = 0.0;
		double minDistance = 1000000000;
		double num_distance = 0.0;
		int closestCentroid = 0;
		double cate_similarity  = 0.0;
		NumberFormat f = NumberFormat.getInstance();
		for (int i = 0; i < splits.length; i++) {
			if (i < number_counter) // numeric values
			{
				try {
					num_values.add(f.parse(splits[i]).doubleValue());
				    } 
				catch (ParseException e) 
				{e.printStackTrace();}
			} 
			else // categorical values
			{
				cate_values.add(splits[i]);
			}
		}

		for (int j = 0; j < clusters.size(); j++) 
		{
			object = new ClusterSummuray();
			object = clusters.get(j);
			logR.info(" num_ in _object" + object.get_num_center());
			num_distance = compute_EculdeanDistance(object.get_num_center(), num_values);
			cate_similarity = compute_MisMatch_distance(object.get_cate_center(), cate_values);
			mixed_diatance = num_distance + Math.abs((1 - cate_similarity));
			if (mixed_diatance < minDistance) {
				minDistance = mixed_diatance;
				closestCentroid = object.getCluster_id();
			}
		}
		context.write(new IntWritable(closestCentroid), value);
	}
}
