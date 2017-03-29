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

public class Kprototype_Mapper extends Mapper <Object, Text, IntWritable, Text>{
	
    public final Log logR = LogFactory.getLog(Kprototype_Mapper.class);
	private ArrayList<ClusterSummuray> cluster_info = new ArrayList<ClusterSummuray>(); 	
	
	@Override
	public void setup(Context context) throws IOException, InterruptedException {

		/*
		 * Reads clusters_representative file ( centriod + clusteriod)
		 * finds the number of numeric and categorical features
		*/
		URI centroidURI = Job.getInstance(context.getConfiguration()).getCacheFiles()[0];
		Path centroidsPath = new Path(centroidURI.getPath());
		BufferedReader br = new BufferedReader(new FileReader(centroidsPath.getName().toString()));
		BufferedReader br1 = new BufferedReader(new FileReader(centroidsPath.getName().toString()));
		ArrayList<Double> center_num;// a list for numeric features
		ArrayList<String> center_cate;// a list for categorical features
		String centroid = null;
		int cluster_id = 0;
		int numeric_counter = 0;// number of numeric  features
		int categorical_counter=0;// number of categorical features
		ClusterSummuray object;
		NumberFormat f = NumberFormat.getInstance();
		String mix_line = br1.readLine();
		numeric_counter = get_num_dim(mix_line, 1);
		categorical_counter = get_num_dim(mix_line, 2);
		ClusterSummuray.cate_feature= categorical_counter; 
		ClusterSummuray.num_feature=numeric_counter; 
		
		while ((centroid = br.readLine()) != null) 
		{
			
			String[] splits = centroid.split("\t")[1].split(" ");
			center_num = new ArrayList<Double>();
			center_cate = new ArrayList<String>();
			for (int k = 0; k < splits.length; k++) 
			{
				if (k < numeric_counter) {
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
			logR.info(" cluster Id before"+ cluster_id);

			object.setCluster_id(cluster_id);
			logR.info(" cluster Id after"+ object.getCluster_id());
			cluster_info.add(object);
			cluster_id += 1;
		}
	}

	/* 
	 * 
	 * this method is used to compute the distance between numeric attribute values of
	  an object and a certriod
	  @param  center centriod of a class k 
	  @param  datapoint the numeric feature of a data point 
	  @ return distance between the two points
	  
	  */
	
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
	 *  this method finds the similarity between clusteriod and categorical values. Two values
	 * x and y have similarity value equal to one if x=y otherwise it's zero
	 * then distance= 1- similarity
	 * 
	 * @param  cateData centriod of a class k 
	 * @param  cate_datapoint the numeric feature of a data point 
	 *  @ return distance between the two points
	 * 
	 */
	public int compute_MisMatch_distance(ArrayList<String> cate_datapoint, ArrayList<String> clusteroid) {
		//logR.info("Mismatch ");
		int total_misMatch = 0;
		for (int i = 0; i < cate_datapoint.size(); i++) {
			if (cate_datapoint.get(i) == clusteroid.get(i))
				total_misMatch += 1;
		}

		return total_misMatch;
	}

	/*this method used to calculate the number of numeric and categorical features
	 * @param line is a data point with mixed attribute values
	 * @param data_type   a value set either to 1 to return No. of numeric attribute or 2 to return the number 
	 * of the categorical attributes 
	 * 
	 * @return integer value of  the requested type 
	*/
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

	 @Override 
	//Emit a cluster id as a key and the data point as a value
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		 String[] splits = value.toString().split(" ");
		ArrayList<String> cate_values = new ArrayList<String>();
		ArrayList<Double> num_values = new ArrayList<Double>();
		int numeric_counter= ClusterSummuray.num_feature; 
		ClusterSummuray object;
		IntWritable c = new IntWritable();
		
		double mixed_diatance = 0.0;
		double minDistance = 1000000000;
		double num_distance = 0.0;
		int closestCentroid = 0;
		double cate_similarity  = 0.0;
		NumberFormat f = NumberFormat.getInstance();
		for (int i = 0; i < splits.length; i++)
		{
			if (i < numeric_counter) // numeric values
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

		for (int j = 0; j < cluster_info.size(); j++) 
		{ 
			logR.info("*************start distance*****************");
			object = new ClusterSummuray();
			object = cluster_info.get(j);
			/*logR.info("object id"+object.getCluster_id());
			//logR.info("center Numeric values"+object.get_num_center());
		//	logR.info(" center cate values"+object.get_cate_center());
			logR.info("*******************************************");
			logR.info(" Numeric values"+num_values);
			logR.info("  cate values"+cate_values);*/
			
			num_distance = compute_EculdeanDistance(object.get_num_center(), num_values);
			cate_similarity = compute_MisMatch_distance(object.get_cate_center(), cate_values);
			mixed_diatance = num_distance ;
			/*logR.info(" out_ miniDistance="+minDistance);
			logR.info(" out_mixDistance="+mixed_diatance);
			logR.info("*******************************************");*/
			//mixed_diatance = num_distance + 0.2* ( Math.abs((1 - cate_similarity)));
			if (mixed_diatance < minDistance) 
			{
				/*logR.info("Enter if mixed_diatance < minDistance");
				logR.info("miniDistance="+minDistance);
				logR.info("mixDistance="+mixed_diatance);*/
				minDistance = mixed_diatance;
				//closestCentroid = object.getCluster_id();
				closestCentroid=j;
				logR.info("closestCentroid="+closestCentroid);
			}
			logR.info("miniDistance="+minDistance);
			
		}
		logR.info("Conclustio ");
		logR.info("closestCentroid "+closestCentroid);
		logR.info(" Numeric values"+num_values);
		logR.info("  cate values"+cate_values);
		c.set(closestCentroid);
		//context.write(new IntWritable(closestCentroid), value);
		context.write(c, value);
	}
}
