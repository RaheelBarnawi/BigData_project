import java.io.IOException;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Reducer.Context;

public class Kprototype_Reducer extends Reducer<IntWritable, Text, IntWritable, Text>  {

	 	public final static Log logR = LogFactory.getLog(Kprototype_Reducer.class);

	/*this method is used to find the most frequent value (mode) for  each  categorical dimension 
	 @param  dim is a hash map with a categorical value as a key and its frequency as  the value
	 @return the value with high frequency
	 */
	public static String find_most_frequent(Map<String, Integer> dim)
	{
		int freq=0; 
		int max_freq=-1; 
		String mode= null; 
		for (String key : dim.keySet()) 
		{
			freq= dim.get(key);
			if (freq> max_freq)
			{
				max_freq= freq; 
				mode=key; 	
			}    
		}

		return mode; 

	}
	
	 @Override 
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		 /*
		  * Emit a cluster id as a key and  cluster representatives as a value
		  * a cluster in this algorithm has tow representatives: a centriod for numeric features
		  * and a clusteroid for categorical features.
		  * centriod is the average  and
		  *  clusteriod is the most frequent value in each  dimension
		 */
		
		// ------- variables for handling numeric values-----------------
		NumberFormat f = NumberFormat.getInstance(); 
		int numeric_counter= ClusterSummuray.num_feature; 
		ArrayList<Double> center= new ArrayList<Double>(); 
		ArrayList<Double> sum_num_values= new ArrayList<Double>(numeric_counter); 
		Object[] list_dim_sum = new Object[numeric_counter]; 
		int nPoints = 0;
		double temp_value=0.0;
		double current_value=0.0;
		double tempvalue=0.0; 
		double avg=0.0; 
		
		// ------- variables for handling categorical values-----------------
		Map<Integer, Dimension_freq > dim_info= new HashMap<Integer, Dimension_freq >(); 
		int categorical_counter= ClusterSummuray.cate_feature; 
		Dimension_freq  object_dim; 
		Dimension_freq  temp_object; 
		int temp_index=0; 
		ArrayList<String> clusteriod =new ArrayList<String>(); 
		String mode_dim_i= null; 
		

		// Initialize  sum of each numeric dimension  to zero 
		for(int initi=0;initi<numeric_counter; initi++ )
			sum_num_values.add(initi, 0.0);
		
		// prepare categorical dimensions
		//for each dimension; create an object to hold the frequency of values in that dimension 
		for (int i=0; i<categorical_counter; i++)
		{
			object_dim= new Dimension_freq (i);
			
			dim_info.put(i, object_dim);
		}
		
		while (values.iterator().hasNext()) 
		{
			nPoints++;
			String[] mixed_values = values.iterator().next().toString().split(" ");
			// sum numeric values in dimension i 
			for(int i=0; i<numeric_counter; i++)
			{
				try {
					current_value= f.parse(mixed_values[i]).doubleValue();
				} catch (ParseException e) {
					e.printStackTrace();
				} 
				temp_value= sum_num_values.get(i) + current_value;
				list_dim_sum[i]= temp_value;

			}
			

			// record the frequency of each value in j dimensions
			for(int j=0; j<categorical_counter; j++)
			{
				temp_index=numeric_counter+j; 
				temp_object= dim_info.get(j);
				temp_object.put(mixed_values[temp_index],1);
				dim_info.put(j, temp_object);
			}

		}

		
		// compute the mean for each numeric dimension 
		
		for(int i=0; i<list_dim_sum.length; i++)
		{
			tempvalue= (double) list_dim_sum[i];
			avg=tempvalue / nPoints; 
			center.add(i, avg);

		}
		// find the mode for each  categorical dimension	
		
		for(int i=0; i<dim_info.size();i++)
		{
			temp_object=  dim_info.get(i) ; 
			Map<String ,Integer > temp= temp_object.getDim_values_freq();
			mode_dim_i= find_most_frequent(temp);
			clusteriod.add(i, mode_dim_i);

		}
		String cluster_representives = "";
		for(int i=0; i<center.size(); i++)
		{
			cluster_representives+= center.get(i).toString();
			if(i!=numeric_counter-1) 
				cluster_representives+=" ";	
		}
		for(int j=0; j<clusteriod.size();j++)
		{
			cluster_representives+=" ";
			cluster_representives+= clusteriod.get(j);

		}
		logR.info("key"+ key );
		context.write(key, new Text(cluster_representives));

	}
}


