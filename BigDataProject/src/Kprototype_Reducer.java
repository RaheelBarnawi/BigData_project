import java.io.IOException;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;



public class Kprototype_Reducer extends Reducer<IntWritable, Text, IntWritable, Text>  {

	// 
	//this method is used to find the most frequent value for  each  categorical dimension 
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
	
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		
		NumberFormat f = NumberFormat.getInstance(); // Gets a NumberFormat with the default locale, you can specify a Locale as first parameter (like Locale.FRENCH)
		Map<Integer, Dimension_freq > dim_info= new HashMap<Integer, Dimension_freq >(); 
		ArrayList<String> cate_values= new ArrayList<String>();
		ArrayList<Double> sum_num_values= new ArrayList<Double>(5); 
		ArrayList<Double> center= new ArrayList<Double>(); 
		Object[] list = new Object[5];
		int dim_cate=6; // categorical dimension
		int dim_num=5; 
		Dimension_freq  object; 
		Dimension_freq  temp_object; 
		int temp_index=0; 
		int nPoints = 0;
		double temp_value=0.0;
		double current_value=0.0;
		
		
		//	int temp_value=0; 
		// 1- handling numerical part
		// compute the mean of the values
		// Initialize  sum_num_values 
		for(int initi=0;initi<dim_num; initi++ )
			sum_num_values.add(initi, 0.0);
		// prepare categorical dimensions
		//for each dimension create an object to hold the frequency
		//of values in that dimension 
		for (int i=0; i<dim_cate; i++)
		{
			//logR.info("create dim_cate ");
			object= new Dimension_freq (i);
			dim_info.put(i, object);
		}
		while (values.iterator().hasNext()) 
		{
			nPoints++;
			String[] mixed_values = values.iterator().next().toString().split(" ");
			// sum numeric values in dimension i 
			for(int i=0; i<dim_num; i++)
			{

				//logR.info("sum_num" );

				// current_value= Double.parseDouble(mixed_values[i]);
				try {
					current_value= f.parse(mixed_values[i]).doubleValue();
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} 
				temp_value= sum_num_values.get(i) + current_value;
				//sum_num_values.add(i, temp_value);  
				list[i]= temp_value;

			}
			

			for(int j=0; j<dim_cate; j++)
			{
				temp_index=dim_num+j; 
				//logR.info("temp_index " + temp_index);
				temp_object= dim_info.get(j);
				temp_object.put(mixed_values[temp_index],1);
				dim_info.put(j, temp_object);
			}

		}// end reading list of values 

		double tempvalue=0.0; 
		// compute the mean for each i_num dimension 
		double avg=0.0; 
		for(int i=0; i<list.length; i++)
		{
			tempvalue= (double) list[i];
			avg=tempvalue / nPoints; 
			center.add(i, avg);

		}
		// compute the mode for each i_cate dimension	
		ArrayList<String> clusteriod =new ArrayList<String>(); 
		String mode_dim_i= null; 
		for(int i=0; i<dim_info.size();i++)
		{
			temp_object=  dim_info.get(i) ; 
			Map<String ,Integer > temp= temp_object.getDim_values_freq();
			mode_dim_i= find_most_frequent(temp);
			clusteriod.add(i, mode_dim_i);

		}
		String cluster_representive = "";
		for(int i=0; i<center.size(); i++)
		{
			cluster_representive+= center.get(i).toString();
			if(i!=4)
				cluster_representive+=" ";	
		}
		for(int j=0; j<clusteriod.size();j++)
		{
			cluster_representive+=" ";
			cluster_representive+= clusteriod.get(j);

		}

		context.write(key, new Text(cluster_representive));

	}
}


