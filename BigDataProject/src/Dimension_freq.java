import java.util.HashMap;
import java.util.Map;

public class Dimension_freq {
	private int dim_number =0; 
	private Map<String, Integer> dim_values_freq= new HashMap<String, Integer>();
	public  Dimension_freq(int dim_num)
	{
		this.dim_number=dim_num;
	}
	
	public int getDim_number() {
		return dim_number;
	}
	public Map<String, Integer> getDim_values_freq() {
		return dim_values_freq;
	}
	 	
	public void put(String key , Integer freq)
	{
		int frequency=0; 
		if (dim_values_freq.isEmpty())
			dim_values_freq.put(key, freq);
		else
		{
			if(dim_values_freq.containsKey(key))
			{
				frequency= dim_values_freq.get(key)+freq;
				dim_values_freq.put(key, frequency);
			}
			else
				dim_values_freq.put(key, freq);				
		}
	}

}
