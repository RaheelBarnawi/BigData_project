import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class ClusterSummuray {
	
	private  int cluster_id;
	private  Map<Integer, ArrayList<Double>> NumFeatue_value = new HashMap<Integer, ArrayList<Double>>(); // numerical dimensions 
	private  Map<Integer, ArrayList<String>> CatFeatue_value = new HashMap<Integer, ArrayList<String>>(); // Categorical dimensions
	private ArrayList<Integer> dataObject_ids= new ArrayList<Integer>();
	private ArrayList<Double>sum_dimention= new ArrayList<Double>();
	private ArrayList<Double>centriod= new ArrayList<Double>();
	private ArrayList<Double>iteration_ids= new ArrayList<Double>();
	private ArrayList<Double> center_num= new ArrayList<Double>(); 
    private ArrayList<String>center_cate= new ArrayList<String>();
	
	
	public void set_center_num(ArrayList<Double> center)
	{center_num=center; }
	
	public void set_center_cate(ArrayList<String> clusteriod)
	{center_cate=clusteriod; }
	public ArrayList<Double> get_num_center()
	{return center_num;}
	public ArrayList<String> get_cate_center()
	{return center_cate;}
	
	public int getCluster_id() 
	{
		return cluster_id;
	}
	public void setCluster_id(int cluster_id) {
		this.cluster_id = cluster_id;
	}
	//this method searches the hash map and returns the list value in index i
	public  ArrayList<Double> getNumFeatue_value(Integer index) 
	{
		//ArrayList<Integer> value= new ArrayList<Integer>();
		return  NumFeatue_value.get(index);
		
	}
	public void setNumFeatue_value(Integer object_id, ArrayList<Double> values) 
	{
		NumFeatue_value.put(object_id, values);
	}
	public  ArrayList<String> getCatFeatue_value(Integer index) 
	{
		return CatFeatue_value.get(index);
	}
	public void setCatFeatue_value(Integer object_id, ArrayList<String> values) 
	{
		CatFeatue_value.put(object_id, values);
	}
	public ArrayList<Integer> getDataObject_ids() {
		return dataObject_ids;
	}
	
	public void setDataObject_ids(Integer dataObject_ids)
	{
		this.dataObject_ids.add(dataObject_ids);
	} 
	// this method returns numbers of objects in a cluster
	public int  clusterSize()
	{
		return dataObject_ids.size();
	}
	// optimazation functions
		//***************************************************
		public void reset_id_iteration()
		{
			iteration_ids.clear();
		}
	// this method is used to uupdate the center // optimazation 
	public void update_centeriod (ArrayList<Integer>datapoint, int dataPoint_id)
	{
		double current_value=0.0; 
		double new_average=0.0;
		int cluster_size= dataObject_ids.size();
		for (int i=0; i< sum_dimention.size(); i++)
		{

			new_average	= sum_dimention.get(i) / cluster_size;
			centriod.add(i, new_average);
			
		}		
		
	}
	// this method used only in the frist iteration 
	public void update_centeriod1 ()
	{
		// dataObject_ids
		//centriod
		double dim_avg=0.0; 
		int cluster_size=dataObject_ids.size();
		ArrayList<Double> dataObject_values= new ArrayList<Double>();
		for(int i=0; i<sum_dimention.size();i++)
		{
			dim_avg= sum_dimention.get(i) /cluster_size ;
			centriod.add(i,dim_avg);	
		}		
		
	}
	// every time a data point assigned to this cluster we sum over the dimention
	public void sumDimention(ArrayList<Double> datapoint)
	{
		if(sum_dimention.size()==0)
		{
			for( int i=0; i<datapoint.size();i++)
			{
				sum_dimention.add(i, datapoint.get(i));
			}
		}
		else
		{
			double sum_dim=0.0;
			for(int j= 0; j<datapoint.size();j++)
			{
				sum_dim= sum_dimention.get(j)+datapoint.get(j);
				sum_dimention.add(j, sum_dim);
				
			}
			
		}
	}
	//this method is used to remove an object form a cluster when assigned to another cluster
	public void remove_dataobject(int datapoint_id,ArrayList<Integer>datapoint )
	{
		double current_value=0.0; 
		double new_value=0.0; 
		for (int i=0; i<sum_dimention.size(); i++)
		{
			current_value= sum_dimention.get(i);
			new_value= current_value- datapoint.get(i);
			sum_dimention.add(i, new_value);	
		}
		//recalculate the mean of the cluster 
		dataObject_ids.remove(datapoint_id);
	}
	

}
