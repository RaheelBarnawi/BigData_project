import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.StringTokenizer;

public class Dimension_freq1 {
	
	public double compute_EculdeanDistance(ArrayList<Double> intData, ArrayList<Double> centriod)
	{
		// find the difference between the two points 
		double differnece=0.0; 
		double square=0.0; 
		double distance=0.0; 
		
		for (int i=0 ; i<intData.size();i++)
		{
			differnece= intData.get(i) - centriod.get(i);
			//square += Math.pow(difference, 2);
			square += differnece *differnece;	
		}
		distance= Math.sqrt(square);
		return distance; 
	}
	
	public int  compute_MisMatch_distance(ArrayList<String> cateData, ArrayList<String> clusteroid )
	{
		int total_misMatch=0;
		for(int i=0; i< cateData.size();i++)
		{
			if (cateData.get(i)== clusteroid.get(i))
				total_misMatch += 1;		
		}
		
		return total_misMatch; 
	}

	public static void main(String[] args) throws IOException 
	{
		int Max_iteration=100; 
		int number_cluster=3;
		double Lampda=1.5;
		int count =1; 
		BufferedReader br = new BufferedReader(new FileReader("test.txt"));
		String line;
	    while ((line = br.readLine()) != null) 
	    {
	    	String[] token= line.split("\t");
	    	
		   
		}
	}// end Main

}
