import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import javax.xml.bind.DatatypeConverter;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Kprototype {

	// private static int string_counter=0; 

    /**
     * **************
     */
    /**
     * ** Mapper  ***
     */
    /**
     * **************
     */
	/*
    public static class KmeansMapper extends Mapper<Object, Text, IntWritable, Text> {

    	public final Log logR = LogFactory.getLog(KmeansMapper.class);
        private ArrayList<ClusterSummuray> clusters= new ArrayList<ClusterSummuray>();  //  a list to hold the clusters'information 
        
        // this method is used to compute the distance between numeric features and centriod
        public double compute_EculdeanDistance(ArrayList <Double> center, ArrayList<Double> datapoint)
        {
        	logR.info("compute Eucledian ");
        	double differnece=0.0; 
    		double square=0.0; 
    		double distance=0.0; 
    		logR.info("center size"+center.size()+ "   " + "data size"+ datapoint.size());
    		logR.info("center info"+center);
    		for (int i=0 ; i<datapoint.size();i++)
    		{
    			differnece= datapoint.get(i) - center.get(i);
    			//square += Math.pow(difference, 2);
    			square += differnece *differnece;	
    		}
    		distance= Math.sqrt(square);
    		logR.info(" finish compute Eucledian ");
    		return distance; 
        }
        
        
        /* find the similarity between clusteriod and categorical values
          two values x and y have similarity value equal to one if x=y otherwise it's zero
          then distance= 1- similarity 
        
        public int  compute_MisMatch_distance(ArrayList<String> cateData, ArrayList<String> clusteroid )
    	{
        	logR.info("Mismatch ");
    		int total_misMatch=0;
    		for(int i=0; i< cateData.size();i++)
    		{
    			if (cateData.get(i)== clusteroid.get(i))
    				total_misMatch += 1;		
    		}
    		
    		return total_misMatch; 
    	}
        public int get_num_dim(String line, int data_type)
        {
        	int  number_counter=0;
        	int string_counter=0; 
        	boolean isnumber;
        	
        	
        	String[] splits = line.split("\t")[1].split(" ");
        	for(int i=0 ; i<splits.length; i++)
        	{
        		isnumber=   NumberUtils.isNumber(splits[i]);
       		 if(isnumber)
       			 number_counter++;
       		 else
       			 string_counter++;
        	}
        	if (data_type==1)
        		return number_counter; 
        	else
        		return string_counter; 
        		
        	
        	 
        }

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
        	logR.info("setup method  " ); 
            // Read the file containing centroids
            URI centroidURI = Job.getInstance(context.getConfiguration()).getCacheFiles()[0];
            Path centroidsPath = new Path(centroidURI.getPath());
            BufferedReader br = new BufferedReader(new FileReader(centroidsPath.getName().toString()));
            BufferedReader br1 = new BufferedReader(new FileReader(centroidsPath.getName().toString()));
            String mix_line= br1.readLine();
            int string_counter=0; 
            int number_counter=0; 
            number_counter= get_num_dim(mix_line,1);
            string_counter= get_num_dim(mix_line,2);
            logR.info(" number of categoriacl "+string_counter +" number of int" + number_counter);  

            

            ArrayList<Double> center_num;// a list for numeric features
            ArrayList<String>center_cate;// a list for categorical features
            String centroid = null;
            int index=3;
            int cluster_id=0; 
            ClusterSummuray object; 
            NumberFormat f = NumberFormat.getInstance();
            
            
        	
            while ((centroid = br.readLine()) != null) 
            {
            	logR.info("centerod " + centroid); 
                String[] splits = centroid.split("\t")[1].split(" ");
                int s_size= splits.length;
                center_num= new ArrayList<Double>(); 
                center_cate= new ArrayList<String>();
                
               logR.info("string length  "+ s_size + "split [1]"+ splits[1]); 
                for (int k=0; k<splits.length; k++)
                {
                	 if (k<number_counter)
                	 {
                		 
                		 
                		 try {
 							logR.info("try to add ");

							center_num.add(f.parse(splits[k]).doubleValue());
						} catch (ParseException e) {
							logR.info("problem in catch ");
							e.printStackTrace();
						}
                		 //center_num.add(Double.parseDouble(splits[k])); 
                	 }
                	 else
                	 {
                		 
                		 center_cate.add(splits[k]); 
                	 }		 
                }              
                logR.info("center size before adding");
                // set up  cluster representations 
                object= new ClusterSummuray();
                object.set_center_num(center_num);
                object.set_center_cate(center_cate);
                object.setCluster_id(cluster_id);
                clusters.add(object);  
                cluster_id+=1; 
            }
            logR.info(" finish setup method  " );  
            //logR.info(" number of categoriacl "+string_counter +" number of int" + number_counter);  
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Emit (i,value) where i is the id of the closest centroidlogR.info("setup " ); 
        logR.info("map method " ); 
      //  logR.info("value " +value); 
            String[] splits = value.toString().split(" ");
            // split the data point into two parts, numeric and categorical 
            ArrayList<String> cate_values= new ArrayList<String>();
            ArrayList<Double> num_values= new ArrayList<Double>(); 
            ClusterSummuray object; 
            double mixed_diatance= 0.0; 
            double minDistance = 1000000000; 
            double num_distance= 0.0;
            int closestCentroid=0;
            double cate_distance=0.0; 
            NumberFormat f = NumberFormat.getInstance();
            double vv = 0.0;
            for(int i=0; i<splits.length; i++)
            {
            	if (i<5) // numeric values
            	{
                   logR.info(" i<5  " );  
            		//current_value= f.parse(mixed_values[i]).doubleValue();
            		
            		try {
            			//logR.info(" try  " ); 
            			
						num_values.add(f.parse(splits[i]).doubleValue());
						//logR.info(" num_value size " + num_values.size()); 
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						logR.info("problem in catch 2 ");
						e.printStackTrace();
					}

            	//	num_values.add(Double.parseDouble(splits[i]) );
            	}
            	else // categorical values
            	{
            		 logR.info(" i>5  " );  
            		cate_values.add(splits[i]);
            	}
            }
                    
            for (int j=0; j< clusters.size(); j++) 
            { 
            	object= new ClusterSummuray (); 
            	object= clusters.get(j);
               logR.info(" num_ in _object" + object.get_num_center()); 
            	num_distance= compute_EculdeanDistance(object.get_num_center(), num_values);
            	cate_distance= compute_MisMatch_distance( object.get_cate_center(),cate_values );
            	mixed_diatance= num_distance + Math.abs((1- cate_distance));// 
             
                if (mixed_diatance < minDistance) {
                    minDistance = mixed_diatance;
                    closestCentroid = object.getCluster_id();
                }
            }
            
            context.write(new IntWritable(closestCentroid),value);
          logR.info(" finish map method " );                 
        }
    }*/

    /**
     * ***********
     */
    /**
     * Reducer  *
     */
    /**
     * ***********
     */
    /*
    public static class KmeansReducer extends Reducer<IntWritable, Text, IntWritable, Text> 
    {
    	public final static Log logR = LogFactory.getLog(KmeansMapper.class);

    	public static String find_most_frequent(Map<String, Integer> dim)
    	{
    		logR.info("find_most_frequen" );
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
           
        	logR.info("Reducer " );
        	
        	NumberFormat f = NumberFormat.getInstance(); // Gets a NumberFormat with the default locale, you can specify a Locale as first parameter (like Locale.FRENCH)
        	 // myNumber now contains 20
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
        	logR.info("sum_num_values" +sum_num_values);
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
               logR.info("mixed_values"+mixed_values.length );
                // sum numeric values in dimension i 
                for(int i=0; i<dim_num; i++)
                {
            
                	//logR.info("sum_num" );
                		
                	   // current_value= Double.parseDouble(mixed_values[i]);
                	    try {
							current_value= f.parse(mixed_values[i]).doubleValue();
						} catch (ParseException e) {
							// TODO Auto-generated catch block
							logR.info("problem in catch 3 ");
							e.printStackTrace();
						} 
                		temp_value= sum_num_values.get(i) + current_value;
                		//sum_num_values.add(i, temp_value);  
                		list[i]= temp_value;
                	
                }
             // logR.info("sum_num"+ sum_num_values);
              logR.info("list sum"+ list.length);
             // logR.info("list sum"+ list.toString());

              
                for(int j=0; j<dim_cate; j++)
                {
                	temp_index=dim_num+j; 
                	//logR.info("temp_index " + temp_index);
                	temp_object= dim_info.get(j);
                	temp_object.put(mixed_values[temp_index],1);
                	dim_info.put(j, temp_object);
                }
                    
            }// end reading list of values 
        	
        	double v=0.0; 
        	// compute the mean for each i_num dimension 
        	double avg=0.0; 
        	for(int i=0; i<list.length; i++)
        	{
        		v= (double) list[i];
        		avg=v / nPoints; 
        		//logR.info("avg " + avg);
        		//centriod.add(i, avg);	
        		center.add(i, avg);
        		
        	}
        	logR.info("center"+ center);
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
            logR.info("centroid_size " +center.size());
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
            logR.info("cluster_representive length" + cluster_representive.length());
      	
            logR.info("cluster_representive " + cluster_representive);
            context.write(key, new Text(cluster_representive));
        	
          logR.info("end reduce" );
        }
    }*/

    /**
     * *********
     */
    /**
     * Main  *
     */
    /**
     * *********
     */
    public static void usage(){
        System.out.println("usage: Kmeans <inputPath> <baseOutputPath> <nClusters> <maxIterations> \n "
                + "<inputPath> is the path to the data (text file) to cluster. It might be on the local "
                + "file system or HDFS. \n <baseOutputPath> must be on HDFS.");
    }
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, Exception {
        // Arguments parsing
        if(args.length != 4)
            usage();
        String inputPath = args[0];
        String baseOutputPath = args[1];
        int nClusters = Integer.parseInt(args[2]);
        int maxIterations = Integer.parseInt(args[3]);

        // Centroid initialization
        FileSystem fs = FileSystem.get(URI.create(inputPath), new Configuration());
        Path path = new Path(inputPath);
        InputStream in = null;
        ArrayList<String> centroids = new ArrayList<>();
        BufferedReader br = null;
        // On this dataset, choosing the k first elements is like choosing k random elements
        try {
            in = fs.open(path);
            br = new BufferedReader(new InputStreamReader(in));
            String line = null;
            for (int i = 0; i < nClusters; i++) {
                centroids.add(br.readLine());
            }
        } finally {
            IOUtils.closeStream(in);
        }

        // Now we will write the centroids in a file
        String centroidFile = baseOutputPath + "-centroids.txt";
        fs = FileSystem.get(URI.create(centroidFile), new Configuration());
        OutputStream out = fs.create(new Path(centroidFile));
        try {
            int i = 0;
            for (String s : centroids) {
                out.write((i++ + "\t" + s + "\n").getBytes());
                System.out.println("line"+ s);
            }
        } finally {
            out.close();
        }

        // Iterations
        Configuration conf = new Configuration();
        FileChecksum oldChecksum = null;
        for (int i = 0; i < maxIterations; i++) 
        {
        	 System.out.println(" start of iteration number "+ (i));
            Job job = Job.getInstance(conf, "Kprototype");
            job.setJarByClass(Kprototype.class);
           // job.setMapperClass(KmeansMapper.class);
            job.setMapperClass(Kprototype_Mapper.class);
            job.setReducerClass(Kprototype_Reducer.class);
            //job.setReducerClass(KmeansReducer.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(inputPath));
            String outputPath = baseOutputPath + "-" + i;
            // Set job output
            FileOutputFormat.setOutputPath(job, new Path(outputPath));
            // Distribute the centroid file to the mappers using the distributed cache
            job.addCacheFile(new URI(centroidFile));
            // Run the job
            if (!job.waitForCompletion(true)) // job failed
            {
                System.exit(1);
            }
            // Prepare for the next iteration
            centroidFile = outputPath + "/part-r-00000";
            // Compute a hash of the centroid file
            fs = FileSystem.get(URI.create(centroidFile), new Configuration());
            path = new Path(centroidFile);
            FileChecksum newChecksum = fs.getFileChecksum(path); // will return null unless path is on hdfs
            if (oldChecksum != null && newChecksum.equals(oldChecksum)) 
            {
                break; // algorithm converged
            } 
            oldChecksum = newChecksum;
            System.out.println(" Ende of iteration number "+ (i));
        }
       // Do a final map step to output the classification
        System.out.println(" End of clustring  ");
        Job job = Job.getInstance(conf, "Kprototype");
        job.setJarByClass(Kprototype.class);
       // job.setMapperClass(KmeansMapper.class);
        job.setMapperClass( Kprototype_Mapper.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        String outputPath = baseOutputPath + "-classification";
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.addCacheFile(new URI(centroidFile));
        if (!job.waitForCompletion(true)) // job failed
        {
            System.exit(1);
        } 
        else {
            System.exit(0);
        }
    }
}
