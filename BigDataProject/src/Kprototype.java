
import java.awt.geom.Point2D;
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
import java.util.ArrayList;
import javax.xml.bind.DatatypeConverter;
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

    /**
     * **************
     */
    /**
     * ** Mapper  ***
     */
    /**
     * **************
     */
    public static class KmeansMapper extends Mapper<Object, Text, IntWritable, Text> {

        private ArrayList<Point2D.Double> centroids = new ArrayList<>();
        private ArrayList<Double> center_num= new ArrayList<Double>(); 
        private ArrayList<String>center_cate= new ArrayList<String>();
        private ArrayList<ClusterSummuray> clusters= new ArrayList<ClusterSummuray>(); 
        
        public double compute_EculdeanDistance(ArrayList <Double> center, ArrayList<Double> datapoint)
        {
        	double differnece=0.0; 
    		double square=0.0; 
    		double distance=0.0; 
    		
    		for (int i=0 ; i<datapoint.size();i++)
    		{
    			differnece= datapoint.get(i) - center.get(i);
    			//square += Math.pow(difference, 2);
    			square += differnece *differnece;	
    		}
    		distance= Math.sqrt(square);
    		return distance; 
        }
        // find the similarity between clusteriod and categorical values 
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

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            // Read the file containing centroids
            URI centroidURI = Job.getInstance(context.getConfiguration()).getCacheFiles()[0];
            Path centroidsPath = new Path(centroidURI.getPath());
            BufferedReader br = new BufferedReader(new FileReader(centroidsPath.getName().toString()));
            String centroid = null;
            int index=3;
            ClusterSummuray object; 
            while ((centroid = br.readLine()) != null) 
            {
                String[] splits = centroid.split("\t")[1].split(" ");
                for(int i=0; i<3;  i++)
                {
                	// construct numeric center
                	center_num.add(i,  Double.parseDouble(splits[i]));
                }
                for(int j=0; j<2; j++)
                {
                	center_cate.add(j, splits[index]); 
                	index+=1; 
                }
                
                object= new ClusterSummuray();
                
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Emit (i,value) where i is the id of the closest centroid
            String[] splits = value.toString().split(" ");
            Point2D.Double point = new Point2D.Double(
                    Double.parseDouble(splits[0]),
                    Double.parseDouble(splits[1])
            );
            double minDistance = 1000000000; 
            Point2D.Double closestCentroid = centroids.get(0);
            for (Point2D.Double centroid : centroids) {
                double distance = centroid.distance(point);
                if (distance < minDistance) {
                    minDistance = distance;
                    closestCentroid = centroid;
                }
            }
            context.write(new IntWritable(centroids.indexOf(closestCentroid)),
                          new Text(point.getX() + " " + point.getY()));
        }
    }

    /**
     * ***********
     */
    /**
     * Reducer  *
     */
    /**
     * ***********
     */
    public static class KmeansReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Point2D.Double point = new Point2D.Double(0, 0);
            int nPoints = 0;
            while (values.iterator().hasNext()) {
                nPoints++;
                String[] pointString = values.iterator().next().toString().split(" ");
                point.setLocation(point.getX() + Double.parseDouble(pointString[0]),
                                  point.getY() + Double.parseDouble(pointString[1]));
            }
            point.setLocation(point.getX() / nPoints,
                              point.getY() / nPoints);
            context.write(key, new Text(point.getX() + " " + point.getY()));
        }
    }

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
            }
        } finally {
            out.close();
        }

        // Iterations
        Configuration conf = new Configuration();
        FileChecksum oldChecksum = null;
        for (int i = 0; i < maxIterations; i++) {
            Job job = Job.getInstance(conf, "Kprototype");
            job.setJarByClass(Kprototype.class);
            job.setMapperClass(KmeansMapper.class);
            job.setReducerClass(KmeansReducer.class);
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
            if (oldChecksum != null && newChecksum.equals(oldChecksum)) {
                break; // algorithm converged
            } 
            oldChecksum = newChecksum;
        }
        // Do a final map step to output the classification
        Job job = Job.getInstance(conf, "Kprototype");
        job.setJarByClass(Kprototype.class);
        job.setMapperClass(KmeansMapper.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        String outputPath = baseOutputPath + "-classification";
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.addCacheFile(new URI(centroidFile));
        if (!job.waitForCompletion(true)) // job failed
        {
            System.exit(1);
        } else {
            System.exit(0);
        }
    }
}
