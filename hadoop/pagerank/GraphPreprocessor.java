import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GraphPreprocessor {

    public static class GraphMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty() || line.startsWith("#")) {
                return; // Skip empty lines and comments
            }

            String[] parts = line.split("\\s+");
            if (parts.length >= 2) {
                String from = parts[0];
                String to = parts[1];
                
                // Emit the edge
                context.write(new Text(from), new Text(to));
                
                // Ensure destination node exists even if it has no outgoing links
                context.write(new Text(to), new Text(""));
            }
        }
    }

    public static class GraphReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text page, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            Set<String> links = new HashSet<>();
            
            for (Text value : values) {
                String link = value.toString().trim();
                if (!link.isEmpty()) {
                    links.add(link);
                }
            }

            Configuration conf = context.getConfiguration();
            int totalNodes = conf.getInt("graph.nodes", 1);
            double initialPageRank = 1.0 / totalNodes;

            StringBuilder linkList = new StringBuilder();
            boolean first = true;
            for (String link : links) {
                if (!first) {
                    linkList.append(",");
                }
                linkList.append(link);
                first = false;
            }

            context.write(page, new Text(initialPageRank + "\t" + linkList.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        
        if (args.length != 3) {
            System.err.println("Usage: GraphPreprocessor <input path> <output path> <nodes>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.setInt("graph.nodes", Integer.parseInt(args[2]));

        Job job = Job.getInstance(conf, "graph preprocessor");
        job.setJarByClass(GraphPreprocessor.class);
        job.setMapperClass(GraphMapper.class);
        job.setReducerClass(GraphReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
} 