import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRank {

    private static final double DAMPING = 0.85;
    private static final double EPSILON = 0.0001;
    private static final NumberFormat nf = new DecimalFormat("00");

    public static class PageRankMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {

            int tIdx = value.toString().indexOf('\t');
            if (tIdx == -1) return;

            String page = value.toString().substring(0, tIdx);
            String pageWithRank = value.toString().substring(tIdx + 1);
            
            int rankIdx = pageWithRank.indexOf('\t');
            if (rankIdx == -1) return;

            String pageRankStr = pageWithRank.substring(0, rankIdx);
            String links = pageWithRank.substring(rankIdx + 1);

            double pageRank = Double.parseDouble(pageRankStr);

            // Emit the page structure
            context.write(new Text(page), new Text("!" + links));

            // Calculate contributions to linked pages
            if (!links.isEmpty()) {
                String[] linkArray = links.split(",");
                double contribution = pageRank / linkArray.length;

                for (String link : linkArray) {
                    Text pageKey = new Text(link);
                    Text contributionValue = new Text(String.valueOf(contribution));
                    context.write(pageKey, contributionValue);
                }
            }
        }
    }

    public static class PageRankReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text page, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String links = "";
            double pageRank = 0.0;

            for (Text value : values) {
                String content = value.toString();

                if (content.startsWith("!")) {
                    links = content.substring(1);
                } else {
                    pageRank += Double.parseDouble(content);
                }
            }

            pageRank = (1 - DAMPING) + DAMPING * pageRank;

            context.write(page, new Text(pageRank + "\t" + links));
        }
    }

    public static void main(String[] args) throws Exception {
        
        if (args.length != 4) {
            System.err.println("Usage: PageRank <input path> <output path> <iterations> <nodes>");
            System.exit(-1);
        }

        String inputPath = args[0];
        String outputPath = args[1];
        int iterations = Integer.parseInt(args[2]);
        int nodes = Integer.parseInt(args[3]);

        Configuration conf = new Configuration();
        
        // Initial setup - convert graph to pagerank format
        System.out.println("========================================");
        System.out.println("Initializing PageRank...");
        System.out.println("Input: " + inputPath);
        System.out.println("Output: " + outputPath);
        System.out.println("Iterations: " + iterations);
        System.out.println("Nodes: " + nodes);
        System.out.println("========================================");

        String lastOutput = inputPath;

        // Run PageRank iterations
        for (int i = 0; i < iterations; i++) {
            System.out.println("Running iteration " + (i + 1) + " of " + iterations);
            
            Job job = Job.getInstance(conf, "pagerank-iter-" + i);
            job.setJarByClass(PageRank.class);
            job.setMapperClass(PageRankMapper.class);
            job.setReducerClass(PageRankReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(lastOutput));
            
            String currentOutput = outputPath + "/iter" + nf.format(i);
            FileOutputFormat.setOutputPath(job, new Path(currentOutput));

            // Delete output directory if it exists
            FileSystem fs = FileSystem.get(conf);
            if (fs.exists(new Path(currentOutput))) {
                fs.delete(new Path(currentOutput), true);
            }

            if (!job.waitForCompletion(true)) {
                System.err.println("PageRank iteration " + i + " failed");
                System.exit(1);
            }

            lastOutput = currentOutput;
        }

        // Create final output
        FileSystem fs = FileSystem.get(conf);
        String finalOutput = outputPath + "/final";
        if (fs.exists(new Path(finalOutput))) {
            fs.delete(new Path(finalOutput), true);
        }
        
        // Copy last iteration to final output
        org.apache.hadoop.fs.FileUtil.copy(fs, new Path(lastOutput), fs, new Path(finalOutput), false, conf);

        System.out.println("========================================");
        System.out.println("PageRank completed successfully!");
        System.out.println("Final results available at: " + finalOutput);
        System.out.println("========================================");
    }
} 