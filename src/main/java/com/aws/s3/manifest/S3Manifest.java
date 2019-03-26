package com.aws.s3.manifest;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.commons.cli.*;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.QuoteMode;
import org.apache.commons.io.FilenameUtils;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.zip.GZIPOutputStream;

public class S3Manifest
{
    /**
     * Recursive listing of S3 bucket
     * @param args the command line arguments
     * @throws IOException thrown on failure
     */
    public static void main(String [] args) throws IOException
    {
        try
        {
            Map<String, String> parameters = parseCommandLine(args);
            Map<String, Long> sizes = new TreeMap<>();
            Map<String, Long> counts = new TreeMap<>();

            FileOutputStream fileOut = new FileOutputStream(parameters.get("output"));
            GZIPOutputStream zipOut = new GZIPOutputStream(fileOut);
            OutputStreamWriter bufferedWriter = new OutputStreamWriter(zipOut);

            CSVPrinter csvPrinter = new CSVPrinter(bufferedWriter,
                    CSVFormat.EXCEL.withHeader("Path", "Name", "StorageClass", "Size").withQuoteMode(QuoteMode.NON_NUMERIC));

            AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();

            if (parameters.containsKey("profile"))
            {
                System.out.println("Using named profile: " + parameters.get("profile"));
                builder.withCredentials(new ProfileCredentialsProvider(parameters.get("profile")));
            }

            if (parameters.containsKey("region"))
            {
                System.out.println("Using region: " + parameters.get("region"));
                builder.withRegion(parameters.get("region"));
            }

            AmazonS3 s3 = builder.build();

            ListObjectsRequest request = new ListObjectsRequest().withBucketName(parameters.get("bucket")).withMaxKeys(1000);

            if (parameters.containsKey("prefix"))
            {
                System.out.println("Using S3 prefix: " + parameters.get("prefix"));
                request.withPrefix(parameters.get("prefix"));
            }

            ObjectListing listing = s3.listObjects(request);

            long objectCount = 0L;

            while (true)
            {
                for (S3ObjectSummary summary : listing.getObjectSummaries())
                {
                    csvPrinter.printRecord(FilenameUtils.getPath(summary.getKey()),
                            FilenameUtils.getName(summary.getKey()),
                            summary.getStorageClass(),
                            summary.getSize());

                    trackObject(sizes, counts, summary.getStorageClass(), summary.getSize());

                    objectCount++;

                    if (objectCount % 100000L == 0L)
                    {
                        System.out.println(String.format("Listed: %d objects...", objectCount));
                    }
                }

                if (!listing.isTruncated())
                {
                    break;
                }

                request.withMarker(listing.getNextMarker());

                listing = s3.listObjects(request);
            }

            csvPrinter.flush();
            bufferedWriter.close();
            zipOut.close();
            fileOut.close();

            System.out.println(String.format("Found a total of: %d objects", objectCount));

            for (String storageClass : sizes.keySet())
            {
                System.out.println(String.format("%s count: %d size: %s",
                        storageClass,
                        counts.get(storageClass),
                        humanReadableByteCount(sizes.get(storageClass))));
            }
        }
        catch (Throwable t)
        {
            System.out.println("Failed to generate manifest: " + t.getMessage());
            System.exit(1);
        }
    }

    /**
     * Track this object
     * @param sizes the map of sizes
     * @param counts the map of counts
     * @param storageClass the storage class for the object
     * @param size the size in bytes
     */
    private static void trackObject(Map<String, Long> sizes, Map<String, Long> counts, String storageClass, long size)
    {
        if (!sizes.containsKey(storageClass))
        {
            sizes.put(storageClass, 0L);
        }

        if (!counts.containsKey(storageClass))
        {
            counts.put(storageClass, 0L);
        }

        long currentSize = sizes.get(storageClass);
        long currentCount = counts.get(storageClass);

        sizes.put(storageClass, currentSize + size);
        counts.put(storageClass, currentCount + 1);
    }

    /**
     * Create a human readable binary byte measurement
     * @param bytes the byte amount
     * @return a formatted string for the requested bytes
     */
    public static String humanReadableByteCount(long bytes)
    {
        int unit = 1024;
        if (bytes < unit) return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(unit));
        char pre = "KMGTPE".charAt(exp-1);
        return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
    }

    /**
     * Parse command line options
     * @param args the command line arguments
     * @return a map of parameters
     */
    private static Map<String, String> parseCommandLine(String [] args)
    {
        Map<String, String> parameters = new HashMap<>();

        final Options options = new Options();

        options.addRequiredOption(null, "bucket", true, "Required S3 bucket name to enumerate");
        options.addRequiredOption(null, "region", true, "Required AWS region for the bucket");
        options.addRequiredOption(null, "output", true, "Required output CSV file (.gz is appended)");
        options.addOption(null, "prefix", true, "Optional S3 prefix to list under");
        options.addOption(null, "profile", true, "Optional AWS profile name to use");

        CommandLineParser parser = new DefaultParser();

        try
        {
            CommandLine line = parser.parse(options, args);

            parameters.put("bucket", line.getOptionValue("bucket"));
            parameters.put("region", line.getOptionValue("region"));

            if (line.getOptionValue("output").endsWith(".gz"))
            {
                parameters.put("output", line.getOptionValue("output"));
            }
            else
            {
                parameters.put("output", line.getOptionValue("output") + ".gz");
            }

            if (line.hasOption("prefix"))
            {
                parameters.put("prefix", line.getOptionValue("prefix"));
            }

            if (line.hasOption("profile"))
            {
                parameters.put("profile", line.getOptionValue("profile"));
            }
        }
        catch (ParseException e)
        {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("java -jar target/S3Manifest-2.0.jar", options);
            System.out.println("Command line parsing failed: " + e.getMessage());
            throw new RuntimeException(e);
        }

        return parameters;
    }
}
