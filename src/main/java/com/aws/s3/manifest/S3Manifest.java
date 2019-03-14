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
import java.nio.file.Files;
import java.nio.file.Paths;
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
        Map<String, String> parameters = parseCommandLine(args);
        Map<String, Long> sizes = new TreeMap<>();
        Map<String, Long> counts = new TreeMap<>();

        sizes.put("STANDARD", 0L);
        sizes.put("REDUCED_REDUNDANCY", 0L);
        sizes.put("GLACIER", 0L);
        sizes.put("STANDARD_IA", 0L);
        sizes.put("ONEZONE_IA", 0L);
        sizes.put("INTELLIGENT_TIERING", 0L);

        counts.put("STANDARD", 0L);
        counts.put("REDUCED_REDUNDANCY", 0L);
        counts.put("GLACIER", 0L);
        counts.put("STANDARD_IA", 0L);
        counts.put("ONEZONE_IA", 0L);
        counts.put("INTELLIGENT_TIERING", 0L);

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
            for (S3ObjectSummary summary: listing.getObjectSummaries())
            {
                csvPrinter.printRecord(FilenameUtils.getPath(summary.getKey()),
                        FilenameUtils.getName(summary.getKey()),
                        summary.getStorageClass(),
                        summary.getSize());

                sizes.put(summary.getStorageClass(), sizes.get(summary.getStorageClass()) + summary.getSize());
                counts.put(summary.getStorageClass(), counts.get(summary.getStorageClass()) + 1);
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

        for (String storageClass: sizes.keySet())
        {
            System.out.println(String.format("%s count: %d size: %s",
                    storageClass,
                    counts.get(storageClass),
                    humanReadableByteCount(sizes.get(storageClass))));
        }

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

        options.addRequiredOption("b", "bucket", true, "Required S3 bucket name to enumerate");
        options.addOption("p", "prefix", true, "Optional S3 prefix to enumerate");
        options.addRequiredOption("o", "output", true, "Output CSV file");
        options.addOption("z", "profile", true, "Optional AWS profile name to use");
        options.addOption("r", "region", true, "AWS region [ap-southeast-2]");

        CommandLineParser parser = new DefaultParser();
        try
        {
            CommandLine line = parser.parse(options, args);

            parameters.put("bucket", line.getOptionValue("bucket"));

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

            if (line.hasOption("region"))
            {
                parameters.put("region", line.getOptionValue("region"));
            }
        }
        catch (ParseException e)
        {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("S3Lister", options);
            System.out.println("Command line parsing failed: " + e.getMessage());
            throw new RuntimeException(e);
        }

        return parameters;
    }
}
