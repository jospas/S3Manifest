package com.aws.parquet;

import org.apache.commons.cli.*;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.*;
import java.util.Iterator;
import java.util.zip.GZIPInputStream;

/**
 * Converts CSV files to Parquet
 */
public class ParquetConverter
{
    private static final Logger LOGGER = Logger.getLogger(ParquetConverter.class);

    private String inputFile = null;
    private String outputFile = null;

    private boolean isGzipped = false;

    /**
     * 100MB default row group size
     */
    private int rowGroupSize = 1024 * 1024 * 100;

    public static void main(String [] args)
    {
        try
        {
            ParquetConverter converter = new ParquetConverter(args);
            converter.convert();
        }
        catch (Throwable t)
        {
            LOGGER.error("Failed to convert", t);
        }
    }

    public ParquetConverter(String [] args)
    {
        parseCommandLine(args);
    }

    private void convert() throws IOException
    {
        ParquetWriter<S3Object> parquetWriter =
                AvroParquetWriter.<S3Object>builder(new Path(outputFile))
                        .withSchema(S3Object.SCHEMA)
                        .withCompressionCodec(CompressionCodecName.SNAPPY)
                        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                        .withRowGroupSize(rowGroupSize)
                        .build();

        InputStream fileStream = null;
        InputStream gzipStream = null;
        Reader reader = null;

        if (isGzipped)
        {
            fileStream = new FileInputStream(inputFile);
            gzipStream = new GZIPInputStream(fileStream);
            reader = new InputStreamReader(gzipStream);
        }
        else
        {
            reader = new FileReader(new File(inputFile).getAbsolutePath());
        }

        S3Object s3Object = new S3Object();

        Iterator<CSVRecord> iterator = CSVFormat.EXCEL.withFirstRecordAsHeader().parse(reader).iterator();

        long rowCount = 0L;

        while (iterator.hasNext())
        {
            CSVRecord record = iterator.next();
            s3Object.populate(record);
            parquetWriter.write(s3Object);

            rowCount++;

            if (rowCount % 1000000L == 0)
            {
                LOGGER.info(String.format("Processed: %d rows", rowCount));
            }
        }

        reader.close();
        parquetWriter.close();
    }

    /**
     * Parse command line options
     * @param args the command line arguments
     */
    private void parseCommandLine(String [] args)
    {
        final Options options = new Options();

        options.addRequiredOption(null, "input", true, "Required input csv file (may be gzipped)");
        options.addRequiredOption(null, "output", true, "Required output file");
        options.addOption(null, "rowgroupsize", true, "Optional row group size in bytes (default 100MB)");

        CommandLineParser parser = new DefaultParser();

        try
        {
            CommandLine line = parser.parse(options, args);

            inputFile = line.getOptionValue("input");
            outputFile = line.getOptionValue("output");

            isGzipped = inputFile.endsWith(".gz");

            if (line.hasOption("rowgroupsize"))
            {
                rowGroupSize = Integer.parseInt(line.getOptionValue("rowgroupsize"));
            }
        }
        catch (Throwable t)
        {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("java -jar target/S3Manifest-2.0.jar", options);
            System.out.println("Command line parsing failed: " + t.getMessage());
            throw new RuntimeException(t);
        }
    }
}
