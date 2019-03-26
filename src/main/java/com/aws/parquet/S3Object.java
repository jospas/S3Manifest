package com.aws.parquet;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.commons.csv.CSVRecord;

public class S3Object extends SpecificRecordBase implements SpecificRecord
{
    private String path = null;
    private String name = null;
    private String storageClass = null;
    private long size = 0L;

    public static final Schema SCHEMA = makeSchema();

    public S3Object(String path, String name, String storageClass, long size)
    {
        this.path = path;
        this.name = name;
        this.storageClass = storageClass;
        this.size = size;
    }

    public S3Object()
    {

    }

    public S3Object populate(CSVRecord record)
    {
        this.path = record.get(0);
        this.name = record.get(1);
        this.storageClass = record.get(2);
        this.size = Long.parseLong(record.get(3));
        return this;
    }

    private static Schema makeSchema()
    {
        Schema schema = SchemaBuilder
                .record("S3Object").namespace("com.aws.parquet")
                .fields()
                .name("path").type().stringType().noDefault()
                .name("name").type().stringType().noDefault()
                .name("storageClass").type().stringType().noDefault()
                .name("size").type().longType().longDefault(0L)
                .endRecord();

        return schema;
    }

    @Override
    public Schema getSchema()
    {
        return SCHEMA;
    }


    @Override
    public Object get(int field)
    {
        switch (field)
        {
            case 0:
            {
                return path;
            }
            case 1:
            {
                return name;
            }
            case 2:
            {
                return storageClass;
            }
            case 3:
            {
                return size;
            }
            default:
            {
                throw new IllegalArgumentException("Invalid field index: " + field);
            }
        }
    }

    @Override
    public void put(int field, Object value)
    {
        switch (field)
        {
            case 0:
            {
                path = (String) value;
                return;
            }
            case 1:
            {
                name = (String) value;
                return;
            }
            case 2:
            {
                storageClass = (String) value;
                return;
            }
            case 3:
            {
                size = (Long) value;
                return;
            }

            default:
            {
                throw new IllegalArgumentException("Invalid field index: " + field);
            }
        }
    }

    public String getPath()
    {
        return path;
    }

    public void setPath(String path)
    {
        this.path = path;
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public String getStorageClass()
    {
        return storageClass;
    }

    public void setStorageClass(String storageClass)
    {
        this.storageClass = storageClass;
    }

    public long getSize()
    {
        return size;
    }

    public void setSize(long size)
    {
        this.size = size;
    }
}
