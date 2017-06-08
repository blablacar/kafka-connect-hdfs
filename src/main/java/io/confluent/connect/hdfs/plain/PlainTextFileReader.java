package io.confluent.connect.hdfs.plain;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.hdfs.SchemaFileReader;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * This classed should not be concerned.
 */
public class PlainTextFileReader implements SchemaFileReader {

    private AvroData avroData;

    public PlainTextFileReader(AvroData avroData) {
        this.avroData = avroData;
    }

    @Override
    public Schema getSchema(Configuration conf, Path path) throws IOException {
        SeekableInput input = new FsInput(path, conf);
        DatumReader<Object> reader = new GenericDatumReader<>();
        FileReader<Object> fileReader = DataFileReader.openReader(input, reader);
        org.apache.avro.Schema schema = fileReader.getSchema();
        fileReader.close();
        return avroData.toConnectSchema(schema);
    }

    @Override
    public Collection<Object> readData(Configuration conf, Path path) throws IOException {
        ArrayList<Object> collection = new ArrayList<>();
        SeekableInput input = new FsInput(path, conf);
        DatumReader<Object> reader = new GenericDatumReader<>();
        FileReader<Object> fileReader = DataFileReader.openReader(input, reader);
        for (Object object: fileReader) {
            collection.add(object);
        }
        fileReader.close();

        return collection;
    }
}
