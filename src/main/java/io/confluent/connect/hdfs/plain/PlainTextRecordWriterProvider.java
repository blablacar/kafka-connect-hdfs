package io.confluent.connect.hdfs.plain;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.hdfs.RecordWriter;
import io.confluent.connect.hdfs.RecordWriterProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;

public class PlainTextRecordWriterProvider implements RecordWriterProvider {

    private final static String EXTENSION = ".txt";

    private static final Logger log = LoggerFactory.getLogger(PlainTextRecordWriterProvider.class);

    @Override
    public String getExtension() {
        return EXTENSION;
    }

    @Override
    public RecordWriter<SinkRecord> getRecordWriter(
                Configuration conf, String fileName, SinkRecord record, AvroData avroData) throws IOException {

        // HDFS file path
        final Path path = new Path(fileName);
        final OutputStream out = path.getFileSystem(conf).create(path);

        return new RecordWriter<SinkRecord>(){
            @Override
            public void write(SinkRecord record) throws IOException {
                log.trace("Sink record: {}", record.toString());

                if (record.value() == null) {
                    log.warn("Sink record with null value: {}", record.toString());
                    return;
                }
                out.write(record.value().toString().getBytes(Charset.forName("UTF-8")));
                out.write("\n".getBytes());
            }

            @Override
            public void close() throws IOException {
                out.close();
            }
        };
    }
}
