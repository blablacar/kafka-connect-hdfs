package io.confluent.connect.hdfs.json;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.connect.avro.AvroData;
import io.confluent.connect.hdfs.RecordWriter;
import io.confluent.connect.hdfs.RecordWriterProvider;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;

public class JsonRecordWriterProvider implements RecordWriterProvider {

    private final static String EXTENSION = ".json";

    private static final String LINE_SEPARATOR = System.lineSeparator();

    private static final byte[] LINE_SEPARATOR_BYTES = LINE_SEPARATOR.getBytes();

    private static final Logger log = LoggerFactory.getLogger(JsonRecordWriterProvider.class);

    private final JsonConverter converter;
    private final ObjectMapper mapper;

    public JsonRecordWriterProvider() {
        // FIXME: perf ? one single instance ?
        this.converter = new JsonConverter();
        this.mapper = new ObjectMapper();
    }

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
//        final JsonGenerator writer = mapper.getFactory()
//                .createGenerator(out)
//                .setRootValueSeparator(null);

        return new RecordWriter<SinkRecord>(){
            @Override
            public void write(SinkRecord record) throws IOException {
                log.trace("Sink record: {}", record.toString());

                Object value = record.value();
//                if (value instanceof Struct) {
//                    byte[] rawJson = converter.fromConnectData(record.topic(), record.valueSchema(), value);
//                    out.write(rawJson);
//                    out.write(LINE_SEPARATOR_BYTES);
//                } else {
//                    writer.write(value.toString().getBytes());
//                    writer.writeRaw(LINE_SEPARATOR);
//                }

                out.write(record.value().toString().getBytes());
                out.write("\n".getBytes());
            }

            @Override
            public void close() throws IOException {
                out.close();
            }
        };
    }
}
