package io.confluent.connect.hdfs.plain;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.hdfs.Format;
import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.RecordWriterProvider;
import io.confluent.connect.hdfs.SchemaFileReader;
import io.confluent.connect.hdfs.hive.HiveMetaStore;
import io.confluent.connect.hdfs.hive.HiveUtil;

public class PlainTextFormat implements Format {
    @Override
    public RecordWriterProvider getRecordWriterProvider() {
        return new PlainTextRecordWriterProvider();
    }

    @Override
    public SchemaFileReader getSchemaFileReader(AvroData avroData) {
        return new PlainTextFileReader(avroData);
    }

    @Override
    public HiveUtil getHiveUtil(HdfsSinkConnectorConfig config, AvroData avroData, HiveMetaStore hiveMetaStore) {
        throw new UnsupportedOperationException("Hive integration is not currently supported with plain text format");
    }
}
