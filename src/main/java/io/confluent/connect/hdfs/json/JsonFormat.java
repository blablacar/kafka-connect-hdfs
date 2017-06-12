package io.confluent.connect.hdfs.json;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.hdfs.Format;
import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.RecordWriterProvider;
import io.confluent.connect.hdfs.SchemaFileReader;
import io.confluent.connect.hdfs.hive.HiveMetaStore;
import io.confluent.connect.hdfs.hive.HiveUtil;

public class JsonFormat implements Format {
    @Override
    public RecordWriterProvider getRecordWriterProvider() {
        return new JsonRecordWriterProvider();
    }

    @Override
    public SchemaFileReader getSchemaFileReader(AvroData avroData) {
        return new JsonFileReader(avroData);
    }

    @Override
    public HiveUtil getHiveUtil(HdfsSinkConnectorConfig config, AvroData avroData, HiveMetaStore hiveMetaStore) {
        return new JsonHiveUtil(config, avroData, hiveMetaStore);
    }
}
