package io.confluent.connect.hdfs.json;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.hive.HiveMetaStore;
import io.confluent.connect.hdfs.hive.HiveUtil;
import io.confluent.connect.hdfs.partitioner.Partitioner;
import org.apache.kafka.connect.data.Schema;

public class JsonHiveUtil extends HiveUtil {
    public JsonHiveUtil(HdfsSinkConnectorConfig connectorConfig, AvroData avroData, HiveMetaStore hiveMetaStore) {
        super(connectorConfig, avroData, hiveMetaStore);
    }

    @Override
    public void createTable(String database, String tableName, Schema schema, Partitioner partitioner) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterSchema(String database, String tableName, Schema schema) {
        throw new UnsupportedOperationException();
    }
}
