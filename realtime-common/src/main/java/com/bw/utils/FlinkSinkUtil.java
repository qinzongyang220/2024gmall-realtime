package com.bw.utils;

import com.alibaba.fastjson.JSONObject;
import com.bw.bean.TableProcessDwd;
import com.bw.common.Constant;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.SimpleStringSerializer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;

public class FlinkSinkUtil {

    public static KafkaSink<String> getKafkaSink(String topicName){
       return  KafkaSink.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topicName)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
               .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
               .setTransactionalIdPrefix("bw-" + topicName + System.currentTimeMillis())
               .setProperty("transaction.timeout.ms", 15 * 60 * 1000 + "")
               .build();

    }

    public static KafkaSink<Tuple2<JSONObject, TableProcessDwd>> getDwdKafkaSink(){
        return  KafkaSink.<Tuple2<JSONObject, TableProcessDwd>>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(new KafkaRecordSerializationSchema<Tuple2<JSONObject, TableProcessDwd>>() {
                    @Nullable
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(Tuple2<JSONObject, TableProcessDwd> jsonObjectTableProcessDwdTuple2, KafkaSinkContext kafkaSinkContext, Long aLong) {
                        JSONObject f0 = jsonObjectTableProcessDwdTuple2.f0;
                        TableProcessDwd f1 = jsonObjectTableProcessDwdTuple2.f1;
                        String sinkTable = f1.getSinkTable();
                        JSONObject data = f0.getJSONObject("data");
                        return new ProducerRecord<>(sinkTable, Bytes.toBytes(data.toJSONString()));
                    }
                })
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("bw-base-db"  + System.currentTimeMillis())

                // 关注一下
                .setProperty("transaction.timeout.ms", 15 * 60 * 1000 + "")
                .build();

    }

    /**
     * 获取doris Sink
     * @param tableName
     * @return
     */
    public static DorisSink<String> getDorisSink(String tableName){
        Properties properties = new Properties();
        // 上游是 json 写入时，需要开启配置
        properties.setProperty("format", "json");
        properties.setProperty("read_json_by_line", "true");
        return DorisSink.<String>builder()
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions( DorisExecutionOptions.builder()
                        .setLabelPrefix("label-doris-"+System.currentTimeMillis()) //streamload label prefix
                        .setDeletable(false)
                        .setStreamLoadProp(properties).build())
                .setSerializer(new SimpleStringSerializer()) //serialize according to string
                .setDorisOptions(DorisOptions.builder()
                        .setFenodes(Constant.FENODES)
                        .setTableIdentifier(Constant.DORIS_DATABASE+"."+tableName)
                        .setUsername(Constant.DORIS_USERNAME)
                        .setPassword(Constant.DORIS_PASSWORD).build())
                .build();
    }

}
