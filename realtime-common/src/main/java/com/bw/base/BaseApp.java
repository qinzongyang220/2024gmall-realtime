package com.bw.base;

import com.bw.utils.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

public abstract class BaseApp {
    public abstract void handle(StreamExecutionEnvironment env,DataStreamSource<String> streamSource);

    public void start(int port,int p,String groupId,String topicDb){
        // 设置Hadoop用户
        System.setProperty("HADOOP_USER_NAME","root");
        // 实例化对象，一般用于开辟空间
        Configuration configuration = new Configuration();
        // 设置端口
        configuration.setInteger("rest.port",port);
        // 创建流式环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        //设置并行度
        env.setParallelism(p);
//        设置CK
        env.setStateBackend(new HashMapStateBackend());
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:9000/gmall2023/stream/"+groupId );
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(RETAIN_ON_CANCELLATION);
        //设置ck状态为后端
//        env.setStateBackend(new HashMapStateBackend());
//        //设置ck执行时间间隔            毫秒
//        env.enableCheckpointing(5000);
//        //设置ck模式                                    精准一次
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        //设置ck最小间隔
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
        //设置ck超时时间
//        env.getCheckpointConfig().setCheckpointTimeout(15000);
//        //设置ck并发数
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//        //设置ck路径
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall2023/stream/"+groupId);
//        //job取消时 ck保留策略
//        env.getCheckpointConfig().setExternalizedCheckpointCleanup(RETAIN_ON_CANCELLATION);
        //调用工具类消费数据
        DataStreamSource<String> streamSource = env.fromSource(FlinkSourceUtil.getKafkaSource(topicDb, groupId), WatermarkStrategy.noWatermarks(), "kafka_source");
//        streamSource.print();
        // 将数据调出
        handle(env,streamSource);
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
