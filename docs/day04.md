数据倾斜
---
在大数据中在数据量庞大的情况会让我们在运行sql语句时等待时间过长，而优化sql数据成了重中之重，我们在yarn的运行工作中发现增加map与reduce的数量
可以减少 我们的代码的运行时长，除了这种方法外，还有以下:

1.数据预处理‌：在数据进入分布式处理框架之前，通过预处理减少数据倾斜的可能性。例如，过滤掉不必要的key值，或者对数据进行采样和分拆处理‌12。
2.‌过滤少数导致倾斜的key‌：在处理过程中，识别并过滤掉那些导致数据倾斜的少数key值。这样可以减少某些节点处理的数据量，从而平衡负载‌23。
3.‌调整shuffle操作的并行度‌：增加shuffle操作的并行度可以分散处理任务，减少单个节点处理过多数据的可能性。可以通过调整Reduce的个数或修改numReduceTasks值来实现‌14。
4.‌使用Map Join代替Reduce Join‌：当处理大表join小表时，可以将小表加载到内存中，在Map阶段完成join操作，避免Reduce阶段的数据倾斜‌3。
5.‌两阶段聚合‌：对于聚合类操作，可以采用两阶段聚合的方法，先进行局部聚合，再进行全局聚合，这样可以减少单个节点处理的数据量‌23。
6.‌使用随机前缀和自定义partitioner‌：在Map阶段给key添加随机前缀或使用自定义partitioner来重新分配key，使得数据均匀分布到各个节点上‌14。
7.‌增加JVM内存‌：对于数据key非常少的情况，增加JVM内存可以显著提高运行效率‌4。
‌调整参数‌：在Hive中设置
使用自定义partitioner‌：通过自定义partitioner来控制数据的分配，避免某些key被集中分配到同一个节点上‌1。


在spark3.0往后的版本会自动处理数据倾斜：
1.‌动态合并shuffle分区‌：在shuffle过后，如果某些reduce task的数据分布不均匀，AQE(工程师在Spark社区版本的基础上改进并实现的自适应执行引擎)会自动合并过小的数据分区，从而平衡负载‌1。
2.‌调整join策略‌：当参与join的表数据量小于广播阈值时，AQE会将join策略调整为Broadcast Hash Join，这种策略将小表广播到每个节点，避免大量的数据shuffle，从而提升效率‌1。
3.‌自动优化数据倾斜‌：AQE会根据实际情况自动拆分过大的数据分区，进一步缓解数据倾斜问题‌1。
---


以上是数据倾斜的解决方法，在老师的提问后我明白自己的基础不太稳固，需要不断的去牢记，不能丢弃，

广播流原理：
广播流是Flink中的一种特殊的流处理方式，其核心是讲某个数据流的数据以广播的形式发送到其他流中，
就是吧数据发出去，吧他们当成一个只可读的共享数据，
实现方法，
拆分广播流：
复制子任务
——-------——————————————————————--------——————————————————


在推进dim层进度时实现HBASE中自动建表时我的服务器的hbase在集群启动时出现无$JavaHONE这种报错：
[root@hadoop102 hbase]# bin/start-hbase.sh
running master, logging to /opt/module/hbase/logs/hbase-root-master-hadoop102.out
hadoop102: +======================================================================+
hadoop102: |                    Error: JAVA_HOME is not set                       |
hadoop102: +----------------------------------------------------------------------+
hadoop102: | Please download the latest Sun JDK from the Sun Java web site        |
hadoop102: |     > http://www.oracle.com/technetwork/java/javase/downloads        |
hadoop102: |                                                                      |
hadoop102: | HBase requires Java 1.8 or later.                                    |
hadoop102: +======================================================================+
从这个报错看起来很不正常，但是我们只需要看一句话就是 ：Error: JAVA_HOME is not set       
通过这个就可以得到我们需要去给他配置 $JAVA_HOME 
果然在HBase的conf 文件中修改hbase-env.sh 
# The java implementation to use.  Java 1.8+ required.
export JAVA_HOME=/opt/module/jdk1.8.0_212/

=----=
在处理完报错后我继续完成dim层的搭建，发现在执行项目时，没有报错，我的hbase也有表，但是表中没有数据，于是开始排查，
首先是看的hbase读取mysql的配置的地放，结果，确实是，在我的mysql中的表名字是叫gmall2024-realtime ,但是我的配置里写的是gmall,
这是一个小错误，但是我们经常因为一些小错误导致我们的数据出现问题
