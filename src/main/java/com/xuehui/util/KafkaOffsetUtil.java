package com.xuehui.util;

/**
 * <p>
 * ----------------------------------------------------------------------------- <br>
 * 工程名 ：springboot-kafka <br>
 * 功能：<br>
 * 描述：<br>
 * 授权 : (C) Copyright (c) 2016<br>
 * 公司 : 北京博创联动科技有限公司<br>
 * ----------------------------------------------------------------------------- <br>
 * 修改历史<br>
 * <table width="432" border="1">
 * <tr><td>版本</td><td>时间</td><td>作者</td><td>改变</td></tr>
 * <tr><td>1.0</td><td>2019/4/17</td><td>xuehui</td><td>创建</td></tr>
 * </table>
 * <br>
 * <font color="#FF0000">注意: 本内容仅限于[北京博创联动科技有限公司]内部使用，禁止转发</font><br>
 *
 * @author xuehui
 * @version 1.0
 * @since JDK1.8
 */
public class KafkaOffsetUtil implements java.io.Serializable {

   /* public java.util.Map<String, String> kafkaParams = new java.util.HashMap<>();//kafka配置参数
    public java.util.Set topics = null;//topics
    private transient org.apache.spark.streaming.kafka.KafkaCluster kafkaCluster = null;

    *//**
     * 初始化
     *
     * @param bootstrapServers
     * @param groupId
     * @param topics
     *//*
    public KafkaOffsetUtil(String bootstrapServers, String groupId, java.util.List<String> topics) {
        this.kafkaParams.put("bootstrap.servers", bootstrapServers);
        this.kafkaParams.put("group.id", groupId);

        this.topics = new java.util.HashSet(topics);

        scala.collection.immutable.Map<String, String> scalaKafkaParamsMap = ScalaUtil.toScalaImmutableMap(kafkaParams);
        kafkaCluster = new org.apache.spark.streaming.kafka.KafkaCluster(scalaKafkaParamsMap);

        System.out.println(String.format("kafka bootstrap.servers %s", kafkaParams.get("bootstrap.servers")));
        System.out.println(String.format("kafka group.id %s", kafkaParams.get("group.id")));
        System.out.println(String.format("kafka topics %s", topics));
    }


    *//**
     * 获得当前组的offsets
     *
     * @return
     *//*
    public java.util.Map<kafka.common.TopicAndPartition, Long> getConsumerOffsets() {
        java.util.Map<kafka.common.TopicAndPartition, Long> consumerOffsetsLong = new java.util.HashMap<>();//当前用户的offsets
        java.util.Map<kafka.common.TopicAndPartition, Long> consumerOffsetsLongfix = new java.util.HashMap<>();//修复offset range后的offsetss

        scala.collection.immutable.Set<kafka.common.TopicAndPartition> partitions =
                (scala.collection.immutable.Set<kafka.common.TopicAndPartition>) kafkaCluster.getPartitions(ScalaUtil.toScalaImmutableSet(topics)).right().get();
        java.util.Set<kafka.common.TopicAndPartition> partitionsSet = ScalaUtil.toJavaSet(partitions);
        System.out.println(String.format("%s partitions %s", kafkaParams.get("group.id"), partitionsSet));

        if (kafkaCluster.getConsumerOffsets(kafkaParams.get("group.id"), partitions).isLeft()) {//当前用户没有保存offsets
            partitionsSet.forEach(topicAndPartition -> consumerOffsetsLong.put(topicAndPartition, 0L));
        } else {//当前用户保存过offsets
            java.util.Map<kafka.common.TopicAndPartition, Object> consumerOffsets =
                    ScalaUtil.toJavaMap(kafkaCluster.getConsumerOffsets(kafkaParams.get("group.id"), partitions).right().get());

            partitionsSet.forEach(topicAndPartition -> consumerOffsetsLong.put(topicAndPartition, (Long) consumerOffsets.get(topicAndPartition)));
        }

        java.util.Map<kafka.common.TopicAndPartition, Long> earliestLeaderOffsets = getEarliestLeaderOffsets(partitions);
        java.util.Map<kafka.common.TopicAndPartition, Long> latestLeaderOffsets = getLatestLeaderOffsets(partitions);

        consumerOffsetsLong.forEach((topicAndPartition, consumerOffset) -> {
            Long earliestLeaderOffset = earliestLeaderOffsets.get(topicAndPartition);
            Long latestLeaderOffset = latestLeaderOffsets.get(topicAndPartition);
            Long fixOffset = 0L;
            //  earliestLeaderOffset <= consumerOffset <= latestLeaderOffset
            if (consumerOffset < earliestLeaderOffset) {
                System.out.println(String.format("fix consumerOffset to earliestLeaderOffset %s %s -> %s", topicAndPartition, consumerOffset, earliestLeaderOffset));
                fixOffset = earliestLeaderOffset;
            } else if (consumerOffset > latestLeaderOffset) {
                System.out.println(String.format("fix consumerOffset to latestLeaderOffset %s %s -> %s", topicAndPartition, consumerOffset, latestLeaderOffset));
                fixOffset = latestLeaderOffset;
            } else {
                fixOffset = consumerOffset;
            }
            consumerOffsetsLongfix.put(topicAndPartition, fixOffset);
        });

        return consumerOffsetsLongfix;
    }


    *//**
     * 获得最初的offsets
     *
     * @return
     *//*
    public java.util.Map<kafka.common.TopicAndPartition, Long> getEarliestLeaderOffsets() {
        scala.collection.immutable.Set<kafka.common.TopicAndPartition> partitions =
                (scala.collection.immutable.Set<kafka.common.TopicAndPartition>) kafkaCluster.getPartitions(ScalaUtil.toScalaImmutableSet(topics)).right().get();

        return getEarliestLeaderOffsets(partitions);
    }

    private java.util.Map<kafka.common.TopicAndPartition, Long> getEarliestLeaderOffsets(scala.collection.immutable.Set<kafka.common.TopicAndPartition> partitions) {
        java.util.Map<kafka.common.TopicAndPartition, Long> offsetsLong = new java.util.HashMap<>();

        java.util.Set<scala.Tuple2<kafka.common.TopicAndPartition, Object>> leaderOffsets = ScalaUtil.toJavaSet(kafkaCluster.getEarliestLeaderOffsets(partitions).right().get().toSet());

        leaderOffsets.forEach(t -> {
            String leaderOffset = t._2.toString();
            offsetsLong.put(t._1, Long.parseLong(leaderOffset.substring(leaderOffset.lastIndexOf(",") + 1, leaderOffset.length() - 1)));
        });

        return offsetsLong;
    }


    *//**
     * 获得最后的offsets
     *
     * @return
     *//*
    public java.util.Map<kafka.common.TopicAndPartition, Long> getLatestLeaderOffsets() {
        scala.collection.immutable.Set<kafka.common.TopicAndPartition> partitions =
                (scala.collection.immutable.Set<kafka.common.TopicAndPartition>) kafkaCluster.getPartitions(ScalaUtil.toScalaImmutableSet(topics)).right().get();

        return getLatestLeaderOffsets(partitions);
    }

    private java.util.Map<kafka.common.TopicAndPartition, Long> getLatestLeaderOffsets(scala.collection.immutable.Set<kafka.common.TopicAndPartition> partitions) {
        java.util.Map<kafka.common.TopicAndPartition, Long> offsetsLong = new java.util.HashMap<>();

        java.util.Set<scala.Tuple2<kafka.common.TopicAndPartition, Object>> leaderOffsets = ScalaUtil.toJavaSet(kafkaCluster.getLatestLeaderOffsets(partitions).right().get().toSet());

        leaderOffsets.forEach(t -> {
            String leaderOffset = t._2.toString();
            offsetsLong.put(t._1, Long.parseLong(leaderOffset.substring(leaderOffset.lastIndexOf(",") + 1, leaderOffset.length() - 1)));
        });

        return offsetsLong;
    }


    *//**
     * 更新soffset到kafkaCluster
     *
     * @param topic
     * @param partition
     * @param untilOffset
     *//*
    public void updateOffsets(String topic, int partition, long untilOffset) {
        java.util.Map<kafka.common.TopicAndPartition, Object> topicAndPartitionObjectMap = new java.util.HashMap<>();
        topicAndPartitionObjectMap.put(new kafka.common.TopicAndPartition(topic, partition), untilOffset);


        scala.collection.immutable.Map<kafka.common.TopicAndPartition, Object> scalaTopicAndPartitionObjectMap = ScalaUtil.toScalaImmutableMap(topicAndPartitionObjectMap);
        kafkaCluster.setConsumerOffsets(kafkaParams.get("group.id"), scalaTopicAndPartitionObjectMap);
    }


    *//**
     * 显示消费进度
     *//*
    public void progress() {
        java.util.Map<kafka.common.TopicAndPartition, Long> curr = getConsumerOffsets();
        java.util.Map<kafka.common.TopicAndPartition, Long> last = getLatestLeaderOffsets();
        final long[] t = {0};
        curr.forEach((topicAndPartition, aLong) -> {
            t[0] += (last.get(topicAndPartition) - aLong);
            System.out.println(topicAndPartition.topic() + "\t" + topicAndPartition.partition() + "\t" + (last.get(topicAndPartition) - aLong));
        });
        System.out.println(t[0]);
    }*/

}
