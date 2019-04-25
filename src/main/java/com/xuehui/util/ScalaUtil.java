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
public class ScalaUtil implements java.io.Serializable {

   /* *//**
     * 将java.util.Map类型转换为scala.collection.immutable.Map类型
     *
     * @param map
     * @param <K>
     * @param <V>
     * @return
     *//*
    public static <K, V> scala.collection.immutable.Map<K, V> toScalaImmutableMap(java.util.Map<K, V> map) {
        java.util.List<scala.Tuple2<K, V>> tuple2List = map.entrySet().stream().map(entry -> new scala.Tuple2<>(entry.getKey(), entry.getValue())).collect(java.util.stream.Collectors.toList());

        scala.collection.Seq<scala.Tuple2<K, V>> scalaSeq = scala.collection.JavaConverters.asScalaIterableConverter(tuple2List).asScala().toSeq();

        return (scala.collection.immutable.Map<K, V>) scala.collection.immutable.Map$.MODULE$.apply(scalaSeq);
    }


    *//**
     * 将java.util.Set类型转换为scala.collection.immutable.Set类型
     *
     * @param set
     * @param <V>
     * @return
     *//*

    public static <V> scala.collection.immutable.Set<V> toScalaImmutableSet(java.util.Set<V> set) {
        return scala.collection.JavaConverters.asScalaSetConverter(set).asScala().toSet();
    }


    *//**
     * 将scala.collection.immutable.Map类型转换为java.util.Map类型
     *
     * @param map
     * @param <K>
     * @param <V>
     * @return
     *//*
    public static <K, V> java.util.Map<K, V> toJavaMap(scala.collection.immutable.Map<K, V> map) {
        return scala.collection.JavaConversions.mapAsJavaMap(map);
    }

    *//**
     * 将scala.collection.immutable.Set类型转换为java.util.Set类型
     *
     * @param set
     * @param <V>
     * @return
     *//*
    public static <V> java.util.Set<V> toJavaSet(scala.collection.immutable.Set<V> set) {
        return scala.collection.JavaConversions.setAsJavaSet(set);
    }
*/
}
