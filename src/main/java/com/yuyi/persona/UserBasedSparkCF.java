package com.yuyi.persona;

import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 基于用户的协同过滤算法
 */
public class UserBasedSparkCF {

    /**
     *
     * @param jsc   java spark context
     * @param data  格式: [(long userid,long itemid,float score),...]
     * @param numRecommendations 推荐数量
     * @param booleanData
     * @param minVisitedPerItem 物品最小访问量，用于筛选掉新上架物品
     * @param maxPrefsPerUser 用户最高访问物品数限制，筛选掉过于活跃用户
     * @param minPrefsPerUser 用户最低访问物品数限制，筛选掉过于新用户
     * @param maxSimilaritiesPerItem 相似物品最高数量
     * @param maxPrefsPerUserForRec 用户推荐最大数量
     * @param min_similary 最小相似度
     * @return
     */
    public CFModel run(JavaSparkContext jsc,
                       JavaRDD<Row> data,
                       final int numRecommendations,
                       final boolean booleanData,
                       final int minVisitedPerItem,
                       final int maxPrefsPerUser,
                       final int minPrefsPerUser,
                       final int maxSimilaritiesPerItem,
                       final int maxPrefsPerUserForRec,
                       final float min_similary) {
        //获取符合条件的用户及他们访问过的物品
        Map<Long, Tuple2<Set<Long>,Set<Long>>> user_visited = data
                //删除score为0的项
                .filter(row->row.getFloat(2)>0)
                .mapToPair(row -> new Tuple2<>(row.getLong(0), new Tuple2<>(row.getLong(1), row.getFloat(2))))
                //获取用户-物品评分矩阵
                .aggregateByKey(new ArrayList<Tuple2<Long, Float>>(),
                        (list, t) -> {
                            list.add(t);
                            return list;
                        },
                        (list1, list2) -> {
                            list1.addAll(list2);
                            return list1;
                        })
                //删除过于活跃用户和新用户
                .filter(t->t._2.size()<=maxPrefsPerUser&&t._2.size()>=minPrefsPerUser)
                .mapValues(list -> {
                    if (list.size() > maxPrefsPerUserForRec) {
                        list = (ArrayList<Tuple2<Long, Float>>) list.stream()
                                .sorted((a, b) -> b._2.compareTo(a._2))
                                .collect(Collectors.toList());
                    }
                    //用于计算用户相似度的物品
                    Set<Long> visited_a = new HashSet<>();
                    //不用于计算用户相似度的物品（抛弃）
                    Set<Long> visited_b = list.size() <= maxPrefsPerUserForRec ? null : new HashSet<>();
                    for (int i = 0; i < list.size(); i++) {
                        if (i < maxPrefsPerUserForRec) {
                            visited_a.add(list.get(i)._1);
                        }
                        if (i >= maxPrefsPerUserForRec) {
                            visited_b.add(list.get(i)._1);
                        }
                    }
                    return new Tuple2<Set<Long>, Set<Long>>(visited_a, visited_b);
                })
                .collectAsMap();
        Broadcast<Map<Long, Tuple2<Set<Long>, Set<Long>>>> user_visited_bd = jsc.broadcast(new HashMap<>(user_visited));

        //获取物品被打分次数
        List<Long> legal_items = data.mapToPair(row -> new Tuple2<Long, Integer>(row.getLong(1), 1))
                .reduceByKey((a,b) -> a+b)
                //筛选掉新物品，即用户访问次数较少的物品
                .filter(t -> t._2 >= minVisitedPerItem)
                .keys()
                .collect();
        Broadcast<Set<Long>> legal_items_bd = jsc.broadcast(new HashSet<>(legal_items));

        //筛选出符合筛选条件的打分数据
        JavaRDD<Row> filted_data = data
                .filter(row -> user_visited_bd.getValue().containsKey(row.getLong(0))
                        && legal_items_bd.getValue().contains(row.getLong(1)));
        filted_data.cache();

        //计算用户相似度分母
        Map<Long, Float> user_norm = filted_data
                .mapToPair(row -> new Tuple2<>(row.getLong(0), new Tuple2<>(row.getLong(1), row.getFloat(2))))
                .groupByKey()
                .mapValues(iter -> {
                    float score = 0.0f;
                    for (Tuple2<Long, Float> item : iter) {
                        score += item._2 * item._2;
                    }
                    return score;
                })
                .collectAsMap();
        Broadcast<Map<Long, Float>> user_norm_bd = jsc.broadcast(new HashMap<>(user_norm));


        //获取用户topN相似度
        JavaPairRDD<Long, List<Tuple2<Long,Float>>> user_similary = filted_data
                //获取物品所属用户列表
                .mapToPair(row -> new Tuple2<>(row.getLong(1),
                        new Tuple2<Long, Float>(row.getLong(0), row.getFloat(2))))
                .groupByKey()
                //计算物品-用户倒排表
                .flatMapToPair(t -> {
                    List<Tuple2<Long, Float>> users = Lists.newArrayList(t._2);
                    Set<Tuple2<UserPair, Float>> list_set = new HashSet<>(users.size() * (users.size() - 1) / 2);
                    for (Tuple2<Long, Float> i : users) {
                        for (Tuple2<Long, Float> j : users) {
                            if (i._1.longValue() == j._1.longValue()) continue;
                            list_set.add(new Tuple2<UserPair, Float>(new UserPair(i._1, j._1), i._2 * j._2));
                        }
                    }
                    return list_set;
                })
                .reduceByKey((a, b) -> a + b)
                //计算用户间余弦相似度
                .mapToPair(t -> {
                    UserPair u = t._1;
                    Float norm_a = user_norm_bd.getValue().get(u.a);
                    Float norm_b = user_norm_bd.getValue().get(u.b);
                    return new Tuple2<>(u, t._2 / (float) Math.sqrt(norm_a * norm_b));
                })
                //筛选掉不相似的用户
                .filter(t -> t._2 > min_similary)
                //获取用户相似度矩阵
                .flatMapToPair(t -> {
                    List<Tuple2<Long, Tuple2<Long, Float>>> list = new ArrayList<>(2);
                    list.add(new Tuple2<>(t._1.a, new Tuple2<Long, Float>(t._1.b, t._2)));
                    list.add(new Tuple2<>(t._1.b, new Tuple2<Long, Float>(t._1.a, t._2)));
                    return list;
                })
                //按相似都排序并获取物品的topN个相似物品
                .aggregateByKey(new MinHeap(maxSimilaritiesPerItem),
                        (heap, t) -> heap.add(t),
                        (heap1, heap2) -> heap1.addAll(heap2))
                //相似度归一化
                .mapValues(heap -> {
                    List<Tuple2<Long, Float>> tops = heap.getSortedItems();
                    Float maxSimilary = tops.get(0)._2;
                    return tops.stream()
                            .map(t -> new Tuple2<>(t._1, t._2 / maxSimilary))
                            .collect(Collectors.toList());
                });
        user_similary.cache();
        return new CFModel(user_similary);
    }

    public static void main(String[] args) throws Exception{
        SparkConf sparkConf = new SparkConf().setMaster("local[2]");
        sparkConf.setAppName("ItemBasedCFSparkJob");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        //获取userid itemid score
        JavaRDD<Row> data = jsc.textFile("./cf_data.txt")
                .map(line -> {
                    String[] info = line.split(" ");
                    return RowFactory.create(Long.parseLong(info[0]),
                            Long.parseLong(info[1]),
                            Float.parseFloat(info[2]));
                });

        UserBasedSparkCF job = new UserBasedSparkCF();
        int numRecommendations = 100;
        boolean booleanData = false;
        int minVisitedPerItem = 1;
        int maxPrefsPerUser = 500;
        int minPrefsPerUser = 2;
        int maxSimilaritiesPerItem = 30;
        int maxPrefsPerUserForRec = 30;
        float min_similary = 0.4f;

        CFModel user_similary = job.run(jsc,
                data,
                numRecommendations,
                booleanData,
                minVisitedPerItem,
                maxPrefsPerUser,
                minPrefsPerUser,
                maxSimilaritiesPerItem,
                maxPrefsPerUserForRec,
                min_similary);
        Map<Long, List<Tuple2<Long, Float>>> user_similary_map = user_similary.getSimilaries().collectAsMap();
        System.out.println("print result");
        System.out.println("result length:"+user_similary_map.size());
        for (Map.Entry e : user_similary_map.entrySet()) {
            System.out.println(e.getKey() + "\t" + e.getValue());
        }


    }
}
