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
 * 基于物品的协同过滤算法
 */
public class ItemBasedSparkCF {

    /**
     * @param jsc
     * @param data                   format: [(long userid,long itemid,float score),...]
     * @param numRecommendations
     * @param booleanData
     * @param minVisitedPerItem      use to filter new item
     * @param maxPrefsPerUser        use to filter too active user
     * @param minPrefsPerUser        use to filter new user
     * @param maxSimilaritiesPerItem
     * @param maxPrefsPerUserForRec  in final computer user's recommendations stage,
     *                               only use top maxPrefsPerUserForRec number of prefs for each user
     * @param min_similary           min similary between items
     * @return ItemBasedCFSparkJob.CFModel model
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
        //collect legal user and their visited items
        //获取符合条件的用户及他们访问过的物品
        Map<Long, Tuple2<Set<Long>, Set<Long>>> user_visited = data
                .filter(row -> row.getFloat(2) > 0)
                .mapToPair(row -> new Tuple2<>(row.getLong(0), new Tuple2<>(row.getLong(1), row.getFloat(2))))
                //将数据根据userid分组，获取每个用户的商品-评分表
                //即获取用户-物品矩阵
                .aggregateByKey(new ArrayList<Tuple2<Long, Float>>(),
                        (list, t) -> {
                            list.add(t);
                            return list;
                        },
                        (l1, l2) -> {
                            l1.addAll(l2);
                            return l1;
                        })
                //筛选掉新用户和过于活跃的用户，即物品评分过多或过少的用户
                .filter(t -> t._2.size() <= maxPrefsPerUser && t._2.size() >= minPrefsPerUser)
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

        //collect legal items
        //获取物品被打分次数
        List<Long> legal_items = data.mapToPair(row -> new Tuple2<Long, Integer>(row.getLong(1), 1))
                .reduceByKey((a,b) -> a+b)
                //筛选掉新物品，即用户访问次数较少的物品
                .filter(t -> t._2 >= minVisitedPerItem)
                .keys()
                .collect();
        Broadcast<Set<Long>> legal_items_bd = jsc.broadcast(new HashSet<>(legal_items));

        //filter legal user
        JavaRDD<Row> filted_data = data
                .filter(row -> user_visited_bd.getValue().containsKey(row.getLong(0))
                        && legal_items_bd.getValue().contains(row.getLong(1)));
        filted_data.cache();

        //user list that visited the same item
        //获取物品所属用户列表
        JavaPairRDD<Long, Iterable<Tuple2<Long, Float>>> item_user_list = filted_data
                .mapToPair(row -> new Tuple2<>(row.getLong(1),
                        new Tuple2<Long, Float>(row.getLong(0), row.getFloat(2))))
                .groupByKey();
        item_user_list.cache();

        //computer sqrt(|item|)
        //计算物品相似度分母
        Map<Long, Float> item_norm = item_user_list
                .mapValues(iter -> {
                    float score = 0.0f;
                    for (Tuple2<Long, Float> t : iter) {
                        score += t._2 * t._2;
                    }
                    return score;
                })
                .collectAsMap();
        Broadcast<Map<Long, Float>> item_norm_bd = jsc.broadcast(new HashMap<>(item_norm));

        //获取物品topN相似度
        JavaPairRDD<Long, List<Tuple2<Long, Float>>> item_similaries = filted_data
                //group items visited by the same user
                //获取用户访问过的物品列表
                .mapToPair(row -> new Tuple2<>(row.getLong(0), new Tuple2<>(row.getLong(1), row.getFloat(2))))
                .groupByKey()
                //computer item1 * item2
                .flatMapToPair(t -> {
                    List<Tuple2<Long, Float>> items = Lists.newArrayList(t._2);
                    Set<Tuple2<ItemPair, Float>> list_set = new HashSet<>(items.size() * (items.size() - 1) / 2);
                    for (Tuple2<Long, Float> i : items) {
                        for (Tuple2<Long, Float> j : items) {
                            if (i._1.longValue() == j._1.longValue()) continue;
                            list_set.add(new Tuple2<ItemPair, Float>(new ItemPair(i._1, j._1), i._2 * j._2));
                        }
                    }
                    return list_set;
                })
                .reduceByKey((a, b) -> a + b)
                //计算物品间的余弦相似度: (item1 * item2) / (|item1| * |item2|)
                .mapToPair(t -> {
                    ItemPair up = t._1;
                    float norm_a = item_norm_bd.getValue().get(up.a);
                    float norm_b = item_norm_bd.getValue().get(up.b);
                    return new Tuple2<>(up, t._2 / (float) Math.sqrt(norm_a * norm_b));
                })
                .filter(t -> t._2 >= min_similary)
                //expand matrix
                //获取物品相似度矩阵
                .flatMapToPair(t -> {
                    List<Tuple2<Long, Tuple2<Long, Float>>> list = new ArrayList<>(2);
                    list.add(new Tuple2<>(t._1.a, new Tuple2<Long, Float>(t._1.b, t._2)));
                    list.add(new Tuple2<>(t._1.b, new Tuple2<Long, Float>(t._1.a, t._2)));
                    return list;
                })
                //sort and get topN similary items for item
                //排序并获取物品的topN个相似物品
                .aggregateByKey(new MinHeap(maxSimilaritiesPerItem),
                        (heap, t) -> heap.add(t),
                        (h1, h2) -> h1.addAll(h2))
                .mapValues(heap -> {
                    List<Tuple2<Long, Float>> tops = heap.getSortedItems();
                    //相似度归一化
                    float max = tops.get(0)._2;
                    return tops.stream()
                            .map(t -> new Tuple2<>(t._1, t._2/max))
                            .collect(Collectors.toList());
                });
        item_similaries.cache();

        //get user topN recommendtions
        JavaPairRDD<Long, List<Tuple2<Long, Float>>> user_similaries = item_user_list
                .join(item_similaries)
                .flatMapToPair(t -> {
                    Set<Tuple2<Long, Tuple2<Long, Float>>> user_similary_items = new HashSet<>();
                    Iterable<Tuple2<Long, Float>> users = t._2._1;
                    List<Tuple2<Long, Float>> items = t._2._2;
                    for (Tuple2<Long, Float> user : users) {
                        Tuple2<Set<Long>, Set<Long>> visited = user_visited_bd.getValue().get(user._1);
                        Set<Long> visited_ids = visited._1;
                        Set<Long> abandoned_ids = visited._2;
                        //filter items > maxPrefsPerUserForRec
                        if (abandoned_ids != null && abandoned_ids.contains(t._1)) continue;

                        for (Tuple2<Long, Float> item : items) {
                            if (!visited_ids.contains(item._1)) {
                                float score = item._2;
                                if (!booleanData) {
                                    score *= user._2;
                                }
                                user_similary_items.add(new Tuple2<>(user._1,
                                        new Tuple2<Long, Float>(item._1, score)));
                            }
                        }
                    }
                    return user_similary_items;
                })
                //sum all scores
                .aggregateByKey(new HashMap<Long, Float>(),
                        (m, item) -> {
                            Float score = m.get(item._1);
                            if (score == null) {
                                m.put(item._1, item._2);
                            } else {
                                m.put(item._1, item._2 + score);
                            }
                            return m;
                        },
                        (m1, m2) -> {
                            HashMap<Long, Float> m_big;
                            HashMap<Long, Float> m_small;
                            if (m1.size() > m2.size()) {
                                m_big = m1;
                                m_small = m2;
                            } else {
                                m_big = m2;
                                m_small = m1;
                            }
                            for (Map.Entry<Long, Float> e : m_small.entrySet()) {
                                Float v = m_big.get(e.getKey());
                                if (v != null) {
                                    m_big.put(e.getKey(), e.getValue() + v);
                                } else {
                                    m_big.put(e.getKey(), e.getValue());
                                }
                            }
                            return m_big;
                        })
                //sort and get topN similary items for user
                .mapValues(all_items -> {
                    List<Tuple2<Long, Float>> limit_items = all_items.entrySet().stream()
                            .map(e -> new Tuple2<Long, Float>(e.getKey(), e.getValue()))
                            .sorted((a, b) -> b._2.compareTo(a._2))
                            .limit(numRecommendations)
                            .collect(Collectors.toList());
                    return limit_items;
                });

        CFModel model = new CFModel(item_similaries);
        return model;
    }

    public static void main(String[] args) throws Exception {
        SparkConf sparkConf = new SparkConf().setMaster("local[2]");
        sparkConf.setAppName("ItemBasedCFSparkJob");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        //input userid itemid score
        JavaRDD<Row> data = jsc.textFile("./cf_data.txt")
                .map(line -> {
                    String[] info = line.split(" ");
                    return RowFactory.create(Long.parseLong(info[0]),
                            Long.parseLong(info[1]),
                            Float.parseFloat(info[2]));
                });


//        data.foreach(row -> System.out.println(row.getLong(0)+" "+row.getLong(1)+" "+row.getFloat(2)));
        ItemBasedSparkCF job = new ItemBasedSparkCF();
        int numRecommendations = 100;
        boolean booleanData = false;
        int minVisitedPerItem = 1;
        int maxPrefsPerUser = 500;
        int minPrefsPerUser = 2;
        int maxSimilaritiesPerItem = 30;
        int maxPrefsPerUserForRec = 30;
        float min_similary = 0.4f;
        CFModel model = job.run(jsc,
                data,
                numRecommendations,
                booleanData,
                minVisitedPerItem,
                maxPrefsPerUser,
                minPrefsPerUser,
                maxSimilaritiesPerItem,
                maxPrefsPerUserForRec,
                min_similary);

        Map<Long, List<Tuple2<Long, Float>>> item_similaries_map = model.getSimilaries().collectAsMap();
        System.out.println("=============== item_similaries_map ==============\n");
        for (Map.Entry e : item_similaries_map.entrySet()) {
            System.out.println(e.getKey() + "\t" + e.getValue());
        }
        System.out.println("\n================================================");


    }
}
