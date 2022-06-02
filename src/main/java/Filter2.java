/**
 * @author Xu Zheng
 * @description
 */

import org.apache.commons.codec.binary.Base64;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.*;
import org.codehaus.janino.Java;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
//import org.apache.commons.text.StringEscapeUtils;
import org.apache.commons.lang.StringEscapeUtils;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

//score 表
//user_id_str, interaction_score, hashtag_score
//2418950218, {"31251":2.0}, {"341":1.0}
public class Filter2 implements Serializable {
    //    private static String INPUT_FILE = "sample.txt";
//    private static String INPUT_FILE = "s3a://cmucc-datasets/twitter/s22/part-r-00000.gz";
    private static String INPUT_FILE = "wasbs://m3blob@blob15619.blob.core.windows.net/output/part-00000-9b083596-17aa-4c44-8ff3-d1df1ccb717b-c000.csv";
    private static String INPUT_FILE_USER = "wasbs://m3blob@blob15619.blob.core.windows.net/output/part-00000-ef14bcdd-c169-46fe-95a6-54743c0844eb-c000.csv";
    private static String INPUT_FILE_POPULAR = "wasbs://m3blob@blob15619.blob.core.windows.net/output/p_hashtags.csv";


    private static final Base64 base64 = new Base64();
    private static transient SQLContext sqlContext = null;
    private static transient JavaSparkContext sc = null;

    public static void main(String[] args) throws IOException, AnalysisException {

        Filter2 job = new Filter2();
        long startTime = System.currentTimeMillis();
        job.startJob();
        long stopTime = System.currentTimeMillis();
        long elapsedTime = stopTime - startTime;
        System.out.println("Execution time in s : " + elapsedTime / 1000);
    }

    //spark job
    private void startJob() throws AnalysisException {
        //configuration
        SparkConf sparkConf = new SparkConf().setAppName("Spark RDD Filter")
                .setMaster("yarn-client")
//                .setMaster("local[4]")
                .set("spark.executor.memory", "4g")
                .set("spark.driver.memory", "1g")
                .set("spark.sql.shuffle.partitions", "300")
                .set("fs.azure.account.key.blob15619.blob.core.windows.net",
                        "QN/yxQ+d+aRLGDze3wAZEQ6x2TtaokJ7OIKHuiMWEKOV/ynSiR1lGcVcBkw2P516j72pQT4tTqjL+ASth4rT2g==");

        sc = new JavaSparkContext(sparkConf);

        sqlContext = new SQLContext(sc);
//        SQLContext sqlContext = new SQLContext(sc);

        //exclude set
        JavaRDD<String> inputFilePopular = sc.textFile(INPUT_FILE_POPULAR);
        List<String> list = inputFilePopular.collect();
        Set<String> excludeSet = new HashSet<>(list);

        //convert input file to rdd
        JavaRDD<String> inputFile = sc.textFile(INPUT_FILE);
        JavaRDD<Tweet> rddTweet = inputFile.map(x -> filter(x));
        Function<Tweet, Boolean> filter1 = k -> (k != null);
        JavaRDD<Tweet> rddTweet2 = rddTweet.filter(filter1);
        //convert rdd to dataset
        Dataset<Tweet> dsTweet = sqlContext.createDataset(rddTweet2.rdd(), Encoders.bean(Tweet.class));
        //create a view of the dataset
        dsTweet.createOrReplaceTempView("tweet");

        //convert input file to rdd
        JavaRDD<String> inputFileUser = sc.textFile(INPUT_FILE_USER);
        JavaRDD<Tweet> rddUser = inputFileUser.map(x -> filterUser(x));
        JavaRDD<Tweet> rddUser2 = rddUser.filter(filter1);

        List<Score> scoreList = new ArrayList<>();
        List<Tweet> tweetList = rddUser2.collect();
        for(Tweet tweet : tweetList){
            long user_id_str = tweet.getUser_id_str();
            String sql = "SELECT user_id_str, in_reply_to_user_id_str, retweeted_user_id_str FROM tweet WHERE user_id_str = " + user_id_str + " AND (in_reply_to_user_id_str != 0 OR retweeted_user_id_str != 0) UNION ALL SELECT user_id_str, in_reply_to_user_id_str, retweeted_user_id_str FROM tweet WHERE in_reply_to_user_id_str = " + user_id_str + " AND user_id_str != " + user_id_str + " UNION ALL SELECT user_id_str,in_reply_to_user_id_str, retweeted_user_id_str FROM tweet WHERE retweeted_user_id_str = " + user_id_str + " AND user_id_str != " + user_id_str;
            String queryHashtag1 = "SELECT hashtags FROM tweet WHERE user_id_str = " + user_id_str;
            if(sqlContext == null){
                sqlContext = new SQLContext(sc);
            }
            Dataset<Row> ds = sqlContext.sql(sql);
            Map<Long, Interaction> interactionMap = new HashMap<>();

            ds.foreach((ForeachFunction<Row>) row -> {
                long contact = row.getLong(0);
                long replyUserId = row.getLong(1);
                long retweetUserId = row.getLong(2);

                if (contact == user_id_str) {
                    if (replyUserId != 0) {
                        Interaction interaction = interactionMap.getOrDefault(replyUserId, new Interaction());
                        interaction.setReply();
                        if (!interactionMap.containsKey(replyUserId)) {
                            interactionMap.put(replyUserId, interaction);
                        }
                    } else if (retweetUserId != 0) {
                        Interaction interaction = interactionMap.getOrDefault(retweetUserId, new Interaction());
                        interaction.setRetweet();
                        if (!interactionMap.containsKey(retweetUserId)) {
                            interactionMap.put(retweetUserId, interaction);
                        }
                    }
                } else {
                    Interaction interaction = interactionMap.getOrDefault(contact, new Interaction());
                    if (replyUserId == user_id_str) {
                        interaction.setReply();
                    } else if (retweetUserId == user_id_str) {
                        interaction.setRetweet();
                    }
                    if (!interactionMap.containsKey(contact)) {
                        interactionMap.put(contact, interaction);
                    }
                }
            });

            Map<Long, Double> interactionScores = new HashMap<>();
            for (Map.Entry<Long, Interaction> entry : interactionMap.entrySet()) {
                double score = log(1 + 2 * entry.getValue().getReply() + entry.getValue().getRetweet());
                interactionScores.put(entry.getKey(), score);
            }
            Set<Long> contactUsers = interactionScores.keySet();
            if(contactUsers.size()==0){
                scoreList.add(new Score(user_id_str,null));
                continue;
            }
            //hashtag score
            Dataset<Row> ds1 = sqlContext.sql(queryHashtag1);
//            List<Row> arrayList1 = new ArrayList<>();
//            arrayList1 = ds1.collectAsList();
            Map<String, Integer> MyHashTagCount = new HashMap<>();
            ds1.foreach((ForeachFunction<Row>) row -> {
                String hashtags = row.getString(0);
                String[] hashtagArr = hashtags.split(",");
                for (String hashtag : hashtagArr) {
                    hashtag = StringEscapeUtils.unescapeJava(new String(base64.decode(hashtag.getBytes(StandardCharsets.UTF_8)))).toLowerCase(Locale.ENGLISH);
                    if (!excludeSet.contains(hashtag)) {
                        MyHashTagCount.put(hashtag, MyHashTagCount.getOrDefault(hashtag, 0) + 1);
                    }
                }
            });

            StringBuilder query2 = new StringBuilder(1024);
            query2.append("SELECT user_id_str, hashtags FROM tweet WHERE user_id_str IN (");
            int index = 0;
            for (long friend : contactUsers) {
                if (index > 0) {
                    query2.append(",");
                }
                query2.append(friend);
                index++;
            }
            query2.append(")");
            Dataset<Row> ds3 = sqlContext.sql(query2.toString());
            Map<Long, Map<String, Integer>> userToHashTag = new HashMap<>();
            ds3.foreach((ForeachFunction<Row>) row ->{
                long friendId = row.getLong(0);
                String friendHashtags = row.getString(1);
                String[] hashtagArr = friendHashtags.split(",");
                for (String hashtag : hashtagArr) {
                    hashtag = StringEscapeUtils.unescapeJava(new String(base64.decode(hashtag.getBytes(StandardCharsets.UTF_8))));
                    hashtag = hashtag.toLowerCase(Locale.ENGLISH);
                    if (!excludeSet.contains(hashtag)) {
                        if (userToHashTag.containsKey(friendId)) {
                            userToHashTag.get(friendId).put(hashtag, userToHashTag.get(friendId).getOrDefault(hashtag, 0) + 1);
                        } else {
                            userToHashTag.put(friendId, new HashMap<>());
                        }
                    }
                }
            });

            for (long friend : userToHashTag.keySet()) {
                Map<String, Integer> hashtagMap = userToHashTag.get(friend);
                Set<String> keySet = hashtagMap.keySet();
                keySet.retainAll(MyHashTagCount.keySet());
                int sameTagCount = 0;
                for (String common : keySet) {
                    sameTagCount = sameTagCount + hashtagMap.get(common) + MyHashTagCount.get(common);
                }
                double score = 0.0;
                if (sameTagCount <= 10 || friend == user_id_str) {
                    score = 1.0;
                } else {
                    score = 1.0 + log(1 + sameTagCount - 10);
                }
                interactionScores.put(friend, interactionScores.get(friend) * score);
            }
            String interactionAndhashtagJSON = new JSONObject(interactionScores).toString();
            scoreList.add(new Score(user_id_str, interactionAndhashtagJSON));
        }

        JavaRDD<Score> rddScore = sc.parallelize(scoreList);
        Dataset<Score> dsScore = sqlContext.createDataset(rddScore.rdd(), Encoders.bean(Score.class));

        //write the dataset to csv file
        dsScore.coalesce(1).write().option("encoding", "utf-8").option("header", "true").option("delimiter", "\t").option("nullValue", null).option("quoteAll", "false").option("numPartitions", 100).format("csv").mode("append").save("wasbs://m3blob@blob15619.blob.core.windows.net/outputscore/");
//        ds1.coalesce(1).write().option("encoding", "utf-8").option("header", "true").option("delimiter", "\t").option("nullValue", null).option("quoteAll", "false").option("numPartitions", 100).format("csv").mode("append").save("output");

        //number of records in dataset
//        System.out.println(dsUser1.count());
//        System.out.println(dsScore.count());

        //stop spark
        sc.stop();
    }

    private Tweet filter(String st) {
        String[] arr = st.split("\\t");
        try {
            Tweet tweet = new Tweet(Long.parseLong(arr[0]), Long.parseLong(arr[1]), arr[2], Long.parseLong(arr[3]), Long.parseLong(arr[4]), Long.parseLong(arr[5]), arr[6]);
            return tweet;
        } catch (Exception e) {
            return null;
        }
    }

    private Tweet filterUser(String st) {
        String[] arr = st.split("\\t");
        try {
            Tweet tweet = new Tweet(Long.parseLong(arr[0]), Long.parseLong(arr[1]), arr[2], arr[3]);
            return tweet;
        } catch (Exception e) {
            return null;
        }
    }

//    private Score calculate(long user_id_str, Set<String> excludeSet,SparkConf sparkConf) {
//        String sql = "SELECT user_id_str, in_reply_to_user_id_str, retweeted_user_id_str FROM tweet WHERE user_id_str = " + user_id_str + " AND (in_reply_to_user_id_str != 0 OR retweeted_user_id_str != 0) UNION ALL SELECT user_id_str, in_reply_to_user_id_str, retweeted_user_id_str FROM tweet WHERE in_reply_to_user_id_str = " + user_id_str + " AND user_id_str != " + user_id_str + " UNION ALL SELECT user_id_str,in_reply_to_user_id_str, retweeted_user_id_str FROM tweet WHERE retweeted_user_id_str = " + user_id_str + " AND user_id_str != " + user_id_str;
//        String queryHashtag1 = "SELECT in_reply_to_user_id_str,retweeted_user_id_str,hashtags FROM tweet WHERE user_id_str = " + user_id_str;
//        String queryHashtag2 = "SELECT user_id_str FROM tweet WHERE in_reply_to_user_id_str = " + user_id_str + " OR retweeted_user_id_str = " + user_id_str;
//
////        dsparam.createOrReplaceTempView("tweet");
//        //run sql
//        JavaSparkContext sc = new JavaSparkContext(sparkConf);
//        SQLContext sqlContext = new SQLContext(sc);
//        Dataset<Row> ds = sqlContext.sql(sql);
//        System.out.println("ds count: #################" + ds.count());
//        Map<Long, Interaction> interactionMap = new HashMap<>();
//        List<Row> arrayList = new ArrayList<>();
//        arrayList = ds.collectAsList();
//        for (Row row : arrayList) {
//            long contact = row.getLong(0);
//            long replyUserId = row.getLong(1);
//            long retweetUserId = row.getLong(2);
//
//            if (contact == user_id_str) {
//                if (replyUserId != 0) {
//                    Interaction interaction = interactionMap.getOrDefault(replyUserId, new Interaction());
//                    interaction.setReply();
//                    if (!interactionMap.containsKey(replyUserId)) {
//                        interactionMap.put(replyUserId, interaction);
//                    }
//                } else if (retweetUserId != 0) {
//                    Interaction interaction = interactionMap.getOrDefault(retweetUserId, new Interaction());
//                    interaction.setRetweet();
//                    if (!interactionMap.containsKey(retweetUserId)) {
//                        interactionMap.put(retweetUserId, interaction);
//                    }
//                }
//            } else {
//                Interaction interaction = interactionMap.getOrDefault(contact, new Interaction());
//                if (replyUserId == user_id_str) {
//                    interaction.setReply();
//                } else if (retweetUserId == user_id_str) {
//                    interaction.setRetweet();
//                }
//                if (!interactionMap.containsKey(contact)) {
//                    interactionMap.put(contact, interaction);
//                }
//            }
//        }
//        JSONObject interactionScores = new JSONObject();
//        for (Map.Entry<Long, Interaction> entry : interactionMap.entrySet()) {
//            double score = log(1 + 2 * entry.getValue().getReply() + entry.getValue().getRetweet());
//            interactionScores.put(String.valueOf(entry.getKey()), score);
//        }
//        String interactionJSON = interactionScores.toString();
//
//        //hashtag score
//        JSONObject hashtagScores = new JSONObject();
//        Dataset<Row> ds1 = sqlContext.sql(queryHashtag1);
//        List<Row> arrayList1 = new ArrayList<>();
//        arrayList1 = ds1.collectAsList();
//        String hashtags = null;
//        long replyTo = 0;
//        long retweetTo = 0;
//        Map<String, Integer> MyHashTagCount = new HashMap<>();
//        Set<Long> contactUsers = new HashSet<>();
//        for (Row row : arrayList1) {
//            hashtags = row.getString(2);
//            replyTo = row.getLong(0);
//            if (replyTo != 0) {
//                contactUsers.add(replyTo);
//            }
//            retweetTo = row.getLong(1);
//            if (retweetTo != 0) {
//                contactUsers.add(retweetTo);
//            }
//            String[] hashtagArr = hashtags.split(",");
//            for (String hashtag : hashtagArr) {
//                hashtag = StringEscapeUtils.unescapeJava(new String(base64.decode(hashtag.getBytes(StandardCharsets.UTF_8)))).toLowerCase(Locale.ENGLISH);
//                if (!excludeSet.contains(hashtag)) {
//                    MyHashTagCount.put(hashtag, MyHashTagCount.getOrDefault(hashtag, 0) + 1);
//                }
//            }
//        }
//        Dataset<Row> ds2 = sqlContext.sql(queryHashtag2);
//        List<Row> arrayList2 = new ArrayList<>();
//        arrayList2 = ds2.collectAsList();
//        long user_id = 0l;
//        for (Row row : arrayList2) {
//            user_id = row.getLong(0);
//            contactUsers.add(user_id);
//        }
//
//        StringBuilder query2 = new StringBuilder(1024);
//        query2.append("SELECT user_id_str, hashtags FROM tweet WHERE user_id_str IN (");
//        int index = 0;
//        for (long friend : contactUsers) {
//            if (index > 0) {
//                query2.append(",");
//            }
//            query2.append(friend);
//            index++;
//        }
//        query2.append(")");
//        Dataset<Row> ds3 = sqlContext.sql(query2.toString());
//        List<Row> arrayList3 = new ArrayList<>();
//        arrayList3 = ds3.collectAsList();
//        String friendHashtags = null;
//        long friendId = 0l;
//        Map<Long, Map<String, Integer>> userToHashTag = new HashMap<>();
//        for (Row row : arrayList3) {
//            friendId = row.getLong(0);
//            friendHashtags = row.getString(1);
//            String[] hashtagArr = friendHashtags.split(",");
//            for (String hashtag : hashtagArr) {
//                hashtag = StringEscapeUtils.unescapeJava(new String(base64.decode(hashtag.getBytes(StandardCharsets.UTF_8))));
//                hashtag = hashtag.toLowerCase(Locale.ENGLISH);
//                if (!excludeSet.contains(hashtag)) {
//                    if (userToHashTag.containsKey(friendId)) {
//                        userToHashTag.get(friendId).put(hashtag, userToHashTag.get(friendId).getOrDefault(hashtag, 0) + 1);
//                    } else {
//                        userToHashTag.put(friendId, new HashMap<>());
//                    }
//                }
//            }
//        }
//
//        for (long friend : userToHashTag.keySet()) {
//            Map<String, Integer> hashtagMap = userToHashTag.get(friend);
//            Set<String> keySet = hashtagMap.keySet();
//            keySet.retainAll(MyHashTagCount.keySet());
//            int sameTagCount = 0;
//            for (String common : keySet) {
//                sameTagCount = sameTagCount + hashtagMap.get(common) + MyHashTagCount.get(common);
//            }
//            double score = 0.0;
//            if (sameTagCount <= 10 || friend == user_id_str) {
//                score = 1.0;
//            } else {
//                score = 1.0 + log(1 + sameTagCount - 10);
//            }
//            hashtagScores.put(String.valueOf(friend), score);
//        }
//        String hashtagJSON = hashtagScores.toString();
//        return new Score(user_id_str, interactionJSON, hashtagJSON);
//    }

    public static double log(double x) {
        return 6 * (x - 1) / (x + 1 + 4 * (Math.sqrt(x)));
    }

    static class Interaction {
        private int reply;
        private int retweet;

        Interaction() {
            reply = 0;
            retweet = 0;
        }

        public int getReply() {
            return reply;
        }

        public void setReply() {
            reply++;
        }

        public int getRetweet() {
            return retweet;
        }

        public void setRetweet() {
            retweet++;
        }

    }
}
