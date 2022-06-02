import org.apache.commons.codec.binary.Base64;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
//import org.apache.commons.text.StringEscapeUtils;
import org.apache.commons.lang.StringEscapeUtils;

import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;


public class Filter implements Serializable {
    //    private static String INPUT_FILE = "sample.txt";
//    private static String INPUT_FILE = "s3a://cmucc-datasets/twitter/s22/part-r-00000.gz";
    private static String INPUT_FILE = "wasb://datasets@clouddeveloper.blob.core.windows.net/twitter-dataset/part-r-00000.gz";

    private static Set<String> ls = new HashSet<>(Arrays.asList("ar", "en", "fr", "in", "pt", "es", "tr", "ja"));

    public static void main(String[] args) throws IOException {
        Filter job = new Filter();
        long startTime = System.currentTimeMillis();
        job.startJob();
        long stopTime = System.currentTimeMillis();
        long elapsedTime = stopTime - startTime;
        System.out.println("Execution time in s : " + elapsedTime / 1000);
    }

    private long transformDate(String time) {
        SimpleDateFormat formatter = new SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZZ yyyy");
        long timestamp = 0l;
        try {
            Date date = formatter.parse(time);
            timestamp = date.getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return timestamp;
    }

    //check whether a string can be converted to json
    private static boolean isJSON(String str) {
        try {
            new JSONObject(str);
        } catch (JSONException ex) {
            try {
                new JSONArray(str);
            } catch (JSONException ex1) {
                return false;
            }
        }
        return true;
    }

    //spark job
    private void startJob() {
        //configuration
        SparkConf sparkConf = new SparkConf().setAppName("Spark RDD Filter")
//                .setMaster("yarn-client")
                .setMaster("local[4]")
                .set("spark.executor.memory", "4g")
                .set("spark.driver.memory", "1g")
                .set("spark.sql.shuffle.partitions", "300");
//                .set("fs.azure.account.key.blob15619.blob.core.windows.net",
//                "");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        SQLContext sqlContext = new SQLContext(sc);

        //convert input file to rdd
        JavaRDD<String> inputFile = sc.textFile(INPUT_FILE);

        //filter each row
        JavaRDD<Tweet[]> rdd0 = inputFile.map(x -> filter(x)).cache();
        JavaRDD<Tweet> rdd1 = rdd0.map(x -> x[0]);
        JavaRDD<Tweet> rddUser1 = rdd0.flatMap(x -> Arrays.asList(new Tweet[]{x[1],x[2]}).iterator());

        //filter null row
        Function<Tweet, Boolean> filter1 = k -> (k != null);
        JavaRDD<Tweet> rdd2 = rdd1.filter(filter1);
        JavaRDD<Tweet> rddUser2 = rddUser1.filter(filter1);

        //filter rddUser2
        //convert rdd to dataset
        Dataset<Tweet> dsUser = sqlContext.createDataset(rddUser2.rdd(), Encoders.bean(Tweet.class));

        //create a view of the dataset
        dsUser.createOrReplaceTempView("user");
        //filter Duplicate Tweets
        String sqlUser = "SELECT\n" +
                "  created_at, \n" +
                "  user_id_str, \n" +
                "  user_screen_name, \n" +
                "  user_description \n" +
                "FROM (\n" +
                "  SELECT\n" +
                "    created_at, \n" +
                "    user_id_str, \n" +
                "    user_screen_name, \n" +
                "    user_description, \n" +
                "    row_number() OVER (PARTITION BY user_id_str ORDER BY created_at DESC) as rank\n" +
                "  FROM user) tmp\n" +
                "WHERE\n" +
                "  rank = 1";

        //run sql
        Dataset<Row> dsUser1 = sqlContext.sql(sqlUser);
        dsUser1.coalesce(1).write().option("encoding", "utf-8").option("header", "true").option("delimiter", "\t").option("nullValue", null).option("quoteAll", "false").option("numPartitions", 100).format("csv").mode("append").save("output");


        //convert rdd to dataset
        Dataset<Tweet> ds = sqlContext.createDataset(rdd2.rdd(), Encoders.bean(Tweet.class));

        //create a view of the dataset
        ds.createOrReplaceTempView("tweet");
        //filter Duplicate Tweets
        String sql = "SELECT\n" +
                "  created_at, \n" +
                "  id_str, \n" +
                "  tweet_text, \n" +
                "  in_reply_to_user_id_str, \n" +
                "  retweeted_user_id_str, \n" +
                "  user_id_str, \n" +
                "  hashtags\n" +
                "FROM (\n" +
                "  SELECT\n" +
                "    created_at, \n" +
                "    id_str, \n" +
                "    tweet_text, \n" +
                "    in_reply_to_user_id_str, \n" +
                "    retweeted_user_id_str, \n" +
                "    user_id_str, \n" +
                "    hashtags,\n" +
                "    row_number() OVER (PARTITION BY id_str ORDER BY id_str DESC) as rank\n" +
                "  FROM tweet) tmp\n" +
                "WHERE\n" +
                "  rank = 1";

        //run sql
        Dataset<Row> ds1 = sqlContext.sql(sql);

//        String sqlJoin = "SELECT distinct IF(u.created_at IS NULL\n" +
//                "          OR (t.user_id_str = u.user_id_str AND t.created_at>=u.created_at), t.created_at, u.created_at)\n" +
//                "       AS t_created_at,\n" +
//                "       IF(u.user_id_str IS NULL\n" +
//                "          OR (t.user_id_str = u.user_id_str AND t.created_at>=u.created_at), t.user_id_str, u.user_id_str)\n" +
//                "       AS user_id_str,\n" +
//                "       IF(u.user_screen_name IS NULL\n" +
//                "          OR (t.user_id_str = u.user_id_str AND t.created_at>=u.created_at), t.user_screen_name, u.user_screen_name) AS\n" +
//                "       user_screen_name,\n" +
//                "       IF(u.user_description IS NULL\n" +
//                "          OR (t.user_id_str = u.user_id_str AND t.created_at>=u.created_at), t.user_description, u.user_description) AS\n" +
//                "       user_description,\n" +
//                "       t.id_str,\n" +
//                "       t.tweet_text,\n" +
//                "       t.in_reply_to_user_id_str,\n" +
//                "       t.retweeted_user_id_str,\n" +
//                "       t.hashtags\n" +
//                "FROM   (SELECT created_at,\n" +
//                "               id_str,\n" +
//                "               tweet_text,\n" +
//                "               in_reply_to_user_id_str,\n" +
//                "               retweeted_user_id_str,\n" +
//                "               user_id_str,\n" +
//                "               user_screen_name,\n" +
//                "               user_description,\n" +
//                "               hashtags\n" +
//                "        FROM   (SELECT created_at,\n" +
//                "                       id_str,\n" +
//                "                       tweet_text,\n" +
//                "                       in_reply_to_user_id_str,\n" +
//                "                       retweeted_user_id_str,\n" +
//                "                       user_id_str,\n" +
//                "                       user_screen_name,\n" +
//                "                       user_description,\n" +
//                "                       hashtags,\n" +
//                "                       Row_number()\n" +
//                "                         over (\n" +
//                "                           PARTITION BY id_str\n" +
//                "                           ORDER BY id_str DESC) AS rank\n" +
//                "                FROM   tweet) tmp\n" +
//                "        WHERE  rank = 1) t\n" +
//                "       left join (SELECT created_at,\n" +
//                "                         user_id_str,\n" +
//                "                         user_screen_name,\n" +
//                "                         user_description\n" +
//                "                  FROM   (SELECT created_at,\n" +
//                "                                 user_id_str,\n" +
//                "                                 user_screen_name,\n" +
//                "                                 user_description,\n" +
//                "                                 Row_number()\n" +
//                "                                   over (\n" +
//                "                                     PARTITION BY user_id_str\n" +
//                "                                     ORDER BY created_at DESC) AS rank\n" +
//                "                          FROM   USER) tmp\n" +
//                "                  WHERE  rank = 1) u\n" +
//                "              ON t.user_id_str = u.user_id_str or t.retweeted_user_id_str = u.user_id_str";
//        Dataset<Row> dsFinal = sqlContext.sql(sqlJoin);

        //write the dataset to csv file
//        ds1.coalesce(1).write().option("encoding", "utf-8") .option("header", "true").option("delimiter", "\t").option("nullValue", null).option("quoteAll", "false").option("numPartitions", 100).format("csv").mode("append").save("wasbs://m3blob@blob15619.blob.core.windows.net/output");
        ds1.coalesce(1).write().option("encoding", "utf-8").option("header", "true").option("delimiter", "\t").option("nullValue", null).option("quoteAll", "false").option("numPartitions", 100).format("csv").mode("append").save("output");

        //number of records in dataset
//        System.out.println(dsUser1.count());
//        System.out.println(ds1.count());

        //stop spark
        sc.stop();
    }

    //filter a string record and return a tweet object, if not legal, return null
    private Tweet[] filter(String st) {

        //Malformed JSON Object
        if (!isJSON(st)) {
//            System.out.println("is not json");
            return new Tweet[]{null, null, null};
        }

        //Malformed Tweets
        JSONObject t = new JSONObject(st);
        if ((!t.has("id") || t.get("id").toString().equals("null")) && (!t.has("id_str") || t.get("id_str").toString().equals("null"))) {
//            System.out.println("no id");
            return new Tweet[]{null, null, null};
        }
        JSONObject u = new JSONObject(t.get("user").toString());
        if ((!u.has("id") || u.get("id").toString().equals("null")) && (!u.has("id_str") || u.get("id_str").toString().equals("null"))) {
//            System.out.println("no user id");
            return new Tweet[]{null, null, null};
        }
        if (!t.has("created_at") || t.get("created_at").toString().equals("null")) {
//            System.out.println("no create at");
            return new Tweet[]{null, null, null};
        }
        if (!t.has("text") || t.get("text").toString().equals("") || t.get("text").toString().equals("null")) {
//            System.out.println("no text");
            return new Tweet[]{null, null, null};
        }
        JSONObject e = new JSONObject(t.get("entities").toString());
        if (!e.has("hashtags") || e.get("hashtags").toString().equals("null")) {
//            System.out.println("no hashtags");
            return new Tweet[]{null, null, null};
        }
        JSONArray h = new JSONArray(e.get("hashtags").toString());
        if (h == null || h.length() == 0) {
//            System.out.println("no hashtag again");
            return new Tweet[]{null, null, null};
        }

        //Language of Tweets
        if (!t.has("lang") || !ls.contains(t.get("lang").toString())) {
//            System.out.println("no valid lang");
            return new Tweet[]{null, null, null};
        }

        long created_at = transformDate(t.get("created_at").toString());
        long id_str = Long.parseLong(t.get("id_str").toString());
//        String text = t.get("text").toString().replaceAll("\r\n|\n|\r|\t", " ");
        Base64 base64 = new Base64();
        String text = StringEscapeUtils.escapeJava(t.get("text").toString());
        text = new String(base64.encode(text.getBytes()));
//        try{
//            text = new UnicodeUnescaper().translate(text);
//        }catch(Exception ee){
//            text = StringEscapeUtils.escapeJava(t.get("text").toString());
//        }
        long in_reply_to_user_id_str = t.get("in_reply_to_user_id_str").toString().equals("null") ? 0 : Long.parseLong(t.get("in_reply_to_user_id_str").toString());
        long user_id_str = Long.parseLong(u.get("id_str").toString());
//        String user_screen_name = u.get("screen_name").toString().replaceAll("\r\n|\n|\r|\t", " ");
        String user_screen_name = StringEscapeUtils.escapeJava(u.get("screen_name").toString());
        user_screen_name = new String(base64.encode(user_screen_name.getBytes()));
//        try{
//            user_screen_name = new UnicodeUnescaper().translate(user_screen_name);
//        }catch(Exception ee){
//            user_screen_name = StringEscapeUtils.escapeJava(u.get("screen_name").toString());
//        }
        String user_description = StringEscapeUtils.escapeJava(u.get("description").toString());
        user_description = new String(base64.encode(user_description.getBytes()));
//        try{
//            user_description = new UnicodeUnescaper().translate(user_description);
//        }catch(Exception ee){
//            user_description = StringEscapeUtils.escapeJava(u.get("description").toString());
//        }
        StringBuilder hashtags = new StringBuilder();
        for (int i = 0; i < h.length(); i++) {
            JSONObject ht = h.getJSONObject(i);
            if (ht.has("text")) {
//                hashtags.append(ht.get("text").toString().replaceAll("\r\n|\n|\r|\t", " ")).append(",");
                String inner_text = StringEscapeUtils.escapeJava(ht.get("text").toString());
                inner_text = new String(base64.encode(inner_text.getBytes()));
//                try{
//                    inner_text = new UnicodeUnescaper().translate(inner_text);
//                }catch(Exception ee){
//                    inner_text = StringEscapeUtils.escapeJava(ht.get("text").toString());
//                }
                hashtags.append(inner_text).append(",");
            }
        }
        long retweeted_user_id_str = 0l;
        String original_user_description = "";
        String original_user_screen_name = "";
        if (t.has("retweeted_status") && !t.get("retweeted_status").toString().equals("null")) {
            JSONObject rs = new JSONObject(t.get("retweeted_status").toString());
            JSONObject ru = new JSONObject(rs.get("user").toString());
            if (!ru.get("id_str").toString().equals("null") && !ru.get("id_str").equals("")) {
                retweeted_user_id_str = Long.parseLong(ru.get("id_str").toString());
                original_user_screen_name = ru.get("screen_name").toString();
                original_user_screen_name = new String(base64.encode(original_user_screen_name.getBytes()));
                original_user_description = ru.get("description").toString();
                original_user_description = new String(base64.encode(original_user_description.getBytes()));
            }
        }
//        if (retweeted_user_id_str != null && retweeted_user_id_str.equals("")) {
//            retweeted_user_id_str = null;
//        }
//        System.out.println("not null");
        //create a tweet object
        Tweet[] objects = new Tweet[9];
        objects[0] = new Tweet(created_at, id_str, text, in_reply_to_user_id_str, retweeted_user_id_str, user_id_str, user_screen_name, user_description, hashtags.substring(0, hashtags.length() - 1).toString());
        objects[1] = new Tweet(created_at, retweeted_user_id_str, original_user_screen_name, original_user_description);
        objects[2] = new Tweet(created_at, user_id_str, user_screen_name, user_description);
        return objects;
    }
}
