
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Sets;
import org.apache.commons.lang.StringEscapeUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.codec.binary.Base64;

import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;

import org.rapidoid.jdbc.JDBC;

/**
 * @author Hazel Yang
 * @date 2022-03-09 1:41 PM
 */
public class userRecommendation {
    private static final Base64 base64 = new Base64();
    private static final String REPLY = "reply";
    private static final String RETWEET = "retweet";
    private static final String BOTH = "both";
    private static final String TEAM = "DwendwenBing,134981881645\n";
    private static final String INVALID_MESSAGE = "DwendwenBing,134981881645\nINVALID";
    private static final DecimalFormat df = new DecimalFormat("#.#####");
    private static final String queryCheckValid = "select 1 from tweet where (user_id_str = ? AND (in_reply_to_user_id_str!=0 OR  retweeted_user_id_str!=0)) OR in_reply_to_user_id_str=? OR retweeted_user_id_str=? limit 1;";
    private static final String queryInfo = "select user_screen_name, user_description from user where user_id_str = ? limit 1;";
    private static final String queryTweet1 = "select tweet_text from tweet where (user_id_str = ? and in_reply_to_user_id_str=?) OR (user_id_str=? AND in_reply_to_user_id_str=?) AND tweet_text != 'bnVsbA==' order by created_at desc, id_str desc limit 1;";
    private static final String queryTweet2 = "select tweet_text from tweet where (user_id_str = ? and retweeted_user_id_str=?) OR (user_id_str=? AND retweeted_user_id_str=?) AND tweet_text != 'bnVsbA==' order by created_at desc, id_str desc limit 1;";
    private static final String queryTweet3 = "select tweet_text from tweet where (user_id_str = ? and (in_reply_to_user_id_str=? or retweeted_user_id_str=?)) OR (user_id_str=? AND (in_reply_to_user_id_str=? OR retweeted_user_id_str=?)) AND tweet_text != 'bnVsbA==' order by created_at desc, id_str desc limit 1;";
    private static final String asUserIdQuery = "SELECT in_reply_to_user_id_str,retweeted_user_id_str FROM tweet WHERE user_id_str = ? AND (in_reply_to_user_id_str != 0 OR retweeted_user_id_str != 0);";
    private static final String reQuery = "SELECT user_id_str, in_reply_to_user_id_str, retweeted_user_id_str FROM tweet WHERE in_reply_to_user_id_str = ? UNION ALL SELECT user_id_str,in_reply_to_user_id_str, retweeted_user_id_str FROM tweet WHERE retweeted_user_id_str = ?;";
    private static final String queryHashtag1 = "SELECT in_reply_to_user_id_str,retweeted_user_id_str,hashtags FROM tweet WHERE user_id_str = ?;";
    private static final String queryHashtag2 = "SELECT user_id_str FROM tweet WHERE in_reply_to_user_id_str = ? OR retweeted_user_id_str = ?;";

    private static final String queryKeyword1and2 = "SELECT user_id_str, in_reply_to_user_id_str, tweet_text, hashtags FROM tweet WHERE (user_id_str = ? AND in_reply_to_user_id_str != 0) UNION ALL SELECT user_id_str, in_reply_to_user_id_str, tweet_text, hashtags FROM tweet WHERE in_reply_to_user_id_str = ?;";
    private static final String queryKeyword3and4 = "SELECT user_id_str, retweeted_user_id_str,tweet_text, hashtags FROM tweet WHERE (user_id_str = ? AND retweeted_user_id_str != 0) UNION ALL SELECT user_id_str, retweeted_user_id_str,tweet_text, hashtags FROM tweet WHERE retweeted_user_id_str = ?;";
    private static final String queryKeyword5 = "SELECT user_id_str,retweeted_user_id_str,in_reply_to_user_id_str,tweet_text, hashtags FROM tweet WHERE in_reply_to_user_id_str = ? OR retweeted_user_id_str = ? OR (user_id_str = ? AND (retweeted_user_id_str != 0 OR in_reply_to_user_id_str != 0));";

    private static final Cache<String, String> interactionCache = CacheBuilder.newBuilder()
            //cache的初始容量
            .initialCapacity(100)
            //cache最大缓存数
            .maximumSize(200)
            //设置写缓存后n秒钟过期
            .expireAfterWrite(17, TimeUnit.SECONDS)
            //设置读写缓存后n秒钟过期,实际很少用到,类似于expireAfterWrite
            //.expireAfterAccess(17, TimeUnit.SECONDS)
            .build();

    private static Set<String> excludeSet = new HashSet<>();

    public static void init() {
//        conn = getDBConnection();
        initializeExcludeSet();
        TreeSet<Interaction> set = new TreeSet<>();

    }

    public static String heartbeat(String userId) {
        long userIdLong = Long.parseLong(userId);
        Connection conn = getDBConnection();
        String query = "select user_id_str from tweet where user_id_str = ?;";
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setLong(1, userIdLong);
            ResultSet rs = statement.executeQuery();

            while (rs.next()) {
                long id = rs.getLong(1);

                conn.close();
                return String.valueOf(id);

            }
            conn.close();
            return "null";

        } catch (SQLException e) {
            System.out.println("checkValid SQLException");
            e.printStackTrace();
        }
        return "null";
    }

    static class UserScore {
        long userId;
        double score;

        public UserScore(long userId, double score) {
            this.userId = userId;
            this.score = score;
        }
    }

    public static String getUserRecommendation(String userId, String type, String phrase, String hashtag) {
        Connection conn = getDBConnection();
        long userIdLong = Long.parseLong(userId);
        if (!checkValid(userIdLong, conn)) {
            return INVALID_MESSAGE;
        }
//            if (!REPLY.equals(type) && !RETWEET.equals(type) && !BOTH.equals(type)) {
//                return INVALID_MESSAGE;
//            }
        int typeNum = -1;
        if (REPLY.equals(type)) {
            typeNum = 0;
        } else if (RETWEET.equals(type)) {
            typeNum = 1;
        } else if (BOTH.equals(type)) {
            typeNum = 2;
        } else {
            return INVALID_MESSAGE;
        }

        Map<Long, Double> interactionScoreMap = interactionScore(userIdLong, conn);
//        System.out.println("finish interaction score");
        Map<Long, Double> hashtagScoreMap = hashtagScore(userIdLong, conn);
//        System.out.println("finish hashtag score");
        Map<Long, Double> keywordScoreMap = keywordScore(userIdLong, typeNum, phrase, hashtag);
//        System.out.println("finish keyword score");
        TreeSet<UserScore> entriesSet = new TreeSet<>(new Comparator<UserScore>() {
            @Override
            public int compare(UserScore me1, UserScore me2) {
                double i = me2.score - me1.score;
                if (i > 0) {
                    return 1;
                } else if (i < 0) {
                    return -1;
                }
                return Long.compare(me2.userId, me1.userId);
            }
        });
        for (long user : interactionScoreMap.keySet()) {
            double finalScore = interactionScoreMap.getOrDefault(user, 0.0) * hashtagScoreMap.getOrDefault(user, 1.0) * keywordScoreMap.getOrDefault(user, 0.0);
            if (finalScore > 0) {
                double finalScoreStr = Double.parseDouble(df.format(finalScore));
                entriesSet.add(new UserScore(user, finalScoreStr));
            }
        }
        StringBuilder sb = new StringBuilder(1024);
        sb.append(TEAM);
        for (UserScore userScore : entriesSet) {
            sb.append(getLatestInformationAndTweet(userScore.userId, userIdLong, typeNum));
        }
        try{
            conn.close();
        }catch(Exception e){
            e.printStackTrace();
        }
        return sb.substring(0, sb.length() - 1);
    }

    private static boolean checkValid(long userId, Connection conn) {
//        Connection conn = getDBConnection();
        try {
            PreparedStatement statement = conn.prepareStatement(queryCheckValid);
            statement.setLong(1, userId);
            statement.setLong(2, userId);
            statement.setLong(3, userId);
            ResultSet rs = statement.executeQuery();

            while (rs.next()) {
//                conn.close();
                return true;
            }
//            conn.close();
            return false;

        } catch (SQLException e) {
            System.out.println("checkValid SQLException");
            e.printStackTrace();
        }
        return false;
    }


    private static String getLatestInformationAndTweet(long contactUserId, long userId, int type) {
        StringBuilder sb = new StringBuilder(1024);
        String userScreenName = "";
        String userDescription = "";
        String tweet = "";
        Connection conn = getDBConnection();
        try {
            PreparedStatement statementInfo = conn.prepareStatement(queryInfo);
            statementInfo.setLong(1, contactUserId);
            ResultSet rsInfo = statementInfo.executeQuery();

            while (rsInfo.next()) {
                if (rsInfo.getString("user_screen_name") != null && !rsInfo.getString("user_screen_name").equals("") && !rsInfo.getString("user_screen_name").trim().equals("bnVsbA==")) {
                    userScreenName = rsInfo.getString("user_screen_name");
                }
                if (rsInfo.getString("user_description") != null && !rsInfo.getString("user_description").equals("") && !rsInfo.getString("user_description").trim().equals("bnVsbA==")) {
                    userDescription = rsInfo.getString("user_description");
                }
            }
            if (type == 0) {
                PreparedStatement statementTweet = conn.prepareStatement(queryTweet1);
                statementTweet.setLong(1, contactUserId);
                statementTweet.setLong(2, userId);
                statementTweet.setLong(3, userId);
                statementTweet.setLong(4, contactUserId);
                ResultSet rsTweet = statementTweet.executeQuery();

                while (rsTweet.next()) {
                    tweet = rsTweet.getString("tweet_text");
                }
            } else if (type == 1) {
                PreparedStatement statementTweet = conn.prepareStatement(queryTweet2);
                statementTweet.setLong(1, contactUserId);
                statementTweet.setLong(2, userId);
                statementTweet.setLong(3, userId);
                statementTweet.setLong(4, contactUserId);
                ResultSet rsTweet = statementTweet.executeQuery();

                while (rsTweet.next()) {
                    tweet = rsTweet.getString("tweet_text");
                }
            } else {
                PreparedStatement statementTweet = conn.prepareStatement(queryTweet3);
                statementTweet.setLong(1, contactUserId);
                statementTweet.setLong(2, userId);
                statementTweet.setLong(3, userId);
                statementTweet.setLong(4, userId);
                statementTweet.setLong(5, contactUserId);
                statementTweet.setLong(6, contactUserId);
                ResultSet rsTweet = statementTweet.executeQuery();
                while (rsTweet.next()) {
                    tweet = rsTweet.getString("tweet_text");
                }
            }
            conn.close();
        } catch (SQLException e) {
            System.out.println("getLatestInformation SQLException");
            e.printStackTrace();
        }

        sb.append(contactUserId).append("\t").append(StringEscapeUtils.unescapeJava(new String(base64.decode(userScreenName.getBytes(StandardCharsets.UTF_8))))).append("\t").append(StringEscapeUtils.unescapeJava(new String(base64.decode(userDescription.getBytes(StandardCharsets.UTF_8))))).append("\t").append(StringEscapeUtils.unescapeJava(new String(base64.decode(tweet.getBytes(StandardCharsets.UTF_8))))).append("\n");
        return sb.toString();
    }

    private static void initializeExcludeSet() {
        String query = "select tag from popular;";
        Connection conn = getDBConnection();
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet resultSet = statement.executeQuery();
            String str = null;
            while (resultSet.next()) {
                str = resultSet.getString("tag");
                excludeSet.add(str);
            }
            conn.close();
        } catch (SQLException e) {
            System.out.println("initializeExcludeSet SQLException");
        }
    }

    private static Connection getDBConnection() {
        return JDBC.getConnection();
    }


    private static Map<Long, Double> interactionScore(long userId, Connection conn) {
        // possible way of optimizing: delete where clause
        Map<Long, Interaction> interactionMap = new HashMap<>();
//        Connection conn = getDBConnection();
        try {
            PreparedStatement asUserIdStatement = conn.prepareStatement(asUserIdQuery);
            asUserIdStatement.setLong(1, userId);
            ResultSet asUserIdResultSet = asUserIdStatement.executeQuery();
            while (asUserIdResultSet.next()) {
                long replyUserId = asUserIdResultSet.getLong("in_reply_to_user_id_str");
                long retweetUserId = asUserIdResultSet.getLong("retweeted_user_id_str");
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
            }

            PreparedStatement reStatement = conn.prepareStatement(reQuery);
            reStatement.setLong(1, userId);
            reStatement.setLong(2, userId);
            ResultSet reResultSet = reStatement.executeQuery();
            while (reResultSet.next()) {
                long contact = reResultSet.getLong("user_id_str");
                Interaction interaction = interactionMap.getOrDefault(contact, new Interaction());
                if (reResultSet.getLong("in_reply_to_user_id_str") == userId) {
                    interaction.setReply();
                } else if (reResultSet.getLong("retweeted_user_id_str") == userId) {
                    interaction.setRetweet();
                }
                if (!interactionMap.containsKey(contact)) {
                    interactionMap.put(contact, interaction);
                }
            }
//            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        Map<Long, Double> interactionScores = new HashMap<>(interactionMap.size());
        for (Map.Entry<Long, Interaction> entry : interactionMap.entrySet()) {
            double score = Math.log(1 + 2 * entry.getValue().getReply() + entry.getValue().getRetweet());
            interactionScores.put(entry.getKey(), score);
        }
        return interactionScores;
    }

    private static Map<Long, Double> hashtagScore(long userId,Connection conn) {
        Map<Long, Double> scoreMap = new HashMap<>();
        //get all tweets of a user
//        Connection conn = getDBConnection();
        try {
            PreparedStatement statement = conn.prepareStatement(queryHashtag1);
            statement.setLong(1, userId);
            ResultSet rs = statement.executeQuery();
            String hashtags = null;
            long replyTo = 0;
            long retweetTo = 0;
            Map<String, Integer> MyHashTagCount = new HashMap<>();
            Set<Long> contactUsers = new HashSet<>();
            while (rs.next()) {
                hashtags = rs.getString("hashtags");
                replyTo = rs.getLong("in_reply_to_user_id_str");
                if (replyTo != 0) {
                    contactUsers.add(replyTo);
                }
                retweetTo = rs.getLong("retweeted_user_id_str");
                if (retweetTo != 0) {
                    contactUsers.add(retweetTo);
                }
                String[] hashtagArr = hashtags.split(",");
                for (String hashtag : hashtagArr) {
                    hashtag = StringEscapeUtils.unescapeJava(new String(base64.decode(hashtag.getBytes(StandardCharsets.UTF_8)))).toLowerCase(Locale.ENGLISH);
                    if (!excludeSet.contains(hashtag)) {
                        MyHashTagCount.put(hashtag, MyHashTagCount.getOrDefault(hashtag, 0) + 1);
                    }
                }
            }
            PreparedStatement statement1 = conn.prepareStatement(queryHashtag2);
            statement1.setLong(1, userId);
            statement1.setLong(2, userId);
            ResultSet rs1 = statement1.executeQuery();
            long user_id_str = 0l;
            while (rs1.next()) {
                user_id_str = rs1.getLong("user_id_str");
                contactUsers.add(user_id_str);
            }

            StringBuilder query2 = new StringBuilder(1024);
            query2.append("SELECT user_id_str, hashtags FROM tweet WHERE user_id_str IN (");

            int size = contactUsers.size();
            for (int i = 0; i < size; i++) {
                if (i > 0) {
                    query2.append(",");
                }
                query2.append("?");
            }
            query2.append(")");
            PreparedStatement statement2 = conn.prepareStatement(query2.toString());
            int index = 1;
            for (long friend : contactUsers) {
                statement2.setLong(index, friend);
                index++;
            }
            ResultSet rs2 = statement2.executeQuery();
            String friendHashtags = null;
            long friendId = 0l;
            Map<Long, Map<String, Integer>> userToHashTag = new HashMap<>();
            while (rs2.next()) {
                friendId = rs2.getLong("user_id_str");
                friendHashtags = rs2.getString("hashtags");
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
            }

            for (long friend : userToHashTag.keySet()) {
                Map<String, Integer> hashtagMap = userToHashTag.get(friend);
                Set<String> keySet = hashtagMap.keySet();
                keySet.retainAll(MyHashTagCount.keySet());
                int sameTagCount = 0;
                for (String common : keySet) {
                    sameTagCount = sameTagCount + hashtagMap.get(common) + MyHashTagCount.get(common);
                }
                double score = 0.0;
                if (sameTagCount <= 10 || friend == userId) {
                    score = 1.0;
                } else {
                    score = 1.0 + Math.log(1 + sameTagCount - 10);
                }
                scoreMap.put(friend, score);
            }

            statement.close();
            statement1.close();
            statement2.close();
            rs.close();
            rs1.close();
            rs2.close();
//            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        //for one tweet get all hashtags
        //a map for one user
        return scoreMap;
    }

    private static Map<Long, Double> keywordScore(long userId, int type, String phrase, String hashtag) {
        Map<Long, Double> keywordScoreMap = new HashMap<>();
        Pattern phrasePattern = Pattern.compile(phrase);
        hashtag = hashtag.toLowerCase(Locale.ENGLISH);
//        System.out.println("phrase "+phrase);
        Connection conn = getDBConnection();
        try {
            if (type == 0) {
                Map<Long, Integer> userToMatches = new HashMap<>();
                PreparedStatement statement = conn.prepareStatement(queryKeyword1and2);
                statement.setLong(1, userId);
                statement.setLong(2, userId);
                ResultSet rs = statement.executeQuery();
                long user_id_str = 0;
                long in_reply_to_user_id_str = 0;
                String text = null;
                String hashtags = null;
                while (rs.next()) {
                    user_id_str = rs.getLong("user_id_str");
                    in_reply_to_user_id_str = rs.getLong("in_reply_to_user_id_str");
                    text = StringEscapeUtils.unescapeJava(new String(base64.decode(rs.getString("tweet_text").getBytes(StandardCharsets.UTF_8))));
                    hashtags = rs.getString("hashtags");
                    Matcher matcher = phrasePattern.matcher(text);
                    int numberOfMatches = 0;
                    int i = 0;
                    while (matcher.find(i)) {
                        numberOfMatches++;
                        i = matcher.start() + 1;
                    }
                    String[] hashtagArr = hashtags.split(",");
                    for (String str : hashtagArr) {
                        str = StringEscapeUtils.unescapeJava(new String(base64.decode(str.getBytes(StandardCharsets.UTF_8)))).toLowerCase(Locale.ENGLISH);
                        if (hashtag.equalsIgnoreCase(str)) {
                            numberOfMatches++;
                        }
                    }
                    if (user_id_str == userId) {
                        userToMatches.put(in_reply_to_user_id_str, userToMatches.getOrDefault(in_reply_to_user_id_str, 0) + numberOfMatches);
                    } else {
                        userToMatches.put(user_id_str, userToMatches.getOrDefault(user_id_str, 0) + numberOfMatches);
                    }
                }
                for (long user : userToMatches.keySet()) {
                    double keywordScore = 1.0 + Math.log(userToMatches.get(user) + 1);
                    keywordScoreMap.put(user, keywordScore);
                }
            } else if (type == 1) {
                PreparedStatement statement = conn.prepareStatement(queryKeyword3and4);
                statement.setLong(1, userId);
                statement.setLong(2, userId);
                ResultSet rs = statement.executeQuery();
                long user_id_str = 0l;
                long retweeted_user_id_str = 0l;
                String text = null;
                String hashtags = null;
                Map<Long, Integer> userToMatches = new HashMap<>();
                while (rs.next()) {
                    user_id_str = rs.getLong("user_id_str");
                    retweeted_user_id_str = rs.getLong("retweeted_user_id_str");
                    text = rs.getString("tweet_text");
                    text = StringEscapeUtils.unescapeJava(new String(base64.decode(text.getBytes(StandardCharsets.UTF_8))));
                    hashtags = rs.getString("hashtags");
                    Matcher matcher = phrasePattern.matcher(text);
                    int numberOfMatches = 0;
                    int i = 0;
                    while (matcher.find(i)) {
                        numberOfMatches++;
                        i = matcher.start() + 1;
                    }
                    String[] hashtagArr = hashtags.split(",");
                    for (String str : hashtagArr) {
                        str = StringEscapeUtils.unescapeJava(new String(base64.decode(str.getBytes(StandardCharsets.UTF_8))));
                        if (hashtag.equalsIgnoreCase(str.toLowerCase(Locale.ENGLISH))) {
                            numberOfMatches++;
                        }
                    }
                    if (user_id_str == userId) {
                        userToMatches.put(retweeted_user_id_str, userToMatches.getOrDefault(retweeted_user_id_str, 0) + numberOfMatches);
                    } else {
                        userToMatches.put(user_id_str, userToMatches.getOrDefault(user_id_str, 0) + numberOfMatches);
                    }

                }

                for (long user : userToMatches.keySet()) {
                    double keywordScore = 1.0 + Math.log(userToMatches.get(user) + 1);
                    keywordScoreMap.put(user, keywordScore);
                }
            } else {
                //both
                PreparedStatement statement = conn.prepareStatement(queryKeyword5);
                statement.setLong(1, userId);
                statement.setLong(2, userId);
                statement.setLong(3, userId);
                ResultSet rs = statement.executeQuery();
                long user_id_str = 0l;
                long in_reply_to_user_id_str = 0l;
                long retweeted_user_id_str = 0l;
                String text = null;
                String hashtags = null;
                Map<Long, Integer> userToMatches = new HashMap<>();
                while (rs.next()) {
                    user_id_str = rs.getLong("user_id_str");
                    retweeted_user_id_str = rs.getLong("retweeted_user_id_str");
                    in_reply_to_user_id_str = rs.getLong("in_reply_to_user_id_str");
                    text = StringEscapeUtils.unescapeJava(new String(base64.decode(rs.getString("tweet_text").getBytes(StandardCharsets.UTF_8))));
                    hashtags = rs.getString("hashtags");
//                    System.out.println(user_id_str + "text "+text);
                    Matcher matcher = phrasePattern.matcher(text);
                    int numberOfMatches = 0;
                    int i = 0;
                    while (matcher.find(i)) {
                        numberOfMatches++;
                        i = matcher.start() + 1;
                    }
                    String[] hashtagArr = hashtags.split(",");
                    for (String str : hashtagArr) {
                        str = StringEscapeUtils.unescapeJava(new String(base64.decode(str.getBytes(StandardCharsets.UTF_8)))).toLowerCase(Locale.ENGLISH);
//                            System.out.println(user_id_str+" "+str.toLowerCase(Locale.ENGLISH));
                        if (hashtag.equals(str)) {
                            numberOfMatches++;
                        }
                    }

                    if (user_id_str == userId) {
                        if (in_reply_to_user_id_str != 0) {
                            userToMatches.put(in_reply_to_user_id_str, userToMatches.getOrDefault(in_reply_to_user_id_str, 0) + numberOfMatches);
                        } else if (retweeted_user_id_str != 0) {
                            userToMatches.put(retweeted_user_id_str, userToMatches.getOrDefault(retweeted_user_id_str, 0) + numberOfMatches);
                        }
                    } else {
                        userToMatches.put(user_id_str, userToMatches.getOrDefault(user_id_str, 0) + numberOfMatches);
                    }
                }

                for (long user : userToMatches.keySet()) {
//                    System.out.println("num "+user+" "+userToMatches.get(user));
                    double keywordScore = 1.0 + Math.log(userToMatches.get(user) + 1);
                    keywordScoreMap.put(user, keywordScore);
                }
            }
            conn.close();
        } catch (SQLException e) {
            System.out.println("keyword score exception");
            e.printStackTrace();
        }

        return keywordScoreMap;
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
