
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.translate.UnicodeEscaper;
import org.json.JSONObject;
import org.json.JSONArray;
import org.rapidoid.annotation.Run;
import org.rapidoid.jdbc.JDBC;
import org.rapidoid.jpa.JPA;
import org.rapidoid.setup.App;
import org.rapidoid.setup.On;

import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.text.DecimalFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


//need to transform date
//need to include retweeted_status.user
/**
 * @author Xu Zheng
 * @description
 */
@Run
public class test {
    public static void main(String[] args) throws Exception{
//        App.bootstrap(args,"profiles=mysql").jpa(); // bootstrap JPA
//        App.bootstrap(args).jpa().auth();
        On.get("/twitter").plain((String user_id, String type, String phrase, String hashtag) -> {
//            System.out.println("user_id: " + user_id);
//            System.out.println("type: " + type);
//            System.out.println("phrase: " + phrase);
//            System.out.println("hashtag: " + hashtag);
            Connection connection = JDBC.getConnection();
            System.out.println(connection);
            long userId = Long.parseLong(user_id);
            StringBuilder sb = new StringBuilder();
            String query = "select user_description,user_screen_name from user where user_id_str = ?;";
            try {
                PreparedStatement statement = connection.prepareStatement(query);
                statement.setLong(1, userId);
                ResultSet rs = statement.executeQuery();

                while (rs.next()) {
                    String desc = rs.getString(1);
                    String name = rs.getString(2);
                    sb.append(name).append("\t").append(desc).append("\n");
                }
                connection.close();
                return sb.toString();
            } catch (SQLException e) {
                System.out.println("checkValid SQLException");
                e.printStackTrace();
            }
            return sb.toString();
//            return String.valueOf("num");
        });
//        double finalScore = 11334.06352564;
//        DecimalFormat df = new DecimalFormat("#.#####");
//        System.out.println(Double.parseDouble(df.format(finalScore)));
//        String phrase = "ولايمارسون نشاط رياض";
//        String text = "ولايماسون نشاط رياضولايمارسون نشاط رياض";
//        Pattern phrasePattern = Pattern.compile(phrase);
//        Matcher matcher = phrasePattern.matcher(text);
//        int count = 0;
//        int i = 0;
//        while (matcher.find(i)) {
//            count++;
//            i = matcher.start() + 1;
//        }
//        System.out.println(count);
//        int c = 0;
//        int j = -1;
//
//        while (j != 0) {
//            j = text.indexOf(phrase, j) + 1;
//            if (j != 0) c++;
//        }
//        System.out.println(c);
//        String text1 = "that's my lifelife lesson";
//        Matcher matcher1 = phrasePattern.matcher(text1);
//        int count1 = 0;
//        int i1 = 0;
//        while (matcher1.find(i1)) {
//            count1++;
//            i1 = matcher1.start() + 1;
//        }
//        System.out.println(count1);
//
//        String a = "ريجيم";
//        String b = "ريجيم";
//        System.out.println(a.toLowerCase(Locale.ENGLISH).equals(b.toLowerCase(Locale.ENGLISH)));

    }
}
