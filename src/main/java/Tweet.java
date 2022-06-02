import java.io.Serializable;

public class Tweet implements Serializable {
    private long created_at;
    private long id_str;
    private String tweet_text;
    private long in_reply_to_user_id_str;
    private long retweeted_user_id_str;
    private long user_id_str;
    private String user_screen_name;
    private String user_description;
    private String hashtags;

    public Tweet() {
    }

    //User
    public Tweet(long created_at, long user_id_str, String user_screen_name, String user_description) {
        this.created_at = created_at;
        this.user_id_str = user_id_str;
        this.user_screen_name = user_screen_name;
        this.user_description = user_description;
    }

    public Tweet(long created_at, long id_str, String text, long in_reply_to_user_id_str, long retweeted_user_id_str, long user_id_str, String hashtags) {
        this.created_at = created_at;
        this.id_str = id_str;
        this.tweet_text = text;
        this.in_reply_to_user_id_str = in_reply_to_user_id_str;
        this.retweeted_user_id_str = retweeted_user_id_str;
        this.user_id_str = user_id_str;
        this.hashtags = hashtags;
    }

    public Tweet(long created_at, long id_str, String text, long in_reply_to_user_id_str, long retweeted_user_id_str, long user_id_str, String user_screen_name, String user_description, String hashtags) {
        this.created_at = created_at;
        this.id_str = id_str;
        this.tweet_text = text;
        this.in_reply_to_user_id_str = in_reply_to_user_id_str;
        this.retweeted_user_id_str = retweeted_user_id_str;
        this.user_id_str = user_id_str;
        this.user_screen_name = user_screen_name;
        this.user_description = user_description;
        this.hashtags = hashtags;
    }

    public long getCreated_at() {
        return created_at;
    }

    public void setCreated_at(long created_at) {
        this.created_at = created_at;
    }

    public long getId_str() {
        return id_str;
    }

    public void setId_str(long id_str) {
        this.id_str = id_str;
    }

    public String getTweet_text() {
        return tweet_text;
    }

    public void setTweet_text(String tweet_text) {
        this.tweet_text = tweet_text;
    }

    public long getIn_reply_to_user_id_str() {
        return in_reply_to_user_id_str;
    }

    public void setIn_reply_to_user_id_str(long in_reply_to_user_id_str) {
        this.in_reply_to_user_id_str = in_reply_to_user_id_str;
    }

    public long getRetweeted_user_id_str() {
        return retweeted_user_id_str;
    }

    public void setRetweeted_user_id_str(long retweeted_user_id_str) {
        this.retweeted_user_id_str = retweeted_user_id_str;
    }

    public long getUser_id_str() {
        return user_id_str;
    }

    public void setUser_id_str(long user_id_str) {
        this.user_id_str = user_id_str;
    }

    public String getUser_screen_name() {
        return user_screen_name;
    }

    public void setUser_screen_name(String user_screen_name) {
        this.user_screen_name = user_screen_name;
    }

    public String getUser_description() {
        return user_description;
    }

    public void setUser_description(String user_description) {
        this.user_description = user_description;
    }

    public String getHashtags() {
        return hashtags;
    }

    public void setHashtags(String hashtags) {
        this.hashtags = hashtags;
    }
}
