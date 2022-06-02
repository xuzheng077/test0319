/**
 * @author Xu Zheng
 * @description
 */
public class Score {
    private long user_id_str;
    private String score;
//    private String interactionScore;//json
//    private String hashtagScore;//json

    public Score(long u_id_str, String iScore){
        user_id_str = u_id_str;
        score = iScore;

    }

    public long getUser_id_str() {
        return user_id_str;
    }

    public void setUser_id_str(long user_id_str) {
        this.user_id_str = user_id_str;
    }

    public String getScore() {
        return score;
    }

    public void setScore(String score) {
        this.score = score;
    }
}
