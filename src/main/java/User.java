import javax.persistence.*;

/**
 * @author Xu Zheng
 * @description
 */
@Entity
@Table(name = "user")
public class User {
    @Column(name = "created_at")
    public long created_at;
    @Id
    @Column(name = "user_id_str")
    @GeneratedValue
    public long user_id_str;
    @Column(name = "user_screen_name")
    public String user_screen_name;
    @Column(name = "user_description")
    public String user_description;

//    public User() {
//    }
//
//    public User(long created_at, long user_id_str, String user_screen_name, String user_description) {
//        this.created_at = created_at;
//        this.user_id_str = user_id_str;
//        this.user_screen_name = user_screen_name;
//        this.user_description = user_description;
//    }
//
//    public long getCreated_at() {
//        return created_at;
//    }
//
//    public void setCreated_at(long created_at) {
//        this.created_at = created_at;
//    }
//
//    public long getUser_id_str() {
//        return user_id_str;
//    }
//
//    public void setUser_id_str(long user_id_str) {
//        this.user_id_str = user_id_str;
//    }
//
//    public String getUser_screen_name() {
//        return user_screen_name;
//    }
//
//    public void setUser_screen_name(String user_screen_name) {
//        this.user_screen_name = user_screen_name;
//    }
//
//    public String getUser_description() {
//        return user_description;
//    }
//
//    public void setUser_description(String user_description) {
//        this.user_description = user_description;
//    }
}
