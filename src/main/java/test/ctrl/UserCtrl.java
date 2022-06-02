package test.ctrl;

import org.rapidoid.annotation.Controller;
import org.rapidoid.annotation.GET;
import org.rapidoid.annotation.Param;
import org.rapidoid.jpa.JPA;
import org.rapidoid.jpa.JPQL;
import test.entity.User;

import java.util.List;

@Controller
public class UserCtrl {
    @GET("/twitter")
    public String get(@Param("user_id") String user_id) {
        System.out.println(user_id);
        String sql = "SELECT user_descritpion FROM User where user_id_str = 17691647";
        JPQL jpql = JPA.jpql(sql);
        jpql.execute();
        List<String> list = jpql.getPage(0,10);
        System.out.println(list.size());
        return "null";
    }

}
