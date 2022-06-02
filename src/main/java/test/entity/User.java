package test.entity;

import lombok.Data;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;

@Entity
@Data
public class User {
    private long created_at;
    @Id
    @GeneratedValue
    private long user_id_str;
    private String user_screen_name;
    private String user_description;
}
