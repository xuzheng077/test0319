package test;

import org.rapidoid.log.Log;
import org.rapidoid.setup.App;
import org.rapidoid.setup.My;

public class Main {
    public static void main(String[] args) {
        App.bootstrap(args).jpa(); // bootstrap JPA

//        My.errorHandler((req, resp, error) -> {
//            Log.error("global error handler", error);
//            return null;
//        });
    }
}
