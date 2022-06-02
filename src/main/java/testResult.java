import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringEscapeUtils;

import java.nio.charset.StandardCharsets;

/**
 * @author Xu Zheng
 * @description
 */
public class testResult {
    public static void main(String[] args) {
        String a = "Ce n'est pas du futur que j'ai peur, ce qui me rend anxieuse c'est de rÃ©pÃ©ter le passÃ©.";
        System.out.println(StringEscapeUtils.escapeJava(a));
        a = StringEscapeUtils.escapeJava(a);
        Base64 base64 = new Base64();
        String b = new String(base64.encode(a.getBytes(StandardCharsets.UTF_8)));
        String c = StringEscapeUtils.unescapeJava(new String(base64.decode(b.getBytes(StandardCharsets.UTF_8))));
        System.out.println(c);
    }
}
