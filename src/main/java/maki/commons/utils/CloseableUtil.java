/** ******CloseableUtil.java*****/
/**
 *Copyright
 *
 **/
package maki.commons.utils;

import java.io.Closeable;
import java.io.IOException;

/**
 * @describe: <pre>
 * </pre>
 */
public class CloseableUtil {

    public static void close(Closeable close) {
        if (close != null) {
            try {
                close.close();
            } catch (IOException e) {
                // ignore
            }
        }
    }
}
