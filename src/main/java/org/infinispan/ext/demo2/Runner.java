package org.infinispan.ext.demo2;

import java.util.Date;

/**
 * Event processing service runner.
 * <p>To run some node use operation:
 * <code>java -cp %your classpath% -Djava.net.preferIPv4Stack=true org.infinispan.ext.demo2.Runner %node name%"</code>
 */
public class Runner {

    /**
     * Run specified node
     *
     * @param args Node name
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        String nodeName = (args.length > 0) ? args[0] : "no name " + new Date();
        EventProcessingService service = new EventProcessingService(nodeName);
        service.start();
    }

}
