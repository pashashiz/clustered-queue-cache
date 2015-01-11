package org.infinispan.ext;

/**
 * Node runner.
 * <p>To run some node use command:
 * <code>Node A: java -cp %your classpath% -Djava.net.preferIPv4Stack=true org.infinispan.ext.Runner %node name%"</code>,
 * where allowed node names: A, B
 */
public class Runner {

    /**
     * Run specified node
     *
     * @param args Node name (A or B)
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        String nodeName = (args.length > 0) ? args[0] : "Empty";
        switch (nodeName) {
            case "A":
                new NodeA(nodeName).run();
                break;
            case "B":
                new NodeB(nodeName).run();
                break;
            default:
                throw new IllegalArgumentException("There is no Node with name: " + nodeName);
        }
    }

}
