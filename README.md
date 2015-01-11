==============================================================================================
                     ##Clustered Replicable Queue (based on Infinispan)
==============================================================================================

This application demonstrates *Replicable Queue* running on *Two Node* in *Java SE*.

The example can be deployed from the command line your IDE directly or using maven.

To try with a *Replicated Queue*, run the following command in separated terminals:

1. Using maven:
    * Node A: `mvn compile exec:java -Dexec.args="A"`
    * Node B: `mvn compile exec:java -Dexec.args="B"`
2. Using command line or IDE directly:
    * Node A: `java -cp "your classpath" -Djava.net.preferIPv4Stack=true org.infinispan.ext.Runner A"`
    * Node B: `java -cp "your classpath" -Djava.net.preferIPv4Stack=true org.infinispan.ext.Runner B"`
