package org.infinispan.ext.demo2;

import org.apache.log4j.Logger;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.spi.Messages;

import java.io.IOException;
import java.util.Scanner;

/**
 * Service for processing of events
 *
 * @author Pavlo Pohrebnyi
 */
public class EventProcessingService {

    // Fields
    private final Logger log = Logger.getLogger(EventProcessingService.class);
    private boolean isStarted;
    private String nodeName;

    /**
     * Create service for processing of events
     *
     * @param name Service name (Node name)
     */
    public EventProcessingService(String name) {
        nodeName = name;
        System.setProperty("nodeName", name);
    }

    /**
     * Check if service is started
     *
     * @return {@code true} - if is started, {@code false} - otherwise
     */
    public boolean isStarted() {
        return isStarted;
    }

    /**
     * Start service
     */
    public void start() {
        if (!isStarted()) {
            isStarted = true;
            Scanner scanner = new Scanner(System.in);
            log.info("Enter the command: ");
            int i = nodeName.equals("A") ? -2 : -1;
            // Init Event Dispatcher
            EventDispatcher.getInstance();
            // Read commands from command line
            while (isStarted() && i < 100) {
//                try {
                    Event event = new Event(i += 2, "reason-1");
                    event.setProcessingTime(50);
                    EventDispatcher.getInstance().postEvent(event);
                try {
                    Thread.sleep((long) (Math.random() * 100));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                // Parse command
//                    String line = scanner.nextLine();
//                    Command command = new Command(line);
//                    command.parse();
//                    switch (command.operation) {
//                        // Send event to process
//                        case SEND:
//                            Event event = new Event(command.eventId, command.eventReason);
//                            event.setProcessingTime(command.eventProcessingTime);
//                            EventDispatcher.getInstance().postEvent(event);
//                            break;
//                        // Stop service
//                        case STOP:
//                            stop();
//                            break;
//                    }
//                } catch (CmdLineException e) {
//                    // Wrong command
//                    log.error(e.getMessage());
//                    e.getParser().printUsage(System.out);
//                }
            }
        }
    }

    /**
     * Stop service
     */
    public void stop() {
        if (isStarted()) {
            isStarted = false;
        }
    }

    /**
     * User operation
     */
    public static class Command {

        /**
         * Supported operations
         */
        public enum Operation {SEND, STOP;

            @Override
            public String toString() {
                return super.toString().toLowerCase();
            }
        }

        @Argument(index = 0, required = true, usage = "operation")
        public Operation operation;

        @Option(name="-id", usage="event id")
        public int eventId = (int) (Math.random() * 10000);

        @Option(name="-r", usage="event reason")
        public String eventReason = "reason-1";

        @Option(name="-t", usage="event processing time, ms")
        public int eventProcessingTime = 5000;

        private String rawCommand;

        /**
         * Create operation
         *
         * @param rawCommand Raw command
         */
        public Command(String rawCommand) {
            this.rawCommand = rawCommand;
        }

        /**
         * Parse operation
         *
         * @throws CmdLineException
         */
        public void parse() throws CmdLineException {
            CmdLineParser parser = new CmdLineParser(this);
            if(rawCommand == null || rawCommand.isEmpty())
                throw new CmdLineException(parser, Messages.ILLEGAL_OPERAND, "No argument is given");
            parser.parseArgument(rawCommand.split("\\s+"));
        }

        @Override
        public String toString() {
            return "Command{" +
                    "operation=" + operation +
                    ", eventId=" + eventId +
                    ", eventReason='" + eventReason + '\'' +
                    ", eventProcessingTime=" + eventProcessingTime +
                    ", rawCommand='" + rawCommand + '\'' +
                    '}';
        }
    }


}
