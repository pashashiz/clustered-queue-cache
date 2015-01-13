package org.infinispan.ext.demo2;

import java.io.Serializable;

/**
 * Event for processing
 *
 * @author Pavlo Pohrebnyi
 */
public class Event implements Serializable, Comparable<Event> {

    // Fields
    private int id;
    private String reason;
    private String owner;
    private boolean isProcessed;
    private int processingTime;

    /**
     * Create new event
     *
     * @param id Event id
     * @param reason Reason
     */
    public Event(int id, String reason) {
        this.id = id;
        this.reason = reason;
    }

    /**
     * Get event id
     *
     * @return Event id
     */
    public int getId() {
        return id;
    }

    /**
     * Get event reason
     *
     * @return Event reason
     */
    public String getReason() {
        return reason;
    }

    /**
     * Get event owner
     *
     * @return Event owner (Node id)
     */
    public String getOwner() {
        return owner;
    }

    /**
     * Set event owner
     *
     * @param owner Event owner (Node id)
     * @return Itself
     */
    public Event setOwner(String owner) {
        this.owner = owner;
        return this;
    }

    /**
     * Check if event processed
     *
     * @return {@code true} - if event is processed, {@code false} - otherwise
     */
    public boolean isProcessed() {
        return isProcessed;
    }

    /**
     * Set event processing status
     *
     * @param isProcessed {@code true} - if event is processed, {@code false} - otherwise
     * @return Itself
     */
    public Event setProcessed(boolean isProcessed) {
        this.isProcessed = isProcessed;
        return this;
    }

    /**
     * Get processing time
     *
     * @return Processing time in sec (dummy time to process event)
     */
    public int getProcessingTime() {
        return processingTime;
    }

    /**
     * Set processing time
     *
     * @param processingTime Processing time in sec (dummy time to process event)
     * @return Itself
     */
    public Event setProcessingTime(int processingTime) {
        this.processingTime = processingTime;
        return this;
    }

    @Override
    public String toString() {
        return "Event{" +
                "id=" + id +
                ", reason='" + reason + '\'' +
                ", owner='" + owner + '\'' +
                ", isProcessed=" + isProcessed +
                ", processingTime=" + processingTime +
                '}';
    }

    @Override
    public int compareTo(Event event) {
        if (getProcessingTime() < event.getProcessingTime()) return -1;
        if (getProcessingTime() == event.getProcessingTime()) return 0;
        return 1;
    }

}
