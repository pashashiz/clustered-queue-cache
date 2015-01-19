package org.infinispan.ext.demo2;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

/**
 * Event DAO.
 *
 * To create Event table use SQL script:
 * <p>CREATE TABLE `event` (
 *  `id` int(11) NOT NULL,
 *  `reason` varchar(64) NOT NULL,
 *  `timestamp` datetime NOT NULL,
 *  `is_processed` bit(1) NOT NULL,
 *  `processing_time` int(11) NOT NULL DEFAULT '0',
 *  PRIMARY KEY (`id`)
 * )
 * @author Pavlo Pohrebnyi
 */
public class EventDao {

    // Constants
    public static final String DB_NAME = "demo2";
    private static final String DB_USER = "root";
    private static final String DB_PSW = "abcABC123";

    // Logger
    private final Logger log = Logger.getLogger(EventDao.class);

    // Get database connection
    private Connection getConnection() throws SQLException {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            return DriverManager.getConnection("jdbc:mysql://localhost/" + DB_NAME, DB_USER, DB_PSW);
        } catch (ClassNotFoundException e) {
            throw new SQLException("MySql driver not found", e);
        }
    }

    /**
     * Get event from database by id
     *
     * @param id Event id
     * @return Event
     * @exception SQLException
     */
    public Event getEvent(int id) throws SQLException {
        try (Connection con = getConnection()) {
            PreparedStatement st = con.prepareStatement(
                    "SELECT * FROM event WHERE id = ?");
            st.setInt(1, id);
            ResultSet rs = st.executeQuery();
            if (rs.next()) {
                Event event = new Event(rs.getInt("id"), rs.getString("reason"), rs.getTimestamp("timestamp"));
                event.setProcessed(rs.getBoolean("is_processed")).setProcessingTime(rs.getInt("processing_time"));
                return event;
            }
            return null;
        }
    }

    /**
     * Get all not processed event reasons from database
     *
     * @return Events
     * @exception SQLException
     */
    public List<String> getNotProcessedReasons() throws SQLException {
        try (Connection con = getConnection()) {
            PreparedStatement st = con.prepareStatement(
                    "SELECT reason FROM event WHERE is_processed = ? GROUP BY reason");
            st.setBoolean(1, false);
            ResultSet rs = st.executeQuery();
            ArrayList<String> reasons = new ArrayList<>();
            while (rs.next())
                reasons.add(rs.getString("reason"));
            return reasons;
        }
    }

    /**
     * Get number of not processed event reasons from database with specified reason
     *
     * @return Events
     * @exception SQLException
     */
    public int countNotProcessedReasons() throws SQLException {
        try (Connection con = getConnection()) {
            PreparedStatement st = con.prepareStatement(
                    "SELECT COUNT(reason) as c FROM event WHERE is_processed = ? GROUP BY reason");
            st.setBoolean(1, false);
            ResultSet rs = st.executeQuery();
            return (rs.next()) ? rs.getInt("c") : 0;
        }
    }

    /**
     * Get all not processed events from database with specified reason
     *
     * @param reason Event reason
     * @return Events
     * @exception SQLException
     */
    public List<Event> getNotProcessedEvents(String reason) throws SQLException {
        try (Connection con = getConnection()) {
            PreparedStatement st = con.prepareStatement(
                    "SELECT * FROM event WHERE (reason = ?) AND (is_processed = ?) ORDER BY processing_time");
            st.setString(1, reason);
            st.setBoolean(2, false);
            ResultSet rs = st.executeQuery();
            ArrayList<Event> events = new ArrayList<>();
            while (rs.next()) {
                Event event = new Event(rs.getInt("id"), rs.getString("reason"), rs.getTimestamp("timestamp"));
                event.setProcessed(rs.getBoolean("is_processed")).setProcessingTime(rs.getInt("processing_time"));
                events.add(event);
            }
            return events;
        }
    }

    /**
     * Get number of not processed events from database with specified reason
     *
     * @param reason Event reason
     * @return Events
     * @exception SQLException
     */
    public int countNotProcessedEvents(String reason) throws SQLException {
        try (Connection con = getConnection()) {
            PreparedStatement st = con.prepareStatement(
                    "SELECT COUNT(id) as c FROM event WHERE (reason = ?) AND (is_processed = ?)");
            st.setString(1, reason);
            st.setBoolean(2, false);
            ResultSet rs = st.executeQuery();
            return (rs.next()) ? rs.getInt("c") : 0;
        }
    }

    /**
     * Save event to database (insert or update operation)
     *
     * @param event Event
     * @return {@code true} - Event was saved successfully, {@code false} - otherwise
     * @exception SQLException
     */
    public boolean saveEvent(Event event) throws SQLException {
        try (Connection con = getConnection()) {
            PreparedStatement st = con.prepareStatement(
                    "INSERT INTO event (id, reason, timestamp, is_processed, processing_time) " +
                    "VALUES (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE " +
                    "reason = VALUES(reason), timestamp = VALUES(timestamp), " +
                    "is_processed = VALUES(is_processed), processing_time = VALUES(processing_time)");
            st.setInt(1, event.getId());
            st.setString(2, event.getReason());
            st.setTimestamp(3, new Timestamp(event.getTimestamp().getTime()));
            st.setBoolean(4, event.isProcessed());
            st.setInt(5, event.getProcessingTime());
            return st.executeUpdate() > 0;
        }
    }

    /**
     * Remove event from database
     *
     * @param id Event id
     * @return {@code true} - Event was removed successfully, {@code false} - otherwise
     * @exception SQLException
     */
    public boolean removeEvent(int id) throws SQLException {
        try (Connection con = getConnection()) {
            PreparedStatement st = con.prepareStatement(
                    "DELETE FROM event WHERE id = ?");
            st.setInt(1, id);
            return st.executeUpdate() > 0;
        }
    }

    /**
     * Remove all events from database
     *
     * @return Number of removed events
     * @exception SQLException
     */
    public int removeAllEvents() throws SQLException {
        try (Connection con = getConnection()) {
            PreparedStatement st = con.prepareStatement(
                    "DELETE FROM event");
            return st.executeUpdate();
        }
    }

}
