package com.github.cassandra.jdbc;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.sql.SQLWarning;

import static org.junit.Assert.*;

public class BaseJdbcObjectTest {
    private BaseJdbcObject jdbcObj;

    @Before
    public void setUp() throws Exception {
        jdbcObj = new BaseJdbcObject(false) {
            @Override
            protected SQLException tryClose() {
                return null;
            }

            @Override
            protected Object unwrap() {
                return null;
            }
        };
    }

    @After
    public void tearDown() throws Exception {
        jdbcObj = null;
    }

    @Test
    public void testGetWarnings() {
        try {
            assertNull(jdbcObj.getWarnings());

            SQLWarning w = new SQLWarning("warning1");
            jdbcObj.appendWarning(w);
            assertEquals(w, jdbcObj.getWarnings());
            assertNull(jdbcObj.getWarnings().getNextWarning());
            jdbcObj.clearWarnings();
            assertNull(jdbcObj.getWarnings());

            jdbcObj.appendWarning(w);
            jdbcObj.appendWarning(w);
            assertEquals(w, jdbcObj.getWarnings());
            assertNull(jdbcObj.getWarnings().getNextWarning());
            jdbcObj.clearWarnings();
            assertNull(jdbcObj.getWarnings());

            SQLWarning w2 = new SQLWarning("warning2");
            jdbcObj.appendWarning(w);
            jdbcObj.appendWarning(w2);
            assertEquals(w, jdbcObj.getWarnings());
            assertEquals(w2, jdbcObj.getWarnings().getNextWarning());
            assertNull(jdbcObj.getWarnings().getNextWarning().getNextWarning());
            jdbcObj.clearWarnings();
            assertNull(jdbcObj.getWarnings());
        } catch (Exception e) {
            e.printStackTrace();
            fail("Exception happened during test: " + e.getMessage());
        }
    }

}
