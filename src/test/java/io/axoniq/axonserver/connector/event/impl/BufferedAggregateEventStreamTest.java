package io.axoniq.axonserver.connector.event.impl;

import io.axoniq.axonserver.connector.impl.StreamClosedException;
import io.axoniq.axonserver.grpc.event.Event;
import org.junit.jupiter.api.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class validationg the {@link BufferedAggregateEventStream}.
 *
 * @author Allard Buijze
 */
class BufferedAggregateEventStreamTest {

    private BufferedAggregateEventStream testSubject;

    @BeforeEach
    void setUp() {
        testSubject = new BufferedAggregateEventStream(10);
    }

    @Test
    void testEventStreamPropagatesErrorOnHasNext() {
        testSubject.onError(new RuntimeException("Mock"));

        assertThrows(StreamClosedException.class, () -> testSubject.hasNext());
    }

    @Test
    void testEventStreamPropagatesErrorOnHasNextAfterReadingAvailableEvents() throws InterruptedException {
        testSubject.onNext(Event.getDefaultInstance());
        testSubject.onNext(Event.getDefaultInstance());
        testSubject.onError(new RuntimeException("Mock"));

        assertTrue(testSubject.hasNext());
        assertEquals(Event.getDefaultInstance(), testSubject.next());
        assertTrue(testSubject.hasNext());
        assertEquals(Event.getDefaultInstance(), testSubject.next());
        assertThrows(StreamClosedException.class, () -> testSubject.hasNext());
    }
}