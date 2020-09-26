/*
 * Copyright (c) 2020. AxonIQ
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.axoniq.axonserver.connector.event.impl;

import io.axoniq.axonserver.connector.AxonServerException;
import io.axoniq.axonserver.connector.ErrorCategory;
import io.axoniq.axonserver.connector.event.EventStream;
import io.axoniq.axonserver.connector.impl.AbstractBufferedStream;
import io.axoniq.axonserver.grpc.FlowControl;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.EventWithToken;
import io.axoniq.axonserver.grpc.event.GetEventsRequest;
import io.axoniq.axonserver.grpc.event.PayloadDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static io.axoniq.axonserver.connector.impl.ObjectUtils.nonNullOrDefault;

/**
 * Buffering implementation of the {@link EventStream} used for Event processing.
 */
public class BufferedEventStream
        extends AbstractBufferedStream<EventWithToken, GetEventsRequest>
        implements EventStream {

    private static final Logger logger = LoggerFactory.getLogger(BufferedEventStream.class);

    private static final EventWithToken TERMINAL_MESSAGE = EventWithToken.newBuilder().setToken(-1729).build();

    private final long trackingToken;
    private final boolean forceReadFromLeader;
    private final int heartbeatInterval;
    private final AtomicLong nextMessageDeadline = new AtomicLong();

    /**
     * Construct a {@link BufferedEventStream}, starting a position {@code trackingToken} with the given {@code
     * bufferSize}.
     *
     * @param trackingToken       the position to start this {@link BufferedEventStream} at
     * @param bufferSize          the buffer size of this event stream
     * @param refillBatch         the number of Events to consume prior refilling the buffer
     * @param forceReadFromLeader a {@code boolean} defining whether Events <b>must</b> be read from the leader
     */
    public BufferedEventStream(long trackingToken, int bufferSize, int refillBatch, boolean forceReadFromLeader,
                               long heartbeatInterval, TimeUnit timeUnit) {
        super("unused", bufferSize, refillBatch);
        this.trackingToken = trackingToken;
        this.forceReadFromLeader = forceReadFromLeader;
        this.heartbeatInterval = (int) Math.max(0, Math.min(Integer.MAX_VALUE, timeUnit.toMillis(heartbeatInterval)));
    }

    @Override
    protected GetEventsRequest buildFlowControlMessage(FlowControl flowControl) {
        GetEventsRequest request = GetEventsRequest.newBuilder().setNumberOfPermits(flowControl.getPermits()).build();
        logger.trace("Sending request for data: {}", request);
        return request;
    }

    @Override
    protected GetEventsRequest buildInitialFlowControlMessage(FlowControl flowControl) {
        GetEventsRequest eventsRequest = GetEventsRequest.newBuilder()
                                                         .setTrackingToken(trackingToken + 1)
                                                         .setForceReadFromLeader(forceReadFromLeader)
                                                         .setHeartbeatInterval(heartbeatInterval)
                                                         .setNumberOfPermits(flowControl.getPermits()).build();
        logger.trace("Sending request for data: {}", eventsRequest);
        if (heartbeatInterval > 0) {
            nextMessageDeadline.set(System.currentTimeMillis() + (heartbeatInterval * 2));
        } else {
            nextMessageDeadline.set(Long.MAX_VALUE);
        }
        return eventsRequest;
    }

    @Override
    protected EventWithToken terminalMessage() {
        return TERMINAL_MESSAGE;
    }

    @Override
    public void excludePayloadType(String payloadType, String revision) {
        PayloadDescription payloadToExclude = PayloadDescription.newBuilder()
                                                                .setType(payloadType)
                                                                .setRevision(nonNullOrDefault(revision, ""))
                                                                .build();
        GetEventsRequest request = GetEventsRequest.newBuilder().addBlacklist(payloadToExclude).build();
        logger.trace("Requesting exclusion of message type: {}", request);
        outboundStream().onNext(request);
    }

    protected boolean validMessageForProcessing(EventWithToken message) {
        boolean isHeartbeatMessage = message != null && (!message.hasEvent() || message.getEvent().equals(Event.getDefaultInstance()));
        if (heartbeatInterval > 0) {
            if (message != null) {
                nextMessageDeadline.set(System.currentTimeMillis() + (heartbeatInterval * 2));
                if (isHeartbeatMessage && logger.isTraceEnabled()) {
                    logger.trace("Received heartbeat message from AxonServer with token {}", message.getToken());
                }
            } else {
                long deadline = nextMessageDeadline.get();
                if (deadline < System.currentTimeMillis()) {
                    close();
                    throw new AxonServerException(ErrorCategory.CONNECTION_FAILED, "Failed to receive message within deadline (" + heartbeatInterval * 2 + " ms). Assuming dead connection.", clientId());
                }
            }
        }
        return message == null || !isHeartbeatMessage;
    }

    @Override
    public EventWithToken next() throws InterruptedException {
        EventWithToken nextMessage;
        do {
            nextMessage = super.next();
        } while (!validMessageForProcessing(nextMessage));
        return nextMessage;
    }

    @Override
    public EventWithToken nextIfAvailable() {
        EventWithToken eventWithToken;
        do {
            eventWithToken = super.nextIfAvailable();
        } while (!validMessageForProcessing(eventWithToken));
        return eventWithToken;
    }

    @Override
    public EventWithToken nextIfAvailable(long timeout, TimeUnit unit) throws InterruptedException {
        long waitTimeout = System.currentTimeMillis() + unit.toMillis(timeout);
        EventWithToken eventWithToken;
        long waitTimeLeft;
        do {
            long now = System.currentTimeMillis();
            long timeToDeadline = nextMessageDeadline.get() - now;
            waitTimeLeft = waitTimeout - now;
            long waitTime = Math.min(Math.max(0, timeToDeadline), Math.max(0, waitTimeLeft));
            logger.debug("Checking events with timeout of {} (timeToDeadline = {}, waitTimeLeft = {})", waitTime, timeToDeadline, waitTimeLeft);
            eventWithToken = super.nextIfAvailable(waitTime, TimeUnit.MILLISECONDS);
            // if this message is a heartbeat, we consider that "no data".
            eventWithToken = !validMessageForProcessing(eventWithToken) ? null : eventWithToken;
        } while (eventWithToken == null && waitTimeLeft > 0);
        return eventWithToken;
    }

    @Override
    public EventWithToken peek() {
        EventWithToken peeked = super.peek();
        while (!validMessageForProcessing(peeked)) {
            // we have peeked a message not meant for downstream consumption (e.g. heartbeat)
            // we consume the message and peek again
            super.nextIfAvailable();
            peeked = super.peek();
        }
        return peeked;
    }
}
