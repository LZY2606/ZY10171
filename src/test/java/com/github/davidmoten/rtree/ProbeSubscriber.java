package com.github.davidmoten.rtree;

import java.util.ArrayList;
import java.util.List;

import com.github.davidmoten.rtree.Entry;
import com.github.davidmoten.rtree.geometry.Geometry;

import rx.Producer;
import rx.Subscriber;

/**
 * A {@link Subscriber} that never requests anything in {@code onStart} so tests
 * drive backpressure deterministically through explicit {@code request} calls.
 * It captures the installed {@link Producer} and every terminal/next signal.
 */
class ProbeSubscriber<T, S extends Geometry> extends Subscriber<Entry<T, S>> {

    private volatile Producer producer;
    private final List<Entry<T, S>> next = new ArrayList<Entry<T, S>>();
    private volatile boolean completed;
    private volatile Throwable error;
    private final boolean rethrowFromOnNext;

    ProbeSubscriber() {
        this(false);
    }

    /**
     * @param rethrowFromOnNext
     *            when true {@code onNext} rethrows so the producer's error
     *            delivery can be exercised
     */
    ProbeSubscriber(boolean rethrowFromOnNext) {
        this.rethrowFromOnNext = rethrowFromOnNext;
    }

    void attachProducer(Producer p) {
        this.producer = p;
        // deliberately does NOT call super.setProducer: that would trigger
        // RxJava's implicit request(Long.MAX_VALUE). Tests drive the producer
        // themselves.
    }

    Producer producer() {
        return producer;
    }

    @Override
    public void onStart() {
        // intentionally empty: explicit producer.request() calls drive demand
    }

    @Override
    public void onNext(Entry<T, S> entry) {
        next.add(entry);
        if (rethrowFromOnNext) {
            throw new TestTraversalException("onNext boom: " + entry.value());
        }
    }

    @Override
    public void onCompleted() {
        completed = true;
    }

    @Override
    public void onError(Throwable e) {
        error = e;
    }

    List<Entry<T, S>> nextEntries() {
        return new ArrayList<Entry<T, S>>(next);
    }

    List<Object> values() {
        List<Object> values = new ArrayList<Object>();
        for (Entry<T, S> entry : nextEntries()) {
            values.add(entry.value());
        }
        return values;
    }

    boolean completed() {
        return completed;
    }

    Throwable error() {
        return error;
    }

    static final class TestTraversalException extends RuntimeException {
        TestTraversalException(String message) {
            super(message);
        }
    }

}
