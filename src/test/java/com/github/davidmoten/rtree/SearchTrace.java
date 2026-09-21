package com.github.davidmoten.rtree;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.github.davidmoten.rtree.Entry;
import com.github.davidmoten.rtree.Node;
import com.github.davidmoten.rtree.SearchObserver;

/**
 * Package-private test support: a {@link SearchObserver} that records the
 * forward-played query traversal as an ordered list of immutable {@link Event}s.
 *
 * <p>
 * Every assertion failure in the traversal tests renders the whole trace so
 * that the actual visit order (including prunes and not-yet-visited nodes) is
 * visible without stepping through a debugger.
 */
final class SearchTrace {

    private final List<Event> events = new ArrayList<Event>();
    private final List<Long> requests = new ArrayList<Long>();
    private final List<Entry<?, ?>> hits = new ArrayList<Entry<?, ?>>();
    private volatile boolean completed;
    private volatile boolean cancelled;
    private volatile Throwable error;

    SearchObserver observer() {
        return new Recorder();
    }

    List<Event> events() {
        return Collections.unmodifiableList(events);
    }

    List<Long> requests() {
        return Collections.unmodifiableList(requests);
    }

    List<Entry<?, ?>> hits() {
        return Collections.unmodifiableList(hits);
    }

    boolean completed() {
        return completed;
    }

    boolean cancelled() {
        return cancelled;
    }

    Throwable error() {
        return error;
    }

    String render() {
        StringBuilder sb = new StringBuilder();
        for (Event event : events) {
            sb.append(event).append('\n');
        }
        return sb.toString();
    }

    /** Filtered view of events of a single kind, used by precise assertions. */
    List<Event> only(Class<? extends Event> type) {
        List<Event> result = new ArrayList<Event>();
        for (Event event : events) {
            if (type.isInstance(event)) {
                result.add(event);
            }
        }
        return result;
    }

    private final class Recorder extends SearchObserver {

        @Override
        void push(Node<?, ?> node, int depth) {
            events.add(new Push(node, depth));
        }

        @Override
        void prune(Node<?, ?> node, int depth) {
            events.add(new Prune(node, depth));
        }

        @Override
        void entryTested(Entry<?, ?> entry, boolean matches) {
            events.add(new EntryTested(entry, matches));
        }

        @Override
        void hit(Entry<?, ?> entry) {
            hits.add(entry);
            events.add(new Hit(entry));
        }

        @Override
        void request(long n) {
            requests.add(n);
            events.add(new Request(n));
        }

        @Override
        void cancel() {
            cancelled = true;
            events.add(new Cancel());
        }

        @Override
        void complete() {
            completed = true;
            events.add(new Complete());
        }

        @Override
        void error(Throwable error) {
            SearchTrace.this.error = error;
            events.add(new ErrorEvent(error));
        }
    }

    abstract static class Event {
        @Override
        public abstract String toString();
    }

    static final class Push extends Event {
        final Node<?, ?> node;
        final int depth;

        Push(Node<?, ?> node, int depth) {
            this.node = node;
            this.depth = depth;
        }

        @Override
        public String toString() {
            return "push(" + TraversalFixtures.label(node) + ", depth=" + depth + ")";
        }
    }

    static final class Prune extends Event {
        final Node<?, ?> node;
        final int depth;

        Prune(Node<?, ?> node, int depth) {
            this.node = node;
            this.depth = depth;
        }

        @Override
        public String toString() {
            return "prune(" + TraversalFixtures.label(node) + ", depth=" + depth + ")";
        }
    }

    static final class EntryTested extends Event {
        final Entry<?, ?> entry;
        final boolean matches;

        EntryTested(Entry<?, ?> entry, boolean matches) {
            this.entry = entry;
            this.matches = matches;
        }

        @Override
        public String toString() {
            return "entryTested(" + entry.value() + ", matches=" + matches + ")";
        }
    }

    static final class Hit extends Event {
        final Entry<?, ?> entry;

        Hit(Entry<?, ?> entry) {
            this.entry = entry;
        }

        @Override
        public String toString() {
            return "hit(" + entry.value() + ")";
        }
    }

    static final class Request extends Event {
        final long n;

        Request(long n) {
            this.n = n;
        }

        @Override
        public String toString() {
            return "request(" + n + ")";
        }
    }

    static final class Cancel extends Event {
        @Override
        public String toString() {
            return "cancel()";
        }
    }

    static final class Complete extends Event {
        @Override
        public String toString() {
            return "complete()";
        }
    }

    static final class ErrorEvent extends Event {
        final Throwable error;

        ErrorEvent(Throwable error) {
            this.error = error;
        }

        @Override
        public String toString() {
            return "error(" + error.getClass().getSimpleName() + ": " + error.getMessage() + ")";
        }
    }

}
