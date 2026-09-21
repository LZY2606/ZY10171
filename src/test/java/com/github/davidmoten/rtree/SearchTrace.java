package com.github.davidmoten.rtree;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.github.davidmoten.rtree.Entry;
import com.github.davidmoten.rtree.Node;
import com.github.davidmoten.rtree.SearchObserver;
import com.github.davidmoten.rtree.geometry.Geometry;

/**
 * Deterministic, in-memory {@link SearchObserver} used by the query-path
 * tests. Every callback appends an immutable {@link Event}; the resulting
 * trace lets a test assert why traversal proceeded the way it did rather than
 * inferring traversal from the final result set.
 */
final class SearchTrace<T, S extends Geometry> extends SearchObserver<T, S> {

    abstract static class Event {
        final int depth;

        Event(int depth) {
            this.depth = depth;
        }

        @Override
        public final String toString() {
            return describe() + "@" + depth;
        }

        abstract String describe();
    }

    static final class Push extends Event {
        final Object nodeKey;

        Push(Object nodeKey, int depth) {
            super(depth);
            this.nodeKey = nodeKey;
        }

        @Override
        String describe() {
            return "push(" + nodeKey + ")";
        }
    }

    static final class Prune extends Event {
        final Object nodeKey;

        Prune(Object nodeKey, int depth) {
            super(depth);
            this.nodeKey = nodeKey;
        }

        @Override
        String describe() {
            return "prune(" + nodeKey + ")";
        }
    }

    static final class LeafHit extends Event {
        final Object value;

        LeafHit(Object value) {
            super(-1);
            this.value = value;
        }

        @Override
        String describe() {
            return "leafHit(" + value + ")";
        }
    }

    static final class Request extends Event {
        final long amount;

        Request(long amount) {
            super(-1);
            this.amount = amount;
        }

        @Override
        String describe() {
            return "request(" + amount + ")";
        }
    }

    static final class Cancel extends Event {
        Cancel() {
            super(-1);
        }

        @Override
        String describe() {
            return "cancel";
        }
    }

    static final class Complete extends Event {
        Complete() {
            super(-1);
        }

        @Override
        String describe() {
            return "complete";
        }
    }

    static final class Error extends Event {
        final Throwable error;

        Error(Throwable error) {
            super(-1);
            this.error = error;
        }

        @Override
        String describe() {
            return "error(" + error.getClass().getSimpleName() + ")";
        }
    }

    private final java.util.function.Function<? super Node<T, S>, String> nodeNamer;
    private final List<Event> events = Collections.synchronizedList(new ArrayList<Event>());

    SearchTrace(java.util.function.Function<? super Node<T, S>, String> nodeNamer) {
        this.nodeNamer = nodeNamer;
    }

    @Override
    boolean enabled() {
        return true;
    }

    @Override
    void onPush(Node<T, S> node, int depth) {
        events.add(new Push(nodeNamer.apply(node), depth));
    }

    @Override
    void onPrune(Node<T, S> node, int depth) {
        events.add(new Prune(nodeNamer.apply(node), depth));
    }

    @Override
    void onLeafHit(Entry<T, S> entry) {
        events.add(new LeafHit(entry.value()));
    }

    @Override
    void onRequest(long n) {
        events.add(new Request(n));
    }

    @Override
    void onCancel() {
        events.add(new Cancel());
    }

    @Override
    void onComplete() {
        events.add(new Complete());
    }

    @Override
    void onError(Throwable error) {
        events.add(new Error(error));
    }

    List<Event> events() {
        synchronized (events) {
            return new ArrayList<Event>(events);
        }
    }

    /**
     * Human readable rendering included in every assertion failure so that a
     * mismatch shows what traversal actually did.
     */
    String render() {
        StringBuilder sb = new StringBuilder("search trace:\n");
        int i = 0;
        for (Event event : events()) {
            sb.append("  ").append(i++).append(": ").append(event).append('\n');
        }
        return sb.toString();
    }

    /** Event descriptions only, e.g. {@code push(root)@0}. */
    List<String> descriptions() {
        List<String> out = new ArrayList<String>();
        for (Event event : events()) {
            out.add(event.toString());
        }
        return out;
    }
}
