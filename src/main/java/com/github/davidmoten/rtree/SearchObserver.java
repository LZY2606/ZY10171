package com.github.davidmoten.rtree;

import com.github.davidmoten.rtree.geometry.Geometry;

/**
 * <p>
 * Package-private instrumentation hook for the query path. This type is
 * <b>not part of the public API</b> and exists only so that tests can record
 * how traversal proceeds independently of the entries that eventually get
 * emitted to a {@code Subscriber}.
 * </p>
 *
 * <p>
 * Two traversal engines report to this hook:
 * </p>
 * <ul>
 * <li>the backpressure-aware stack walk in {@code Backpressure} (entered when
 * downstream requests a finite number of entries), and</li>
 * <li>the no-backpressure recursive walk in {@code LeafHelper} and
 * {@code NonLeafHelper} (entered by a {@code Long.MAX_VALUE} request).</li>
 * </ul>
 *
 * <p>
 * Events describe structural decisions only: a node being pushed onto the
 * traversal stack (MBR satisfied the predicate), being pruned (MBR failed), a
 * leaf entry matching, demand expressed by {@code request} and terminal
 * cancellation/completion/error. Leaf entries whose predicate returns false
 * are intentionally not reported: they are neither hits nor pruned nodes,
 * they simply do not consume demand.
 * </p>
 *
 * @param <T> entry value type
 * @param <S> entry geometry type
 */
abstract class SearchObserver<T, S extends Geometry> {

    private static final SearchObserver<Object, Geometry> NOOP = new SearchObserver<Object, Geometry>() {
    };

    @SuppressWarnings("unchecked")
    static <T, S extends Geometry> SearchObserver<T, S> noop() {
        return (SearchObserver<T, S>) NOOP;
    }

    /**
     * Returns true when this observer wants events. The default no-op
     * observer returns false so the traversal engines avoid the cost of
     * calculating stack depth or preparing event arguments.
     *
     * @return true if the event callbacks should be invoked
     */
    boolean enabled() {
        return false;
    }

    /**
     * A node is about to be visited: its minimum bounding rectangle satisfied
     * the search predicate and it has been pushed onto the traversal stack
     * (backpressure path) or entered recursively (fast path).
     *
     * @param node
     *            the node whose MBR passed the predicate
     * @param depth
     *            tree depth of the push; the root is depth 0, its children
     *            depth 1 and so on
     */
    void onPush(Node<T, S> node, int depth) {
    }

    /**
     * A node has been pruned: its minimum bounding rectangle failed the
     * search predicate so neither it nor any descendant is visited. This is
     * distinct from a node that merely has not been reached yet while demand
     * is exhausted.
     *
     * @param node
     *            the node whose MBR failed the predicate
     * @param depth
     *            tree depth at which pruning occurred
     */
    void onPrune(Node<T, S> node, int depth) {
    }

    /**
     * A leaf entry's geometry satisfied the predicate and the entry is being
     * delivered to {@code onNext}. One demand unit is consumed per hit.
     *
     * @param entry
     *            the matched entry
     */
    void onLeafHit(Entry<T, S> entry) {
    }

    /**
     * The producer received a demand signal through {@code Producer.request}.
     * Every call is reported with the raw argument including zero and
     * negative values; only positive demand drives traversal.
     *
     * @param n
     *            raw request amount
     */
    void onRequest(long n) {
    }

    /**
     * Traversal stopped because the subscriber was unsubscribed. Reported at
     * most once per subscription.
     */
    void onCancel() {
    }

    /**
     * Traversal finished with the stack exhausted and the subscriber still
     * active, so {@code onCompleted} was (or is about to be) delivered.
     */
    void onComplete() {
    }

    /**
     * A predicate evaluation or subscriber callback terminated traversal with
     * an exception that was forwarded to {@code onError}.
     *
     * @param error
     *            the forwarded throwable
     */
    void onError(Throwable error) {
    }

    private boolean cancelled;

    /** Invokes {@link #onCancel()} at most once per subscription. */
    final void cancelOnce() {
        if (!cancelled) {
            cancelled = true;
            onCancel();
        }
    }
}
