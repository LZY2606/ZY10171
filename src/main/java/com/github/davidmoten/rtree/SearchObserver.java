package com.github.davidmoten.rtree;

/**
 * Test-only instrumentation hooks for the query traversal performed by
 * {@link OnSubscribeSearch} and {@link Backpressure}.
 *
 * <p>
 * This type is package private and deliberately NOT part of the public API. It
 * exists so that tests can record an executable, forward-played trace of a
 * search &mdash; node push, geometric prune, leaf hit/miss, request, cancel,
 * terminal and error events &mdash; without inferring the traversal order from
 * the final result set. Production code paths always use {@link #noop()}, so
 * observable behaviour and performance are unchanged for library users.
 */
abstract class SearchObserver {

    /**
     * A {@link NodePosition} was pushed onto the traversal stack. The root is
     * pushed once when the producer is created; each non-leaf child whose MBR
     * satisfied the query condition is pushed when its parent is descended.
     *
     * @param node
     *            node that entered the traversal stack
     * @param depth
     *            depth of the node in the tree (0 for the root)
     */
    void push(Node<?, ?> node, int depth) {
    }

    /**
     * A node was not descended into because its minimum bounding rectangle was
     * proven not to satisfy the query condition. A pruned subtree is
     * geometrically impossible to contain a hit; it must not be confused with a
     * subtree that has not been reached yet because of backpressure.
     *
     * @param node
     *            node whose MBR failed the condition
     * @param depth
     *            depth of the node in the tree (0 for the root)
     */
    void prune(Node<?, ?> node, int depth) {
    }

    /**
     * A leaf entry was tested against the query condition.
     *
     * @param entry
     *            entry that was evaluated
     * @param matches
     *            whether the entry geometry satisfied the condition
     */
    void entryTested(Entry<?, ?> entry, boolean matches) {
    }

    /**
     * A leaf entry satisfied the condition and was emitted with
     * {@code onNext}.
     *
     * @param entry
     *            emitted entry
     */
    void hit(Entry<?, ?> entry) {
    }

    /**
     * A {@code request(long)} arrived at the producer (including {@code 0} and
     * negative requests, which are terminal no-ops for that call).
     *
     * @param n
     *            number of items requested
     */
    void request(long n) {
    }

    /**
     * The traversal observed that the subscriber had unsubscribed and stopped
     * before the stack was exhausted.
     */
    void cancel() {
    }

    /**
     * The whole stack was exhausted and the subscriber received
     * {@code onCompleted}.
     */
    void complete() {
    }

    /**
     * The traversal terminated with {@code onError}.
     *
     * @param error
     *            error delivered to the subscriber
     */
    void error(Throwable error) {
    }

    static SearchObserver noop() {
        return NoopSearchObserver.INSTANCE;
    }

    private static final class NoopSearchObserver extends SearchObserver {
        static final SearchObserver INSTANCE = new NoopSearchObserver();
    }

}
