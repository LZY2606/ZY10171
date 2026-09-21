package com.github.davidmoten.rtree;

import java.util.concurrent.atomic.AtomicLong;

import com.github.davidmoten.guavamini.annotations.VisibleForTesting;
import com.github.davidmoten.rtree.geometry.Geometry;
import com.github.davidmoten.rtree.internal.util.ImmutableStack;

import rx.Observable.OnSubscribe;
import rx.Producer;
import rx.Subscriber;
import rx.functions.Func1;

final class OnSubscribeSearch<T, S extends Geometry> implements OnSubscribe<Entry<T, S>> {

    private final Node<T, S> node;
    private final Func1<? super Geometry, Boolean> condition;
    private final SearchObserver<T, S> observer;

    OnSubscribeSearch(Node<T, S> node, Func1<? super Geometry, Boolean> condition) {
        this(node, condition, SearchObserver.<T, S>noop());
    }

    OnSubscribeSearch(Node<T, S> node, Func1<? super Geometry, Boolean> condition,
            SearchObserver<T, S> observer) {
        this.node = node;
        this.condition = condition;
        this.observer = observer;
    }

    @Override
    public void call(Subscriber<? super Entry<T, S>> subscriber) {
        subscriber.setProducer(new SearchProducer<T, S>(node, condition, subscriber, observer));
    }

    @VisibleForTesting
    static class SearchProducer<T, S extends Geometry> implements Producer {

        private final Subscriber<? super Entry<T, S>> subscriber;
        private final Node<T, S> node;
        private final Func1<? super Geometry, Boolean> condition;
        private final SearchObserver<T, S> observer;
        private volatile ImmutableStack<NodePosition<T, S>> stack;
        private final AtomicLong requested = new AtomicLong(0);

        SearchProducer(Node<T, S> node, Func1<? super Geometry, Boolean> condition,
                Subscriber<? super Entry<T, S>> subscriber) {
            this(node, condition, subscriber, SearchObserver.<T, S>noop());
        }

        SearchProducer(Node<T, S> node, Func1<? super Geometry, Boolean> condition,
                Subscriber<? super Entry<T, S>> subscriber, SearchObserver<T, S> observer) {
            this.node = node;
            this.condition = condition;
            this.subscriber = subscriber;
            this.observer = observer;
            stack = ImmutableStack.create(new NodePosition<T, S>(node, 0));
            // the root is on the traversal stack from subscription time even
            // before any demand arrives
            if (observer.enabled()) {
                observer.onPush(node, 0);
            }
        }

        @Override
        public void request(long n) {
            observer.onRequest(n);
            try {
                if (n <= 0 || requested.get() == Long.MAX_VALUE) {
                    // none requested, or the no-backpressure fast path has
                    // already run (RxJava may deliver a second unbounded
                    // request after onCompleted); repeated demand after
                    // termination never restarts traversal
                    return;
                } else if (n == Long.MAX_VALUE && requested.compareAndSet(0, Long.MAX_VALUE)) {
                    // fast path
                    requestAll();
                } else {
                    requestSome(n);
                }
            } catch (RuntimeException e) {
                terminalError(e);
            }
        }

        private void terminalError(RuntimeException e) {
            // release the traversal stack so paused searches that fail do not
            // retain more of the immutable tree than the producer's root
            stack = null;
            observer.onError(e);
            subscriber.onError(e);
        }

        private void requestAll() {
            if (node instanceof NonLeaf || node instanceof Leaf) {
                // mirrors NonLeafHelper.search / LeafHelper.search: each node
                // tests its own MBR on entry
                if (condition.call(node.geometry())) {
                    searchFast(node, condition, subscriber, observer, 0);
                }
            } else {
                // custom Node implementations own their recursion (for example
                // the flatbuffers-backed nodes); delegate unchanged
                node.searchWithoutBackpressure(condition, subscriber);
            }
            stack = null;
            if (subscriber.isUnsubscribed()) {
                observer.cancelOnce();
            } else {
                observer.onComplete();
                subscriber.onCompleted();
            }
        }

        private void requestSome(long n) {
            // back pressure path
            // this algorithm copied roughly from
            // rxjava-core/OnSubscribeFromIterable.java

            // rxjava used AtomicLongFieldUpdater instead of AtomicLong
            // but benchmarks showed no benefit here so reverted to AtomicLong
            long previousCount = getAndAddRequest(requested, n);
            if (previousCount == 0) {
                // don't touch stack every time during the loop because
                // is a volatile and every write forces a thread memory
                // cache flush
                ImmutableStack<NodePosition<T, S>> st = stack;
                while (true) {
                    // minimize atomic reads by assigning to a variable here
                    long r = requested.get();
                    st = Backpressure.search(condition, subscriber, st, r, observer);
                    if (st.isEmpty()) {
                        // release some state for gc (although empty stack so not very significant)
                        stack = null;
                        if (subscriber.isUnsubscribed()) {
                            observer.cancelOnce();
                        } else {
                            observer.onComplete();
                            subscriber.onCompleted();
                        }
                        return;
                    } else {
                        stack = st;
                        if (requested.addAndGet(-r) == 0)
                            return;
                    }
                }

            }
        }
    }

    /**
     * Observed counterpart of the recursion performed by
     * {@code NonLeafHelper.search} and {@code LeafHelper.search} on the
     * no-backpressure fast path. The MBR of {@code node} is assumed to have
     * already passed the predicate by the caller (the producer for the root,
     * the child loop below for descendants), matching the original algorithm
     * where every node tests exactly its own MBR on entry. Nodes that are
     * neither {@link NonLeaf} nor {@link Leaf} are delegated to their own
     * {@link Node#searchWithoutBackpressure} implementation without emitting
     * deeper events.
     */
    private static <T, S extends Geometry> void searchFast(Node<T, S> node,
            final Func1<? super Geometry, Boolean> condition,
            final Subscriber<? super Entry<T, S>> subscriber,
            final SearchObserver<T, S> observer, final int depth) {
        if (node instanceof NonLeaf) {
            NonLeaf<T, S> nonLeaf = (NonLeaf<T, S>) node;
            for (int i = 0; i < nonLeaf.count(); i++) {
                if (subscriber.isUnsubscribed()) {
                    observer.cancelOnce();
                    return;
                }
                Node<T, S> child = nonLeaf.child(i);
                int childDepth = depth + 1;
                if (condition.call(child.geometry())) {
                    if (observer.enabled()) {
                        observer.onPush(child, childDepth);
                    }
                    searchFast(child, condition, subscriber, observer, childDepth);
                } else if (observer.enabled()) {
                    observer.onPrune(child, childDepth);
                }
            }
        } else if (node instanceof Leaf) {
            Leaf<T, S> leaf = (Leaf<T, S>) node;
            for (int i = 0; i < leaf.count(); i++) {
                if (subscriber.isUnsubscribed()) {
                    observer.cancelOnce();
                    return;
                }
                Entry<T, S> entry = leaf.entry(i);
                if (condition.call(entry.geometry())) {
                    observer.onLeafHit(entry);
                    subscriber.onNext(entry);
                }
            }
        } else {
            node.searchWithoutBackpressure(condition, subscriber);
        }
    }

    /**
     * Adds {@code n} to {@code requested} and returns the value prior to
     * addition once the addition is successful (uses CAS semantics). If
     * overflows then sets {@code requested} field to {@code Long.MAX_VALUE}.
     * 
     * @param requested
     *            atomic field updater for a request count
     * @param n
     *            the number of requests to add to the requested count
     * @return requested value just prior to successful addition
     */
    private static long getAndAddRequest(AtomicLong requested, long n) {
        // add n to field but check for overflow
        while (true) {
            long current = requested.get();
            long next = current + n;
            // check for overflow
            if (next < 0) {
                next = Long.MAX_VALUE;
            }
            if (requested.compareAndSet(current, next)) {
                return current;
            }
        }
    }

}
