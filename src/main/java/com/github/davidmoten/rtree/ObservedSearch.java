package com.github.davidmoten.rtree;

import com.github.davidmoten.rtree.geometry.Geometry;

import rx.Subscriber;
import rx.functions.Func1;

/**
 * Package-private instrumentation of the no-backpressure fast path used by
 * {@link OnSubscribeSearch.SearchProducer#requestAll()}.
 *
 * <p>
 * This mirrors {@code LeafHelper.search} and {@code NonLeafHelper.search} step
 * for step &mdash; the MBR gate at node entry, cancellation between every
 * element, the leaf entry condition and {@code onNext} &mdash; while emitting
 * the same event vocabulary as the backpressure path in {@link Backpressure}.
 * It is not part of the public API and is only used when a subscription is
 * created with a {@link SearchObserver}.
 *
 * <p>
 * The root is pushed once by the {@code SearchProducer} constructor for both
 * paths. In the fast path every entered node (root included) is gated by its
 * own MBR, so this is the only path where a depth-0 {@code prune} can occur.
 */
final class ObservedSearch {

    private ObservedSearch() {
        // prevent instantiation
    }

    static <T, S extends Geometry> void searchWithoutBackpressure(Node<T, S> node,
            Func1<? super Geometry, Boolean> condition,
            Subscriber<? super Entry<T, S>> subscriber, SearchObserver observer) {
        searchNode(node, condition, subscriber, observer, 0);
    }

    private static <T, S extends Geometry> void searchNode(Node<T, S> node,
            Func1<? super Geometry, Boolean> condition,
            Subscriber<? super Entry<T, S>> subscriber, SearchObserver observer, int depth) {
        // Mirrors LeafHelper.search / NonLeafHelper.search: the entered node
        // gates on its own MBR, including the root; this is therefore the only
        // path where a depth-0 prune can be observed.
        if (!condition.call(node.geometry().mbr())) {
            observer.prune(node, depth);
            return;
        }
        if (node instanceof NonLeaf) {
            NonLeaf<T, S> nonLeaf = (NonLeaf<T, S>) node;
            for (int i = 0; i < nonLeaf.count(); i++) {
                if (subscriber.isUnsubscribed()) {
                    return;
                }
                Node<T, S> child = nonLeaf.child(i);
                // the child performs its own MBR gate on entry below, matching
                // NonLeafHelper which only checks isUnsubscribed here
                searchEnteredChild(child, condition, subscriber, observer, depth + 1);
            }
        } else {
            Leaf<T, S> leaf = (Leaf<T, S>) node;
            for (int i = 0; i < leaf.count(); i++) {
                if (subscriber.isUnsubscribed()) {
                    return;
                }
                Entry<T, S> entry = leaf.entry(i);
                boolean matches = condition.call(entry.geometry());
                observer.entryTested(entry, matches);
                if (matches) {
                    subscriber.onNext(entry);
                    observer.hit(entry);
                }
            }
        }
    }

    private static <T, S extends Geometry> void searchEnteredChild(Node<T, S> child,
            Func1<? super Geometry, Boolean> condition,
            Subscriber<? super Entry<T, S>> subscriber, SearchObserver observer, int depth) {
        if (condition.call(child.geometry().mbr())) {
            observer.push(child, depth);
            searchNode(child, condition, subscriber, observer, depth);
        } else {
            observer.prune(child, depth);
        }
    }

}
