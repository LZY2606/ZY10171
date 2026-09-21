package com.github.davidmoten.rtree;

import com.github.davidmoten.rtree.geometry.Geometry;
import com.github.davidmoten.rtree.internal.util.ImmutableStack;

import rx.Producer;

/**
 * Package-private test support bridging a captured {@link Producer} back to the
 * package-private {@code SearchProducer} state used by the traversal tests.
 */
final class OnSubscribeSearchAccess {

    private OnSubscribeSearchAccess() {
        // prevent instantiation
    }

    @SuppressWarnings("unchecked")
    static ImmutableStack<?> stackOf(Producer producer) {
        return ((OnSubscribeSearch.SearchProducer<?, ? extends Geometry>) producer)
                .stackForTesting();
    }

    @SuppressWarnings("unchecked")
    static Node<?, ?> rootNodeOf(Producer producer) {
        return ((OnSubscribeSearch.SearchProducer<?, ? extends Geometry>) producer)
                .rootNodeForTesting();
    }

}
