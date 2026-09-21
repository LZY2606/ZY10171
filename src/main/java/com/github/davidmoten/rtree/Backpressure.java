package com.github.davidmoten.rtree;

import com.github.davidmoten.rtree.geometry.Geometry;
import com.github.davidmoten.rtree.internal.util.ImmutableStack;

import rx.Subscriber;
import rx.functions.Func1;

/**
 * Utility methods for controlling backpressure of the tree search.
 */
final class Backpressure {

    private Backpressure() {
        // prevent instantiation
    }

    static <T, S extends Geometry> ImmutableStack<NodePosition<T, S>> search(
            final Func1<? super Geometry, Boolean> condition,
            final Subscriber<? super Entry<T, S>> subscriber,
            final ImmutableStack<NodePosition<T, S>> stack, final long request) {
        return search(condition, subscriber, stack, request, SearchObserver.<T, S>noop());
    }

    static <T, S extends Geometry> ImmutableStack<NodePosition<T, S>> search(
            final Func1<? super Geometry, Boolean> condition,
            final Subscriber<? super Entry<T, S>> subscriber,
            final ImmutableStack<NodePosition<T, S>> stack, final long request,
            final SearchObserver<T, S> observer) {
        StackAndRequest<NodePosition<T, S>> state = StackAndRequest.create(stack, request);
        return searchAndReturnStack(condition, subscriber, state, observer);
    }

    private static <S extends Geometry, T> ImmutableStack<NodePosition<T, S>> searchAndReturnStack(
            final Func1<? super Geometry, Boolean> condition,
            final Subscriber<? super Entry<T, S>> subscriber,
            StackAndRequest<NodePosition<T, S>> state, final SearchObserver<T, S> observer) {

        while (!state.stack.isEmpty()) {
            NodePosition<T, S> np = state.stack.peek();
            if (subscriber.isUnsubscribed()) {
                observer.cancelOnce();
                return ImmutableStack.empty();
            } else if (state.request <= 0)
                // demand exhausted: nodes still on the stack are not pruned,
                // they simply have not been visited yet
                return state.stack;
            else if (np.position() == np.node().count()) {
                // handle after last in node
                state = StackAndRequest.create(searchAfterLastInNode(state.stack), state.request);
            } else if (np.node() instanceof NonLeaf) {
                // handle non-leaf
                state = StackAndRequest.create(searchNonLeaf(condition, state.stack, np, observer),
                        state.request);
            } else {
                // handle leaf
                state = searchLeaf(condition, subscriber, state, np, observer);
            }
        }
        return state.stack;
    }

    private static class StackAndRequest<T> {
        private final ImmutableStack<T> stack;
        private final long request;

        StackAndRequest(ImmutableStack<T> stack, long request) {
            this.stack = stack;
            this.request = request;
        }

        static <T> StackAndRequest<T> create(ImmutableStack<T> stack, long request) {
            return new StackAndRequest<T>(stack, request);
        }

    }

    private static <T, S extends Geometry> StackAndRequest<NodePosition<T, S>> searchLeaf(
            final Func1<? super Geometry, Boolean> condition,
            final Subscriber<? super Entry<T, S>> subscriber,
            StackAndRequest<NodePosition<T, S>> state, NodePosition<T, S> np,
            final SearchObserver<T, S> observer) {
        final long nextRequest;
        Entry<T, S> entry = ((Leaf<T, S>) np.node()).entry(np.position());
        if (condition.call(entry.geometry())) {
            observer.onLeafHit(entry);
            subscriber.onNext(entry);
            nextRequest = state.request - 1;
        } else
            // a non-matching entry does not consume demand
            nextRequest = state.request;
        return StackAndRequest.create(state.stack.pop().push(np.nextPosition()), nextRequest);
    }

    private static <S extends Geometry, T> ImmutableStack<NodePosition<T, S>> searchNonLeaf(
            final Func1<? super Geometry, Boolean> condition,
            ImmutableStack<NodePosition<T, S>> stack, NodePosition<T, S> np,
            final SearchObserver<T, S> observer) {
        Node<T, S> child = ((NonLeaf<T, S>) np.node()).child(np.position());
        boolean accepted = condition.call(child.geometry());
        if (accepted) {
            stack = stack.push(new NodePosition<T, S>(child, 0));
            if (observer.enabled()) {
                observer.onPush(child, depth(stack));
            }
        } else {
            if (observer.enabled()) {
                observer.onPrune(child, depth(stack) + 1);
            }
            stack = stack.pop().push(np.nextPosition());
        }
        return stack;
    }

    private static <S extends Geometry, T> ImmutableStack<NodePosition<T, S>> searchAfterLastInNode(
            ImmutableStack<NodePosition<T, S>> stack) {
        ImmutableStack<NodePosition<T, S>> stack2 = stack.pop();
        if (stack2.isEmpty())
            stack = stack2;
        else {
            NodePosition<T, S> previous = stack2.peek();
            stack = stack2.pop().push(previous.nextPosition());
        }
        return stack;
    }

    /**
     * Returns the depth of the node at the top of the stack. A stack holding
     * only the root position reports depth 0. Only invoked when an observer is
     * enabled so production traversal never walks the stack.
     */
    private static <T, S extends Geometry> int depth(
            ImmutableStack<NodePosition<T, S>> stack) {
        int depth = -1;
        for (@SuppressWarnings("unused")
        NodePosition<T, S> np : stack) {
            depth++;
        }
        // the pushed child sits one level deeper than the position of its parent
        return depth;
    }

}
