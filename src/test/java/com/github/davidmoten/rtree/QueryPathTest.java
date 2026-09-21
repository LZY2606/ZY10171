package com.github.davidmoten.rtree;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

import com.github.davidmoten.rtree.QueryPathFixtures.FixedTree;
import com.github.davidmoten.rtree.QueryPathFixtures.SharedTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Geometry;
import com.github.davidmoten.rtree.geometry.Intersects;
import com.github.davidmoten.rtree.geometry.Point;
import com.github.davidmoten.rtree.geometry.Rectangle;
import com.github.davidmoten.rtree.internal.util.ImmutableStack;

import rx.Subscriber;
import rx.functions.Func1;

/**
 * Query-path anatomy tests. Traversal is verified through recorded
 * {@link SearchTrace} events, never inferred from the final result set.
 *
 * <p>
 * Slow-path tests construct {@code SearchProducer} directly so that RxJava's
 * {@code SafeSubscriber} (which implicitly requests {@code Long.MAX_VALUE} in
 * {@code onStart}) cannot turn demand stepping into the fast path. Everything
 * is synchronous and free of sleeps or clocks.
 * </p>
 */
public class QueryPathTest {

    private static final Rectangle FULL = Geometries.rectangle(0, 0, 10, 3);
    private static final Rectangle LEFT_ONLY = Geometries.rectangle(0, 0, 1, 1);

    /** Records emissions; tests drive demand explicitly. */
    static class Rec extends Subscriber<Entry<String, Point>> {
        final List<String> values = new ArrayList<String>();
        volatile boolean completed;
        volatile Throwable error;
        boolean cancelAfterFirstHit;
        boolean cancelAfterSecondHit;
        boolean throwOnNext;

        @Override
        public void onStart() {
            // request nothing: tests drive demand explicitly
        }

        void demand(long n) {
            request(n);
        }

        @Override
        public void onCompleted() {
            completed = true;
        }

        @Override
        public void onError(Throwable e) {
            error = e;
        }

        @Override
        public void onNext(Entry<String, Point> entry) {
            values.add(entry.value());
            if (throwOnNext) {
                throw new IllegalStateException("onNext boom");
            }
            if (cancelAfterSecondHit && values.size() == 2) {
                unsubscribe();
            } else if (cancelAfterFirstHit) {
                unsubscribe();
            }
        }
    }

    private static final Func1<Geometry, Boolean> intersects(Rectangle r) {
        return RTree.intersects(r);
    }

    private static OnSubscribeSearch.SearchProducer<String, Point> producer(
            FixedTree fixture, SearchTrace<String, Point> trace,
            Subscriber<Entry<String, Point>> sub, Func1<Geometry, Boolean> condition) {
        return new OnSubscribeSearch.SearchProducer<String, Point>(fixture.root, condition, sub,
                trace);
    }

    private static void assertDescriptions(SearchTrace<String, Point> trace,
            List<String> expected) {
        assertEquals('\n' + trace.render(), expected, trace.descriptions());
    }

    private static List<String> structure(SearchTrace<String, Point> trace) {
        List<String> out = new ArrayList<String>();
        for (String description : trace.descriptions()) {
            if (!description.startsWith("request(")) {
                out.add(description);
            }
        }
        return out;
    }

    // ---- rectangle: MBR pruning, demand pausing, unvisited distinction ----

    @Test
    public void testRectangleSearchPrunesDisjointLeafAndPausesOnDemand() {
        FixedTree fixture = QueryPathFixtures.fixedTree();
        SearchTrace<String, Point> trace = new SearchTrace<String, Point>(fixture.namer());
        Rec sub = new Rec();
        OnSubscribeSearch.SearchProducer<String, Point> p =
                producer(fixture, trace, sub, intersects(LEFT_ONLY));

        assertDescriptions(trace, Collections.singletonList("push(root)@0"));
        p.request(1);
        // leafA pushed, a hits and consumes the single demand; leafB has not
        // been touched at all (neither pushed nor pruned)
        assertDescriptions(trace, Arrays.asList(
                "push(root)@0", "request(1)@-1", "push(leafA)@1", "leafHit(a)@-1"));
        assertFalse(sub.completed);

        p.request(1);
        // b(2,1) is outside [0,0:1,1]: tested, misses, does not consume demand;
        // leafB MBR is disjoint so it is explicitly pruned; traversal completes
        assertDescriptions(trace, Arrays.asList(
                "push(root)@0", "request(1)@-1", "push(leafA)@1", "leafHit(a)@-1",
                "request(1)@-1", "prune(leafB)@1", "complete@-1"));
        assertEquals(Arrays.asList("a"), sub.values);
        assertTrue(sub.completed);
    }

    @Test
    public void testFastAndSlowPathsReportIdenticalStructuralEvents() {
        FixedTree fixture = QueryPathFixtures.fixedTree();
        SearchTrace<String, Point> slow = new SearchTrace<String, Point>(fixture.namer());
        SearchTrace<String, Point> fast = new SearchTrace<String, Point>(fixture.namer());
        Rec slowSub = new Rec();
        Rec fastSub = new Rec();

        OnSubscribeSearch.SearchProducer<String, Point> sp =
                producer(fixture, slow, slowSub, intersects(FULL));
        sp.request(1);
        sp.request(1);
        sp.request(1);
        sp.request(1);
        // the fourth hit exactly exhausts demand; completion of the drained
        // stack is delivered when the next demand signal arrives (same drain
        // ordering as RxJava's OnSubscribeFromIterable)
        assertFalse(slowSub.completed);
        sp.request(1);
        assertTrue(slowSub.completed);

        OnSubscribeSearch.SearchProducer<String, Point> fp =
                producer(fixture, fast, fastSub, intersects(FULL));
        fp.request(Long.MAX_VALUE);

        assertEquals(slowSub.values, fastSub.values);
        assertEquals(Arrays.asList("a", "b", "d", "e"), fastSub.values);
        List<String> expected = Arrays.asList(
                "push(root)@0", "push(leafA)@1", "leafHit(a)@-1", "leafHit(b)@-1",
                "push(leafB)@1", "leafHit(d)@-1", "leafHit(e)@-1", "complete@-1");
        assertEquals('\n' + slow.render(), expected, structure(slow));
        assertEquals('\n' + fast.render(), expected, structure(fast));
        // only the fast path reports a single unbounded demand signal
        assertEquals(Collections.singletonList(
                "request(9223372036854775807)@-1"),
                fast.descriptions().subList(1, 2));
    }

    // ---- point: single containing leaf ----

    @Test
    public void testPointSearchPrunesNonContainingLeafFastPath() {
        FixedTree fixture = QueryPathFixtures.fixedTree();
        SearchTrace<String, Point> trace = new SearchTrace<String, Point>(fixture.namer());
        Rec sub = new Rec();
        producer(fixture, trace, sub, RTree.intersects(Geometries.point(8, 2)))
                .request(Long.MAX_VALUE);

        assertDescriptions(trace, Arrays.asList(
                "push(root)@0", "request(9223372036854775807)@-1",
                "prune(leafA)@1", "push(leafB)@1", "leafHit(d)@-1", "complete@-1"));
        assertEquals(Arrays.asList("d"), sub.values);
    }

    // ---- circle: MBR impossibility vs exact geometry miss ----

    @Test
    public void testCirclePublicSearchFiltersExactGeometryAfterMbrTraversal() {
        FixedTree fixture = QueryPathFixtures.fixedTree();
        final List<String> values = new ArrayList<String>();
        // circle centre (2,2), radius 1: b(2,1) lies exactly on the boundary
        // (intersects), a(0,0) at sqrt(5) is an exact-geometry miss inside an
        // MBR-overlapping leaf, the leafB branch is impossible by MBR distance
        fixture.tree.search(Geometries.circle(2, 2, 1), Intersects.geometryIntersectsCircle)
                .subscribe(new Subscriber<Entry<String, Point>>() {
                    @Override
                    public void onStart() {
                        request(Long.MAX_VALUE);
                    }

                    @Override
                    public void onCompleted() {
                    }

                    @Override
                    public void onError(Throwable e) {
                        throw new AssertionError(e);
                    }

                    @Override
                    public void onNext(Entry<String, Point> entry) {
                        values.add(entry.value());
                    }
                });
        assertEquals(Arrays.asList("b"), values);
    }

    @Test
    public void testMbrOverlapVisitsLeafWhilePointMissIsNeitherHitNorPrune() {
        FixedTree fixture = QueryPathFixtures.fixedTree();
        SearchTrace<String, Point> trace = new SearchTrace<String, Point>(fixture.namer());
        Rec sub = new Rec();
        // MBR box of the circle [1,1:3,3] overlaps leafA but contains only b;
        // a is tested and missed without consuming demand
        producer(fixture, trace, sub, intersects(Geometries.rectangle(1, 1, 3, 3)))
                .request(Long.MAX_VALUE);
        assertDescriptions(trace, Arrays.asList(
                "push(root)@0", "request(9223372036854775807)@-1",
                "push(leafA)@1", "leafHit(b)@-1", "prune(leafB)@1", "complete@-1"));
        assertEquals(Arrays.asList("b"), sub.values);
    }

    // ---- nearest ordering, ties, maxDistance ----

    @Test
    public void testNearestReturnsAscendingDistanceAndTraversesBothLeaves() {
        FixedTree fixture = QueryPathFixtures.fixedTree();
        SearchTrace<String, Point> trace = new SearchTrace<String, Point>(fixture.namer());
        Rec sub = new Rec();
        // nearest requests its upstream unbounded: underlying walk is fast path
        producer(fixture, trace, sub,
                g -> g.distance(Geometries.rectangle(0, 0, 0, 0)) < 100).request(Long.MAX_VALUE);
        // apply the same bounded-priority ordering nearest() would
        List<String> ordered = new ArrayList<String>();
        for (Entry<String, Point> entry : fixture.tree
                .nearest(Geometries.rectangle(0, 0, 0, 0), 100, 4)
                .toList().toBlocking().single()) {
            ordered.add(entry.value());
        }
        assertEquals(Arrays.asList("a", "b", "d", "e"), ordered);
        assertDescriptions(trace, Arrays.asList(
                "push(root)@0", "request(9223372036854775807)@-1",
                "push(leafA)@1", "leafHit(a)@-1", "leafHit(b)@-1",
                "push(leafB)@1", "leafHit(d)@-1", "leafHit(e)@-1", "complete@-1"));
    }

    @Test
    public void testNearestMaxCountHonouredWithoutAssertingTieOrder() {
        FixedTree fixture = QueryPathFixtures.fixedTree();
        final List<String> values = new ArrayList<String>();
        fixture.tree.nearest(Geometries.rectangle(0, 0, 0, 0), 100, 2)
                .subscribe(new Subscriber<Entry<String, Point>>() {
                    @Override
                    public void onStart() {
                        request(Long.MAX_VALUE);
                    }

                    @Override
                    public void onCompleted() {
                    }

                    @Override
                    public void onError(Throwable e) {
                        throw new AssertionError(e);
                    }

                    @Override
                    public void onNext(Entry<String, Point> entry) {
                        values.add(entry.value());
                    }
                });
        assertEquals("maxCount bounds emissions exactly", 2, values.size());
        // strict distance ordering is guaranteed; tie identity order is not
        double previous = -1;
        for (String value : values) {
            Point p = value.equals("a") ? Geometries.point(0, 0) : Geometries.point(2, 1);
            double distance = p.distance(Geometries.rectangle(0, 0, 0, 0));
            assertTrue(distance >= previous);
            previous = distance;
        }
    }

    @Test
    public void testNearestWithAllTiesDoesNotCommitToGlobalOrder() {
        FixedTree fixture = QueryPathFixtures.tieTree();
        final List<String> values = new ArrayList<String>();
        fixture.tree.nearest(Geometries.rectangle(0, 0, 0, 0), 100, 2)
                .subscribe(new Subscriber<Entry<String, Point>>() {
                    @Override
                    public void onStart() {
                        request(Long.MAX_VALUE);
                    }

                    @Override
                    public void onCompleted() {
                    }

                    @Override
                    public void onError(Throwable e) {
                        throw new AssertionError(e);
                    }

                    @Override
                    public void onNext(Entry<String, Point> entry) {
                        values.add(entry.value());
                    }
                });
        // exactly maxCount entries, each a valid tied candidate; no order claim
        assertEquals(2, values.size());
        assertTrue(values.toString(),
                Arrays.asList("w", "x", "y", "z").containsAll(values));
        assertEquals(2, new HashSet<String>(values).size());
    }

    @Test
    public void testMaxDistanceIsStrictAndPrunesByMbrDistance() {
        FixedTree fixture = QueryPathFixtures.fixedTree();
        SearchTrace<String, Point> trace = new SearchTrace<String, Point>(fixture.namer());
        Rec sub = new Rec();
        Point origin = Geometries.point(0, 0);
        producer(fixture, trace, sub, g -> g.distance(origin.mbr()) < Math.sqrt(5.0))
                .request(Long.MAX_VALUE);
        // a passes; b is exactly sqrt(5) away and excluded by strict <; leafB
        // is pruned on MBR distance without ever testing its entries
        assertDescriptions(trace, Arrays.asList(
                "push(root)@0", "request(9223372036854775807)@-1",
                "push(leafA)@1", "leafHit(a)@-1", "prune(leafB)@1", "complete@-1"));
        assertEquals(Arrays.asList("a"), sub.values);
    }

    @Test
    public void testEntryOutsideBoundInVisitedLeafDoesNotConsumeDemand() {
        FixedTree fixture = QueryPathFixtures.fixedTree();
        SearchTrace<String, Point> trace = new SearchTrace<String, Point>(fixture.namer());
        Rec sub = new Rec();
        Point origin = Geometries.point(0, 0);
        OnSubscribeSearch.SearchProducer<String, Point> p =
                producer(fixture, trace, sub, g -> g.distance(origin.mbr()) < 1.0);
        p.request(1);
        // a consumes the only demand so the walk pauses immediately (it checks
        // the remaining budget before moving on); b was never tested in this
        // request and leafB is still unvisited, not pruned
        assertDescriptions(trace, Arrays.asList(
                "push(root)@0", "request(1)@-1",
                "push(leafA)@1", "leafHit(a)@-1"));
        assertFalse(sub.completed);
        // the next request re-enters with zero hits outstanding: b misses
        // without consuming demand, leafB is pruned by MBR distance and the
        // empty stack triggers completion
        p.request(1);
        assertDescriptions(trace, Arrays.asList(
                "push(root)@0", "request(1)@-1", "push(leafA)@1", "leafHit(a)@-1",
                "request(1)@-1", "prune(leafB)@1", "complete@-1"));
        assertEquals(Arrays.asList("a"), sub.values);
        assertTrue(sub.completed);
    }

    // ---- cancellation: no onNext after cancel, both engines ----

    @Test
    public void testSlowPathCancelStopsTraversalAndReportsSingleCancel() {
        FixedTree fixture = QueryPathFixtures.fixedTree();
        SearchTrace<String, Point> trace = new SearchTrace<String, Point>(fixture.namer());
        Rec sub = new Rec();
        sub.cancelAfterSecondHit = true;
        producer(fixture, trace, sub, intersects(FULL)).request(10);

        assertEquals(Arrays.asList("a", "b"), sub.values);
        assertFalse(sub.completed);
        assertNull(sub.error);
        List<String> descriptions = trace.descriptions();
        assertEquals(trace.render(), "cancel@-1",
                descriptions.get(descriptions.size() - 1));
        int cancels = 0;
        for (SearchTrace.Event event : trace.events()) {
            if (event instanceof SearchTrace.Cancel) {
                cancels++;
            }
        }
        assertEquals("cancel reported at most once", 1, cancels);
    }

    @Test
    public void testFastPathCancelAfterFirstHitSuppressesFurtherEmission() {
        FixedTree fixture = QueryPathFixtures.fixedTree();
        SearchTrace<String, Point> trace = new SearchTrace<String, Point>(fixture.namer());
        final List<String> values = new ArrayList<String>();
        producer(fixture, trace, new Subscriber<Entry<String, Point>>() {
            @Override
            public void onStart() {
            }

            @Override
            public void onCompleted() {
                fail("unsubscribe must suppress completion");
            }

            @Override
            public void onError(Throwable e) {
                throw new AssertionError(e);
            }

            @Override
            public void onNext(Entry<String, Point> entry) {
                values.add(entry.value());
                unsubscribe();
            }
        }, intersects(FULL)).request(Long.MAX_VALUE);

        assertEquals(trace.render(), Collections.singletonList("a"), values);
        assertEquals("cancel@-1",
                trace.descriptions().get(trace.descriptions().size() - 1));
    }

    // ---- request edges: zero, negative, repeat, surplus demand ----

    @Test
    public void testZeroAndNegativeRequestsAreRecordedButNeverTraverse() {
        FixedTree fixture = QueryPathFixtures.fixedTree();
        SearchTrace<String, Point> trace = new SearchTrace<String, Point>(fixture.namer());
        Rec sub = new Rec();
        OnSubscribeSearch.SearchProducer<String, Point> p =
                producer(fixture, trace, sub, intersects(LEFT_ONLY));
        p.request(0);
        p.request(-7);
        assertDescriptions(trace, Arrays.asList(
                "push(root)@0", "request(0)@-1", "request(-7)@-1"));
        assertEquals(Collections.emptyList(), sub.values);
        assertFalse(sub.completed);

        p.request(1);
        assertEquals(Arrays.asList("a"), sub.values);
        p.request(1);
        assertTrue(sub.completed);
    }

    @Test
    public void testRepeatedOneRequestsWalkFullTreeAndCompleteOnce() {
        FixedTree fixture = QueryPathFixtures.fixedTree();
        SearchTrace<String, Point> trace = new SearchTrace<String, Point>(fixture.namer());
        Rec sub = new Rec();
        OnSubscribeSearch.SearchProducer<String, Point> p =
                producer(fixture, trace, sub, intersects(FULL));
        for (int i = 0; i < 4; i++) {
            p.request(1);
        }
        assertEquals(Arrays.asList("a", "b", "d", "e"), sub.values);
        assertFalse("drain loop defers completion when the last hit exhausts demand",
                sub.completed);
        // any subsequent demand signal observes the empty stack and completes
        p.request(1);
        assertTrue(sub.completed);
        int completes = 0;
        for (SearchTrace.Event event : trace.events()) {
            if (event instanceof SearchTrace.Complete) {
                completes++;
            }
        }
        assertEquals(1, completes);
    }

    @Test
    public void testSurplusDemandDoesNotReEnterTree() {
        FixedTree fixture = QueryPathFixtures.fixedTree();
        SearchTrace<String, Point> trace = new SearchTrace<String, Point>(fixture.namer());
        Rec sub = new Rec();
        OnSubscribeSearch.SearchProducer<String, Point> p =
                producer(fixture, trace, sub, intersects(FULL));
        p.request(1);
        p.request(100); // far more than the three remaining entries
        assertEquals(Arrays.asList("a", "b", "d", "e"), sub.values);
        assertTrue(sub.completed);
        assertEquals(1, count(trace, SearchTrace.Complete.class));
    }

    @Test
    public void testPausedTreeHasUnvisitedLeafNeitherPushedNorPruned() {
        FixedTree fixture = QueryPathFixtures.fixedTree();
        SearchTrace<String, Point> trace = new SearchTrace<String, Point>(fixture.namer());
        Rec sub = new Rec();
        OnSubscribeSearch.SearchProducer<String, Point> p =
                producer(fixture, trace, sub, intersects(FULL));
        p.request(2);
        // leafB must not appear anywhere: not pruned, not pushed, just paused
        assertFalse(trace.render(), trace.descriptions().toString().contains("leafB"));
        for (SearchTrace.Event event : trace.events()) {
            assertFalse(trace.render(), event instanceof SearchTrace.Prune);
        }
        p.request(2);
        assertEquals(Arrays.asList("a", "b", "d", "e"), sub.values);
    }

    @Test
    public void testRepeatedUnboundedRequestAfterCompletionDoesNotRestart() {
        FixedTree fixture = QueryPathFixtures.fixedTree();
        SearchTrace<String, Point> trace = new SearchTrace<String, Point>(fixture.namer());
        Rec sub = new Rec();
        OnSubscribeSearch.SearchProducer<String, Point> p =
                producer(fixture, trace, sub, intersects(FULL));
        p.request(Long.MAX_VALUE);
        p.request(Long.MAX_VALUE); // late duplicate from e.g. SafeSubscriber
        assertEquals(Arrays.asList("a", "b", "d", "e"), sub.values);
        assertEquals(1, count(trace, SearchTrace.Complete.class));
    }

    // ---- subscriber and predicate errors ----

    @Test
    public void testThrowingOnNextOnSlowPathRoutesToOnErrorAndStopsTraversal() {
        FixedTree fixture = QueryPathFixtures.fixedTree();
        SearchTrace<String, Point> trace = new SearchTrace<String, Point>(fixture.namer());
        Rec sub = new Rec();
        sub.throwOnNext = true;
        OnSubscribeSearch.SearchProducer<String, Point> p =
                producer(fixture, trace, sub, intersects(FULL));
        p.request(10);
        assertEquals(Collections.singletonList("a"), sub.values);
        assertNotNull(sub.error);
        List<String> descriptions = trace.descriptions();
        assertEquals(trace.render(), "error(IllegalStateException)@-1",
                descriptions.get(descriptions.size() - 1));
        // a later demand cannot resume a terminated traversal
        p.request(10);
        assertEquals(Collections.singletonList("a"), sub.values);
    }

    @Test
    public void testThrowingOnNextOnFastPathRoutesToOnError() {
        FixedTree fixture = QueryPathFixtures.fixedTree();
        SearchTrace<String, Point> trace = new SearchTrace<String, Point>(fixture.namer());
        Rec sub = new Rec();
        sub.throwOnNext = true;
        producer(fixture, trace, sub, intersects(FULL)).request(Long.MAX_VALUE);
        assertEquals(Collections.singletonList("a"), sub.values);
        assertNotNull(sub.error);
        assertTrue(trace.descriptions().toString(), trace.descriptions()
                .contains("error(IllegalStateException)@-1"));
    }

    @Test
    public void testThrowingPredicateRoutesToOnError() {
        FixedTree fixture = QueryPathFixtures.fixedTree();
        SearchTrace<String, Point> trace = new SearchTrace<String, Point>(fixture.namer());
        Rec sub = new Rec();
        ArithmeticException boom = new ArithmeticException("predicate");
        producer(fixture, trace, sub, g -> {
            throw boom;
        }).request(Long.MAX_VALUE);
        assertSame(boom, sub.error);
        assertTrue(trace.descriptions().toString(), trace.descriptions()
                .contains("error(ArithmeticException)@-1"));
    }

    // ---- traversal stack lifetime ----

    @SuppressWarnings("unchecked")
    private static ImmutableStack<NodePosition<String, Point>> stackOf(
            OnSubscribeSearch.SearchProducer<String, Point> producer) throws Exception {
        Field field = OnSubscribeSearch.SearchProducer.class.getDeclaredField("stack");
        field.setAccessible(true);
        return (ImmutableStack<NodePosition<String, Point>>) field.get(producer);
    }

    @Test
    public void testStackHeldWhilePausedAndReleasedOnCompletion() throws Exception {
        FixedTree fixture = QueryPathFixtures.fixedTree();
        SearchTrace<String, Point> trace = new SearchTrace<String, Point>(fixture.namer());
        Rec sub = new Rec();
        OnSubscribeSearch.SearchProducer<String, Point> p =
                producer(fixture, trace, sub, intersects(FULL));
        p.request(1);
        assertFalse(trace.render(), stackOf(p).isEmpty());
        p.request(10);
        assertNull("completed traversal must not retain the continuation stack",
                stackOf(p));
        assertTrue(sub.completed);
    }

    @Test
    public void testStackReleasedOnCancelAndOnError() throws Exception {
        FixedTree fixture = QueryPathFixtures.fixedTree();

        SearchTrace<String, Point> cancelTrace = new SearchTrace<String, Point>(fixture.namer());
        Rec cancelSub = new Rec();
        cancelSub.cancelAfterFirstHit = true;
        OnSubscribeSearch.SearchProducer<String, Point> cancelling =
                producer(fixture, cancelTrace, cancelSub, intersects(FULL));
        cancelling.request(10);
        assertNull(stackOf(cancelling));
        assertTrue(cancelTrace.descriptions().contains("cancel@-1"));

        SearchTrace<String, Point> errorTrace = new SearchTrace<String, Point>(fixture.namer());
        Rec errorSub = new Rec();
        errorSub.throwOnNext = true;
        OnSubscribeSearch.SearchProducer<String, Point> failing =
                producer(fixture, errorTrace, errorSub, intersects(FULL));
        failing.request(1);
        assertNull(stackOf(failing));
        assertNotNull(errorSub.error);
    }

    // ---- structural sharing and node identity ----

    @Test
    public void testAdditionSharesUntouchedSiblingAndRebuildsDescendedBranch() {
        SharedTree shared = QueryPathFixtures.sharedTree();
        Set<Node<String, Point>> afterNodes = SharedTree.collectNodes(shared.after);
        Set<Node<String, Point>> beforeNodes = SharedTree.collectNodes(shared.before);

        assertTrue("untouched sibling leaf keeps identity",
                afterNodes.contains(shared.rightLeaf));
        assertFalse("descended leaf is rebuilt",
                afterNodes.contains(shared.leftLeaf));
        assertFalse("root is rebuilt on add",
                afterNodes.contains(shared.before.root().get()));
        assertTrue(beforeNodes.contains(shared.leftLeaf));
    }

    @Test
    public void testQueryEventsCarrySharedNodeIdentityAcrossVersions() {
        SharedTree shared = QueryPathFixtures.sharedTree();
        AtomicReference<Node<String, Point>> beforeRight =
                new AtomicReference<Node<String, Point>>();
        AtomicReference<Node<String, Point>> afterRight =
                new AtomicReference<Node<String, Point>>();

        shared.before.search(intersects(Geometries.rectangle(8, 2, 10, 3)),
                identityObserver(shared.rightLeaf, beforeRight))
                .subscribe(fullDemand());
        shared.after.search(intersects(Geometries.rectangle(8, 2, 10, 3)),
                identityObserver(shared.rightLeaf, afterRight))
                .subscribe(fullDemand());

        assertSame(shared.rightLeaf, beforeRight.get());
        assertSame(shared.rightLeaf, afterRight.get());
    }

    private static SearchObserver<String, Point> identityObserver(
            final Node<String, Point> target,
            final AtomicReference<Node<String, Point>> sink) {
        return new SearchObserver<String, Point>() {
            @Override
            boolean enabled() {
                return true;
            }

            @Override
            void onPush(Node<String, Point> node, int depth) {
                if (node == target) {
                    sink.set(node);
                }
            }
        };
    }

    private static <T> Subscriber<Entry<T, Point>> fullDemand() {
        return new Subscriber<Entry<T, Point>>() {
            @Override
            public void onStart() {
                request(Long.MAX_VALUE);
            }

            @Override
            public void onCompleted() {
            }

            @Override
            public void onError(Throwable e) {
                throw new AssertionError(e);
            }

            @Override
            public void onNext(Entry<T, Point> t) {
            }
        };
    }

    @Test
    public void testEmptyTreeInstallsNoProducerAndEmitsNoStructuralEvents() {
        SearchTrace<Object, Geometry> trace = new SearchTrace<Object, Geometry>(node -> "x");
        RTree<Object, Geometry> empty = RTree.create();
        final AtomicInteger hits = new AtomicInteger();
        empty.search(g -> true, trace).subscribe(
                new Subscriber<Entry<Object, Geometry>>() {
                    @Override
                    public void onStart() {
                        request(Long.MAX_VALUE);
                    }

                    @Override
                    public void onCompleted() {
                    }

                    @Override
                    public void onError(Throwable e) {
                        throw new AssertionError(e);
                    }

                    @Override
                    public void onNext(Entry<Object, Geometry> t) {
                        hits.incrementAndGet();
                    }
                });
        assertEquals(0, hits.get());
        assertEquals(Collections.emptyList(), trace.events());
    }

    private static int count(SearchTrace<?, ?> trace, Class<?> type) {
        int count = 0;
        for (SearchTrace.Event event : trace.events()) {
            if (type.isInstance(event)) {
                count++;
            }
        }
        return count;
    }
}
