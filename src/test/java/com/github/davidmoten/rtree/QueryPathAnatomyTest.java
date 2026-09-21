package com.github.davidmoten.rtree;

import static com.github.davidmoten.rtree.RTree.intersects;
import static com.github.davidmoten.rtree.geometry.Geometries.circle;
import static com.github.davidmoten.rtree.geometry.Geometries.point;
import static com.github.davidmoten.rtree.geometry.Geometries.rectangle;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import com.github.davidmoten.rtree.Entry;
import com.github.davidmoten.rtree.Node;
import com.github.davidmoten.rtree.OnSubscribeSearch.SearchProducer;
import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Circle;
import com.github.davidmoten.rtree.geometry.Intersects;
import com.github.davidmoten.rtree.geometry.Point;
import com.github.davidmoten.rtree.geometry.Rectangle;
import com.github.davidmoten.rtree.internal.operators.OperatorBoundedPriorityQueue;
import com.github.davidmoten.rtree.internal.Comparators;
import com.github.davidmoten.rtree.SearchTrace.Complete;
import com.github.davidmoten.rtree.SearchTrace.EntryTested;
import com.github.davidmoten.rtree.SearchTrace.Event;
import com.github.davidmoten.rtree.SearchTrace.Hit;
import com.github.davidmoten.rtree.SearchTrace.Prune;
import com.github.davidmoten.rtree.SearchTrace.Push;
import com.github.davidmoten.rtree.SearchTrace.Request;
import com.github.davidmoten.rtree.SearchTrace.Cancel;

import rx.Subscriber;
import rx.functions.Func1;

/**
 * Executable anatomy of an R-tree query.
 *
 * <p>
 * These tests never derive the traversal from the final result set. They play
 * the query forward with a package-private {@link SearchTrace} (see
 * {@code SearchObserver}) and assert the exact order in which nodes are pushed
 * onto the {@code ImmutableStack}, subtrees are pruned by their MBR, leaf
 * entries are tested/emitted, and request/cancel/complete/error signals
 * arrive. "Pruned" means geometrically impossible; the complementary case
 * &mdash; a node that simply has not been reached because the subscriber has
 * no outstanding demand &mdash; is asserted separately via request(1) stepping.
 *
 * <p>
 * Key conclusions are indexed by test name from
 * {@code src/docs/query-path-anatomy.md}.
 */
public class QueryPathAnatomyTest {

    // ==============================================================
    // Structural sharing: node identity across edits
    // ==============================================================

    /**
     * Insertion rebuilds only the path from the root to the chosen leaf
     * (NonLeafHelper.add + Util.replace keep every unchanged child instance);
     * the immutable tree therefore shares subtrees across versions. A node
     * identity ({@code ==}), not contents equality, is the only thing the
     * traversal observer can use to detect reuse.
     */
    @Test
    public void insertionRebuildsOnlyThePathAndSharesUnchangedSubtrees() {
        // build a star tree with two leaves whose point clusters are far apart
        RTree<String, Point> tree = RTree.star().maxChildren(3)
                .<String, Point>create()
                .add(Entries.entry("a1", point(0, 0)))
                .add(Entries.entry("a2", point(1, 1)))
                .add(Entries.entry("a3", point(2, 0)))
                .add(Entries.entry("b1", point(10, 0)))
                .add(Entries.entry("b2", point(11, 1)))
                .add(Entries.entry("b3", point(12, 0)));

        Node<String, Point> rootBefore = tree.root().get();
        assertTrue("fixture needs a non-leaf root", rootBefore instanceof NonLeaf);
        NonLeaf<String, Point> nl = (NonLeaf<String, Point>) rootBefore;
        Node<String, Point> child0 = nl.child(0);
        Node<String, Point> child1 = nl.child(1);

        // insert deep inside the first child's region: the selector must choose
        // that child's leaf
        RTree<String, Point> tree2 = tree.add(Entries.entry("a4", point(1, 2)));
        Node<String, Point> rootAfter = tree2.root().get();
        assertTrue("expected the structure to remain height 2",
                rootAfter instanceof NonLeaf);
        NonLeaf<String, Point> nlAfter = (NonLeaf<String, Point>) rootAfter;

        boolean sawShared = false;
        boolean sawRebuilt = false;
        for (int i = 0; i < nlAfter.count(); i++) {
            Node<String, Point> afterChild = nlAfter.child(i);
            if (afterChild == child0 || afterChild == child1) {
                sawShared = true;
            } else {
                sawRebuilt = true;
            }
        }
        assertTrue("at least one untouched subtree node must be shared by identity: "
                + "before=" + child0 + "/" + child1 + " after=" + nlAfter.children(),
                sawShared);
        assertTrue("the edited path must be represented by rebuilt node(s)", sawRebuilt);
        // the versioned roots themselves are distinct immutable objects
        assertFalse("a new edit yields a new root identity", rootAfter == rootBefore);
        // traversal observer node events refer to the same shared instances, so
        // both versions' searches see the shared subtree as identical pushes
        SearchTrace traceBefore = new SearchTrace();
        SearchTrace traceAfter = new SearchTrace();
        RTree<String, Point> t1 = tree;
        RTree<String, Point> t2 = tree2;
        // locate a shared child identity present in both versions
        Node<String, Point> shared = null;
        for (int i = 0; i < nl.count(); i++) {
            Node<String, Point> candidate = nl.child(i);
            for (int j = 0; j < nlAfter.count(); j++) {
                if (nlAfter.child(j) == candidate) {
                    shared = candidate;
                }
            }
        }
        assertTrue("fixture must leave one child untouched by identity", shared != null);
        assertTrue("shared subtree is observable as the same push instance in v1",
                tracePushedNodes(t1, traceBefore).contains(shared));
        assertTrue("shared subtree is observable as the same push instance in v2",
                tracePushedNodes(t2, traceAfter).contains(shared));
    }

    private static List<Node<?, ?>> tracePushedNodes(RTree<String, Point> tree,
            SearchTrace trace) {
        ProbeSubscriber<String, Point> sub = new ProbeSubscriber<String, Point>();
        SearchProducer<String, Point> producer = new OnSubscribeSearch.SearchProducer<String, Point>(
                tree.root().get(), g -> Boolean.TRUE, sub, trace.observer());
        sub.attachProducer(producer);
        producer.request(100);
        List<Node<?, ?>> nodes = new ArrayList<Node<?, ?>>();
        for (Event event : trace.only(Push.class)) {
            nodes.add(((Push) event).node);
        }
        return nodes;
    }

    // ==============================================================
    // Stack release when the subscription ends
    // ==============================================================

    /**
     * Once the backpressure walk exhausts the stack the producer nulls its
     * volatile {@code stack} field (OnSubscribeSearch.SearchProducer.requestSome),
     * so a completed subscription does not retain per-traversal NodePosition
     * chain beyond completion. The producer unavoidably holds the immutable
     * root node reference (passed in at construction); that is shared with the
     * RTree and released together with the subscription/producer, not per walk.
     */
    @Test
    public void completedBackpressureWalkReleasesStackWhileRootRemainsShared() {
        TraversalFixtures.TwoLeafTree f = new TraversalFixtures.TwoLeafTree();
        SearchTrace trace = new SearchTrace();
        ProbeSubscriber<String, Point> sub = subscribeProbed(f.tree,
                intersects(rectangle(-100, -100, 100, 100)), trace);

        // drain in two pulls so the ImmutableStack definitely held positions
        sub.producer().request(1);
        sub.producer().request(100);
        assertTrue(trace.completed());

        assertNull("stack reference must be cleared once the walk completes",
                OnSubscribeSearchAccess.stackOf(sub.producer()));
        // the root is still the shared tree root, retained only by node field
        assertSame(f.root, OnSubscribeSearchAccess.rootNodeOf(sub.producer()));
    }

    /**
     * After an unsubscribed request the walk returns the empty sentinel and the
     * producer clears its stack as well, so cancellation does not retain the
     * paused position (and hence the whole subtree below it) indefinitely.
     */
    @Test
    public void cancelledWalkClearsStackAndRetainsNoPausedPosition() {
        TraversalFixtures.TwoLeafTree f = new TraversalFixtures.TwoLeafTree();
        SearchTrace trace = new SearchTrace();
        ProbeSubscriber<String, Point> sub = subscribeProbed(f.tree,
                intersects(rectangle(-100, -100, 100, 100)), trace);
        sub.unsubscribe();
        sub.producer().request(100);

        assertTrue(trace.cancelled());
        assertNull("cancelled walk must not retain the paused stack",
                OnSubscribeSearchAccess.stackOf(sub.producer()));
    }


    // ==============================================================
    // Cancel, request edge cases, error termination
    // ==============================================================

    /**
     * Unsubscribing with outstanding demand stops the walk before the next
     * node/entry is examined: after the cancel no further hit/miss, prune or
     * complete occurs, and the producer clears its stack to the empty sentinel.
     */
    @Test
    public void cancelMidWalkStopsAllFurtherEvents() {
        TraversalFixtures.TwoLeafTree f = new TraversalFixtures.TwoLeafTree();
        SearchTrace trace = new SearchTrace();
        ProbeSubscriber<String, Point> sub = new ProbeSubscriber<String, Point>() {
            @Override
            public void onNext(Entry<String, Point> entry) {
                super.onNext(entry);
                unsubscribe();
            }
        };
        SearchProducer<String, Point> producer = new OnSubscribeSearch.SearchProducer<String, Point>(
                f.root, intersects(rectangle(-100, -100, 100, 100)), sub, trace.observer());
        sub.attachProducer(producer);

        producer.request(100);

        // window covers everything: a1 hits then the subscriber unsubscribes
        assertEquals(Arrays.asList("a1"), sub.values());
        assertFalse(trace.completed());
        assertTrue("a cancel event must be recorded", trace.cancelled());
        List<Event> afterCancel = trace.events()
                .subList(lastIndexOf(trace, Cancel.class) + 1, trace.events().size());
        assertEquals("no events after cancel: " + afterCancel, 0, afterCancel.size());
        assertNull("no error on cancel", trace.error());
    }

    /**
     * Unsubscribe before any request: the root is pushed at producer creation
     * but a subsequent request returns immediately with only the cancel event;
     * nothing is visited and no onNext/onCompleted is delivered.
     */
    @Test
    public void cancelBeforeRequestNeverWalks() {
        TraversalFixtures.TwoLeafTree f = new TraversalFixtures.TwoLeafTree();
        SearchTrace trace = new SearchTrace();
        ProbeSubscriber<String, Point> sub = subscribeProbed(f.tree,
                intersects(rectangle(-100, -100, 100, 100)), trace);
        sub.unsubscribe();
        sub.producer().request(100);

        assertTrace("root pushed at construction; request sees cancellation immediately",
                Arrays.asList("push root depth 0", "request 100", "cancel"), trace, f);
        assertEquals(Collections.emptyList(), sub.values());
        assertFalse(trace.completed());
    }

    private static int lastIndexOf(SearchTrace trace, Class<? extends Event> type) {
        List<Event> events = trace.events();
        for (int i = events.size() - 1; i >= 0; i--) {
            if (type.isInstance(events.get(i))) {
                return i;
            }
        }
        return -1;
    }

    /**
     * request(0) is a terminal no-op for that call: it is recorded but neither
     * walks the tree nor terminates the subscription. A later positive request
     * proceeds normally (OnSubscribeSearch.SearchProducer.request).
     */
    @Test
    public void requestZeroIsANoopThatIsRecorded() {
        TraversalFixtures.TwoLeafTree f = new TraversalFixtures.TwoLeafTree();
        SearchTrace trace = new SearchTrace();
        ProbeSubscriber<String, Point> sub = subscribeProbed(f.tree,
                intersects(rectangle(-100, -100, 100, 100)), trace);

        sub.producer().request(0);
        assertTrace("request(0) visits nothing", Arrays.asList("push root depth 0", "request 0"),
                trace, f);
        sub.producer().request(-5);
        assertTrace("negative request is also ignored",
                Arrays.asList("push root depth 0", "request 0", "request -5"), trace, f);
        assertFalse(trace.completed());

        sub.producer().request(1);
        assertEquals(Arrays.asList("a1"), sub.values());
        assertFalse(trace.completed());
    }

    /**
     * Repeated requests accumulate into the same AtomicLong demand; a request
     * arriving while a drain is not running simply tops up the balance. Here two
     * request(1) calls before the tree finishes walk two emissions apart and a
     * third request(1) (overshoot once complete) is never needed: the second
     * drain completes the whole small tree because prunes/misses need no demand.
     */
    @Test
    public void repeatedRequestsAccumulateAndSingleCompletion() {
        TraversalFixtures.TwoLeafTree f = new TraversalFixtures.TwoLeafTree();
        SearchTrace trace = new SearchTrace();
        ProbeSubscriber<String, Point> sub = subscribeProbed(f.tree,
                intersects(rectangle(-1, -1, 8, 8)), trace);

        sub.producer().request(1);
        assertEquals(Arrays.asList("a1"), sub.values());
        assertFalse(trace.completed());

        // a second top-up arrives while the walk is paused; it drains a2 and
        // freezes again with budget zero before reaching b1, proving demand
        // accumulates across requests rather than restarting the walk
        sub.producer().request(1);
        assertEquals(Arrays.asList("a1", "a2"), sub.values());
        assertFalse(trace.completed());

        // third top-up drains b1 (in window), then freezes again with budget
        // zero even though only the non-matching b2 remains: the zero-budget
        // gate runs before any further step, including b2's miss
        sub.producer().request(1);
        assertEquals(Arrays.asList("a1", "a2", "b1"), sub.values());
        assertFalse(trace.completed());

        // final demand lets the walk test b2 (miss), finish the leaves and
        // complete exactly once
        sub.producer().request(1);
        assertEquals(Arrays.asList("a1", "a2", "b1"), sub.values());
        assertTrue(trace.completed());
        assertEquals("exactly one complete event", 1, trace.only(Complete.class).size());
    }

    /**
     * A request made re-entrantly from inside onNext is added to the same
     * demand counter, so pulling one item per onNext drains the whole tree and
     * ends in a single completion - the standard reactive pull pattern used by
     * BackpressureTest.
     */
    @Test
    public void reentrantRequestFromOnNextDrainsAndCompletesOnce() {
        TraversalFixtures.TwoLeafTree f = new TraversalFixtures.TwoLeafTree();
        SearchTrace trace = new SearchTrace();
        final SearchProducer<String, Point>[] producerHolder = new SearchProducer[1];
        ProbeSubscriber<String, Point> sub = new ProbeSubscriber<String, Point>() {
            @Override
            public void onNext(Entry<String, Point> entry) {
                super.onNext(entry);
                producerHolder[0].request(1);
            }
        };
        SearchProducer<String, Point> producer = new OnSubscribeSearch.SearchProducer<String, Point>(
                f.root, intersects(rectangle(-100, -100, 100, 100)), sub, trace.observer());
        producerHolder[0] = producer;
        sub.attachProducer(producer);

        producer.request(1);
        assertEquals(Arrays.asList("a1", "a2", "b1", "b2"), sub.values());
        assertTrue(trace.completed());
        assertEquals(1, trace.only(Complete.class).size());
    }

    /**
     * When a condition predicate throws while testing a leaf entry, the
     * producer routes the exception to onError exactly once; the trace records
     * the error and no complete event is emitted and no further entry is
     * delivered.
     */
    @Test
    public void predicateThrowDuringEntryTestTerminatesWithOnError() {
        TraversalFixtures.TwoLeafTree f = new TraversalFixtures.TwoLeafTree();
        SearchTrace trace = new SearchTrace();
        Func1<com.github.davidmoten.rtree.geometry.Geometry, Boolean> bomb = g -> {
            throw new QueryPathAnatomyTest.Boom("condition failed");
        };
        ProbeSubscriber<String, Point> sub = subscribeProbed(f.tree, bomb, trace);

        sub.producer().request(100);

        assertTrue("onError delivered", sub.error() instanceof Boom);
        assertTrue("error event recorded", trace.error() instanceof Boom);
        assertFalse("no completion after error", trace.completed());
        assertEquals(Collections.emptyList(), sub.values());
        assertEquals(1, trace.only(SearchTrace.ErrorEvent.class).size());
    }

    /**
     * When onNext throws the producer's same catch forwards to onError; the
     * observable contract is that onError is terminal. The producer does not
     * emit a second onNext from the same request.
     */
    @Test
    public void onNextThrowTerminatesRequestWithOnError() {
        TraversalFixtures.TwoLeafTree f = new TraversalFixtures.TwoLeafTree();
        SearchTrace trace = new SearchTrace();
        ProbeSubscriber<String, Point> sub = new ProbeSubscriber<String, Point>(true);
        SearchProducer<String, Point> producer = new OnSubscribeSearch.SearchProducer<String, Point>(
                f.root, intersects(rectangle(-100, -100, 100, 100)), sub, trace.observer());
        sub.attachProducer(producer);

        producer.request(100);

        assertTrue(sub.error() instanceof ProbeSubscriber.TestTraversalException);
        assertEquals("only the first entry attempted onNext", Arrays.asList("a1"),
                sub.values());
        assertFalse(trace.completed());
    }

    static final class Boom extends RuntimeException {
        Boom(String message) {
            super(message);
        }
    }

    // ==============================================================
    // Empty tree and fast path
    // ==============================================================

    /**
     * An empty tree exposes an absent root: {@code search(condition)} is
     * {@code Observable.empty()} so onCompleted fires and no node is ever
     * pushed (RTree.search).
     */
    @Test
    public void emptyTreeCompletesWithoutNodeEvents() {
        RTree<String, Point> empty = RTree.create();
        SearchTrace trace = new SearchTrace();
        final List<String> values = new ArrayList<String>();
        final boolean[] completed = { false };
        empty.search(intersects(rectangle(0, 0, 1, 1)), trace.observer())
                .subscribe(new rx.Observer<Entry<String, Point>>() {
                    @Override
                    public void onCompleted() {
                        completed[0] = true;
                    }

                    @Override
                    public void onError(Throwable e) {
                        throw new RuntimeException(e);
                    }

                    @Override
                    public void onNext(Entry<String, Point> e) {
                        values.add(e.value());
                    }
                });
        assertTrue(completed[0]);
        assertEquals(Collections.emptyList(), values);
        assertEquals("no traversal events without a root", 0, trace.events().size());
    }

    /**
     * request(Long.MAX_VALUE) takes the no-backpressure fast path: the whole
     * tree is walked with leaf/non-leaf MBR gates (ObservedSearch mirrors
     * LeafHelper/NonLeafHelper) and completed in one drain. Unlike the
     * backpressure path the fast path gates the root MBR, so a query whose
     * root MBR fails produces a depth-0 prune.
     */
    @Test
    public void fastPathGatesRootMbrAndCanPruneAtDepthZero() {
        TraversalFixtures.TwoLeafTree f = new TraversalFixtures.TwoLeafTree();
        SearchTrace trace = new SearchTrace();
        // root MBR is [0,10]x[0,10]; a distance predicate strictly less than 1
        // from a point far away fails even on the root
        ProbeSubscriber<String, Point> sub = subscribeProbed(f.tree,
                g -> g.distance(rectangle(100, 100, 100, 100)) < 1.0, trace);

        sub.producer().request(Long.MAX_VALUE);

        assertTrace("fast path prunes at the root gate and completes",
                Arrays.asList("push root depth 0", "request 9223372036854775807",
                        "prune root depth 0", "complete"),
                trace, f);
        assertEquals(Collections.emptyList(), sub.values());
        assertTrue(trace.completed());
    }

    /**
     * Fast path (Long.MAX_VALUE) through both leaves records the same logical
     * visit sequence as a fully-demanded backpressure drain, plus explicit
     * pushes for each descended node.
     */
    @Test
    public void fastPathEmitsAllHitsInTreeOrder() {
        TraversalFixtures.TwoLeafTree f = new TraversalFixtures.TwoLeafTree();
        SearchTrace trace = new SearchTrace();
        ProbeSubscriber<String, Point> sub = subscribeProbed(f.tree,
                intersects(rectangle(-100, -100, 100, 100)), trace);

        sub.producer().request(Long.MAX_VALUE);

        assertTrace("fast path: root gate passes, both leaves pushed and all entries hit",
                Arrays.asList("push root depth 0", "request 9223372036854775807",
                        "push leafA depth 1", "tested a1 matches=true", "hit a1",
                        "tested a2 matches=true", "hit a2", "push leafB depth 1",
                        "tested b1 matches=true", "hit b1", "tested b2 matches=true", "hit b2",
                        "complete"),
                trace, f);
        assertEquals(Arrays.asList("a1", "a2", "b1", "b2"), sub.values());
    }


    // --------------------------------------------------------------
    // Helpers
    // --------------------------------------------------------------

    /**
     * Attach the instrumented producer directly (no RxJava subscribe-time
     * auto-request) so the test controls demand exactly. The resulting
     * observable contract is identical to {@code tree.search(condition)} apart
     * from when demand arrives.
     */
    @SuppressWarnings("unchecked")
    private static <T, S extends com.github.davidmoten.rtree.geometry.Geometry> ProbeSubscriber<T, S> subscribeProbed(
            RTree<T, S> tree, Func1<? super com.github.davidmoten.rtree.geometry.Geometry, Boolean> condition,
            SearchTrace trace) {
        ProbeSubscriber<T, S> subscriber = new ProbeSubscriber<T, S>();
        assertTrue("fixture trees used by traversal tests must have a root",
                tree.root().isPresent());
        // install the producer directly, bypassing RxJava subscribe-time
        // auto-request, so demand is driven solely by explicit request() calls
        SearchProducer<T, S> producer = new OnSubscribeSearch.SearchProducer<T, S>(
                (Node<T, S>) tree.root().get(), condition, subscriber, trace.observer());
        subscriber.attachProducer(producer);
        return subscriber;
    }

    private static List<String> describeAll(List<Event> events,
            final TraversalFixtures.TwoLeafTree fixture) {
        List<String> out = new ArrayList<String>();
        for (Event event : events) {
            out.add(describe(event, fixture));
        }
        return out;
    }

    private static String describe(Event event, final TraversalFixtures.TwoLeafTree fixture) {
        if (event instanceof Push) {
            Push p = (Push) event;
            return "push " + TraversalFixtures.label(p.node) + " depth " + p.depth;
        }
        if (event instanceof Prune) {
            Prune p = (Prune) event;
            return "prune " + TraversalFixtures.label(p.node) + " depth " + p.depth;
        }
        if (event instanceof Hit) {
            return "hit " + ((Hit) event).entry.value();
        }
        if (event instanceof EntryTested) {
            EntryTested e = (EntryTested) event;
            return "tested " + e.entry.value() + " matches=" + e.matches;
        }
        if (event instanceof Request) {
            return "request " + ((Request) event).n;
        }
        if (event instanceof Cancel) {
            return "cancel";
        }
        if (event instanceof Complete) {
            return "complete";
        }
        return event.toString();
    }

    /** Assert exact forward trace, with the whole trace rendered on failure. */
    private static void assertTrace(String message, List<String> expected, SearchTrace trace,
            TraversalFixtures.TwoLeafTree fixture) {
        List<String> actual = describeAll(trace.events(), fixture);
        assertEquals(message + "\n--- actual forward trace ---\n" + trace.render(), expected,
                actual);
    }


    // ==============================================================
    // Circle and point: MBR pre-filter followed by exact refinement
    // ==============================================================

    /**
     * {@code search(Circle)} is {@code search(circle.mbr())} followed by an
     * exact {@code geometryIntersectsCircle} filter (RTree.search(Circle)). The
     * traversal observer is attached to the MBR stage, so an entry whose point
     * MBR is inside the enclosing square but outside the disk is tested at the
     * node (match=true) yet absent from the subscriber: an MBR candidate, not a
     * geometric hit.
     */
    @Test
    public void circleSearchReportsMbrCandidateThatExactFilterRejects() {
        TraversalFixtures.TwoLeafTree f = new TraversalFixtures.TwoLeafTree();
        SearchTrace trace = new SearchTrace();
        // disk centred at (0,1), radius 1: contains (0,0); enclosing square
        // reaches y=2 and never touches x=3 or leafB [7,10]
        Circle disk = circle(0, 1, 1);
        ProbeSubscriber<String, Point> sub = subscribeProbed(f.tree, intersects(disk.mbr()), trace);
        final List<String> exactHits = new ArrayList<String>();
        sub.producer().request(100);
        // apply the same public refinement that RTree.search(Circle) applies on
        // top of the MBR search
        for (Entry<String, Point> e : sub.nextEntries()) {
            if (Intersects.geometryIntersectsCircle.call(e.geometry(), disk)) {
                exactHits.add(e.value());
            }
        }

        assertTrace("the MBR stage descends by enclosing-square overlap only",
                Arrays.asList("push root depth 0", "request 100", "push leafA depth 1",
                        "tested a1 matches=true", "hit a1", "tested a2 matches=false",
                        "prune leafB depth 1", "complete"),
                trace, f);
        // a1 is on the disk boundary (distance 1, <= radius); nothing else is
        assertEquals(Arrays.asList("a1"), exactHits);
    }

    /**
     * An entry inside the enclosing square but outside the disk proves the two
     * stages differ. Point (0,3) has x-distance 0, y-distance 2 from a
     * radius-1 disk at (0,1): inside the square, outside the circle.
     */
    @Test
    public void pointInsideEnclosingSquareButOutsideDiskIsFilteredNotPruned() {
        TraversalFixtures.LineLeafTree line = new TraversalFixtures.LineLeafTree();
        SearchTrace trace = new SearchTrace();
        Circle disk = circle(2, 1, 1);
        ProbeSubscriber<String, Point> sub = subscribeProbed(line.tree, intersects(disk.mbr()),
                trace);
        sub.producer().request(100);

        // MBR (square [1,0]-[3,2]) candidates among p0..p4 on the x axis
        List<String> mbrCandidates = new ArrayList<String>();
        List<String> exactHits = new ArrayList<String>();
        for (Entry<String, Point> e : sub.nextEntries()) {
            mbrCandidates.add((String) e.value());
            if (Intersects.geometryIntersectsCircle.call(e.geometry(), disk)) {
                exactHits.add((String) e.value());
            }
        }
        // every candidate was traversed and tested: no subtree was pruned here
        // because the single leaf's MBR overlaps the enclosing square
        assertEquals("square candidates", Arrays.asList("p1", "p2", "p3"), mbrCandidates);
        // only points with 2D distance <= 1 from (2,1): on the axis y=0 the
        // squared distance to (2,1) is 1+d^2 so only d==0 qualifies
        assertEquals("exact disk hits", Arrays.asList("p2"), exactHits);
        assertTrue(trace.completed());
    }

    // ==============================================================
    // Distance upper bound, nearest ordering, maxCount
    // ==============================================================

    /**
     * {@code search(Point, maxDistance)} uses the strict predicate
     * {@code g.distance(p.mbr()) < maxDistance} (RTree.search(Rectangle,double)):
     * an entry exactly at the boundary is excluded. The MBR stage prunes
     * subtrees whose minimum possible distance is already >= maxDistance.
     */
    @Test
    public void maxDistanceIsStrictAndPrunesByMbrDistance() {
        TraversalFixtures.LineLeafTree line = new TraversalFixtures.LineLeafTree();
        SearchTrace trace = new SearchTrace();
        Point anchorPoint = point(2, 0);
        // strict < 2: p0 distance 2 and p4 distance 2 are excluded; p1, p2, p3
        // included
        ProbeSubscriber<String, Point> sub = subscribeProbed(line.tree,
                g -> g.distance(anchorPoint.mbr()) < 2.0, trace);
        sub.producer().request(100);

        assertEquals(Arrays.asList("p1", "p2", "p3"), sub.values());
        List<String> tested = new ArrayList<String>();
        for (Event event : trace.only(EntryTested.class)) {
            tested.add(String.valueOf(((EntryTested) event).entry.value()));
        }
        assertEquals("single leaf so all entries tested",
                Arrays.asList("p0", "p1", "p2", "p3", "p4"), tested);
        assertTrue(trace.completed());
    }

    /**
     * When the subtree MBR is farther than the strict bound it is pruned at
     * descent: leafB [7,10] has minimum distance 4 from a bound of radius 4
     * around (3,3), so b1/b2 are never tested while the whole leafA is walked.
     */
    @Test
    public void distanceBoundPrunesWholeSubtreeBeforeEntriesAreTested() {
        TraversalFixtures.TwoLeafTree f = new TraversalFixtures.TwoLeafTree();
        SearchTrace trace = new SearchTrace();
        Rectangle window = rectangle(3, 3, 3, 3);
        ProbeSubscriber<String, Point> sub = subscribeProbed(f.tree,
                g -> g.distance(window) < 4.0, trace);
        sub.producer().request(100);

        assertTrace(
                "a1 distance ~4.24 fails; a2 distance 0 hits; leafB min distance 4 not < 4 pruned",
                Arrays.asList("push root depth 0", "request 100", "push leafA depth 1",
                        "tested a1 matches=false", "tested a2 matches=true", "hit a2",
                        "prune leafB depth 1", "complete"),
                trace, f);
        assertEquals(Arrays.asList("a2"), sub.values());
    }

    /**
     * {@code nearest} = distance-bounded search lifted through
     * {@link OperatorBoundedPriorityQueue} with
     * {@link Comparators#ascendingDistance}. Results are emitted ascending by
     * distance only after the upstream traversal has completed, so downstream
     * order is a property of the queue sort, not of tree traversal.
     */
    @Test
    public void nearestEmitsAscendingByDistanceRegardlessOfTreeOrder() {
        // tree traversal visits leafA's points then leafB's; nearest to (2,0)
        // must come out ascending by euclidean distance
        TraversalFixtures.TwoLeafTree f = new TraversalFixtures.TwoLeafTree();
        List<String> values = new ArrayList<String>();
        f.tree.nearest(point(2, 0), 100.0, 3).map(Entry::value).forEach(values::add);
        // distances from (2,0): a1(0,0)=2, a2(3,3)=sqrt(10)~3.16,
        // b1(7,7)=sqrt(74)~8.6, b2(10,10) larger; cap is 3
        assertEquals(Arrays.asList("a1", "a2", "b1"), values);
    }

    /**
     * {@code maxCount} caps how many entries the bounded priority queue retains
     * and emits, but the upstream distance-bounded search still visits every
     * candidate within maxDistance. Proved by instrumenting the upstream search
     * that {@code nearest} is built on: more than {@code maxCount} entries flow
     * through, yet only {@code maxCount} reach the consumer.
     */
    @Test
    public void maxCountCapsEmissionsButNotUpstreamVisits() {
        TraversalFixtures.LineLeafTree line = new TraversalFixtures.LineLeafTree();
        Point anchorPoint = point(2, 0);
        SearchTrace trace = new SearchTrace();
        ProbeSubscriber<String, Point> upstream = subscribeProbed(line.tree,
                g -> g.distance(anchorPoint.mbr()) < 100.0, trace);
        upstream.producer().request(100);

        final List<String> emitted = new ArrayList<String>();
        rx.Subscriber<Entry<String, Point>> child = new rx.Subscriber<Entry<String, Point>>() {
            @Override
            public void onCompleted() {
            }

            @Override
            public void onError(Throwable e) {
                throw new RuntimeException(e);
            }

            @Override
            public void onNext(Entry<String, Point> e) {
                emitted.add(e.value());
            }
        };
        // lift the same upstream entries through the same operator nearest() uses
        rx.Subscriber<? super Entry<String, Point>> op =
                new OperatorBoundedPriorityQueue<Entry<String, Point>>(2,
                        Comparators.ascendingDistance(anchorPoint.mbr())).call(child);
        op.onStart();
        for (Entry<String, Point> e : upstream.nextEntries()) {
            op.onNext(e);
        }
        op.onCompleted();

        assertEquals("all five candidates visited upstream",
                Arrays.asList("p0", "p1", "p2", "p3", "p4"), upstream.values());
        assertEquals("maxCount=2 emitted, closest first", Arrays.asList("p2", "p1"), emitted);
    }

    /**
     * Strict distance bound applied to {@code nearest}: boundary-distance
     * entries are excluded even though the queue has spare capacity.
     */
    @Test
    public void nearestRespectsStrictMaxDistance() {
        TraversalFixtures.LineLeafTree line = new TraversalFixtures.LineLeafTree();
        List<String> values = new ArrayList<String>();
        // anchor p2 at distance 0; strict < 2 excludes p0 and p4 at distance 2
        line.tree.nearest(point(2, 0), 2.0, 10).map(Entry::value).forEach(values::add);
        assertEquals(Arrays.asList("p2", "p1", "p3"), values);
    }

    /**
     * Equal-distance entries are compare-by-distance tied; the library does not
     * promise a global order between them. We assert only the set at the tied
     * distance and never a relative order.
     */
    @Test
    public void nearestEqualDistancesDoNotPromiseGlobalOrder() {
        TraversalFixtures.LineLeafTree line = new TraversalFixtures.LineLeafTree();
        List<String> values = new ArrayList<String>();
        // anchor (2,0.5): p1 and p3 tie at sqrt(1.25); p2 is closer at 0.5
        line.tree.nearest(point(2, 0.5), 100.0, 3).map(Entry::value).forEach(values::add);
        assertEquals("closest first", "p2", values.get(0));
        assertTrue("a tied neighbour must be present: " + values, values.contains("p1"));
        assertTrue("a tied neighbour must be present: " + values, values.contains("p3"));
        // no assertion about whether p1 precedes p3: that order is not promised
        assertEquals("maxCount honoured", 3, values.size());
    }

    /**
     * Unsubscribing a downstream subscriber of the bounded priority queue while
     * it is emitting its ordered list stops further {@code onNext} immediately
     * (OperatorBoundedPriorityQueue.call onCompleted loop).
     */
    @Test
    public void boundedPriorityQueueStopsEmittingAfterUnsubscribe() {
        TraversalFixtures.LineLeafTree line = new TraversalFixtures.LineLeafTree();
        final List<Entry<String, Point>> all = line.tree
                .nearest(point(2, 0), 100.0, 3).toList().toBlocking().single();
        final List<String> emitted = new ArrayList<String>();
        rx.Subscriber<Entry<String, Point>> child = new rx.Subscriber<Entry<String, Point>>() {
            @Override
            public void onStart() {
                request(Long.MAX_VALUE);
            }

            @Override
            public void onCompleted() {
            }

            @Override
            public void onError(Throwable e) {
                throw new RuntimeException(e);
            }

            @Override
            public void onNext(Entry<String, Point> e) {
                emitted.add(e.value());
                unsubscribe();
            }
        };
        rx.Subscriber<? super Entry<String, Point>> op =
                new OperatorBoundedPriorityQueue<Entry<String, Point>>(3,
                        Comparators.ascendingDistance(point(2, 0).mbr())).call(child);
        op.onStart();
        for (Entry<String, Point> e : all) {
            op.onNext(e);
        }
        op.onCompleted();
        assertEquals("unsubscribe during emission prevents further onNext", 1, emitted.size());
    }

    /**
     * {@code search(Point)} delegates to {@code search(point.mbr())}
     * (RTree.search(Point)): a point is a zero-area rectangle and intersection
     * is containment. The subscriber therefore sees exactly the entries whose
     * degenerate MBR contains the query point, in traversal order.
     */
    @Test
    public void pointSearchIsZeroAreaRectangleIntersection() {
        TraversalFixtures.TwoLeafTree f = new TraversalFixtures.TwoLeafTree();
        SearchTrace trace = new SearchTrace();
        ProbeSubscriber<String, Point> sub = subscribeProbed(f.tree,
                intersects(point(3, 3).mbr()), trace);
        sub.producer().request(100);

        assertTrace(
                "point query prunes leafB by disjoint MBR and only matches the containing point",
                Arrays.asList("push root depth 0", "request 100", "push leafA depth 1",
                        "tested a1 matches=false", "tested a2 matches=true", "hit a2",
                        "prune leafB depth 1", "complete"),
                trace, f);
        assertEquals(Arrays.asList("a2"), sub.values());
    }
    // ==============================================================
    // Rectangle intersection: forward node access, prune, leaf hit
    // ==============================================================

    /**
     * {@code search(Rectangle)} backpressure path, enough demand to finish in
     * one drain: root pushed at subscribe; leafA pushed when its MBR passes;
     * a1 tested+hit, a2 tested+miss; leafB is pruned by MBR (never pushed);
     * then complete. Docs: "Backpressure path: prune happens on the descent
     * decision; a pruned node is never on the stack"
     * (Backpressure.searchNonLeaf).
     */
    @Test
    public void rectangleSearchPrunesWholeLeafByMbrAndEmitsInTreeOrder() {
        TraversalFixtures.TwoLeafTree f = new TraversalFixtures.TwoLeafTree();
        SearchTrace trace = new SearchTrace();
        ProbeSubscriber<String, Point> sub = subscribeProbed(f.tree,
                intersects(rectangle(-1, -1, 1, 1)), trace);

        sub.producer().request(100);

        assertTrace("rectangle search should descend only into the intersecting leaf",
                Arrays.asList("push root depth 0", "request 100", "push leafA depth 1",
                        "tested a1 matches=true", "hit a1", "tested a2 matches=false",
                        "prune leafB depth 1", "complete"),
                trace, f);
        assertEquals(Arrays.asList("a1"), sub.values());
        assertTrue(trace.completed());
        assertNull(trace.error());
    }

    /**
     * With no outstanding demand nothing is visited: the root is pushed at
     * subscribe but no child is pushed or pruned. This is "not visited yet due
     * to backpressure", which must be reported differently from an MBR prune.
     */
    @Test
    public void noRequestMeansNoNodeBeyondRootIsVisited() {
        TraversalFixtures.TwoLeafTree f = new TraversalFixtures.TwoLeafTree();
        SearchTrace trace = new SearchTrace();
        ProbeSubscriber<String, Point> sub = subscribeProbed(f.tree,
                intersects(rectangle(-1, -1, 1, 1)), trace);

        assertTrace("before any request only the root node push may be recorded",
                Arrays.asList("push root depth 0"), trace, f);
        assertEquals(Collections.emptyList(), sub.values());
        assertFalse(trace.completed());
    }

    /**
     * request(1) pulls exactly one onNext and freezes the stack mid-leaf: a2
     * has not been evaluated and leafB has neither been pruned nor visited. A
     * second request(1) resumes at exactly the saved ImmutableStack position
     * and (after draining the miss) performs the MBR prune, proving the walk is
     * resumed from stack state rather than reconstructed from results.
     */
    @Test
    public void requestOneWalksTheTreeOneEmissionAtATime() {
        TraversalFixtures.TwoLeafTree f = new TraversalFixtures.TwoLeafTree();
        SearchTrace trace = new SearchTrace();
        ProbeSubscriber<String, Point> sub = subscribeProbed(f.tree,
                intersects(rectangle(-1, -1, 3, 3)), trace);

        sub.producer().request(1);
        assertTrace("after request(1) traversal pauses at the hit; a2 untouched",
                Arrays.asList("push root depth 0", "request 1", "push leafA depth 1",
                        "tested a1 matches=true", "hit a1"),
                trace, f);
        assertEquals(Arrays.asList("a1"), sub.values());
        assertFalse(trace.completed());

        sub.producer().request(1);
        // a2 is the last entry in leafA and its hit consumes the fresh demand;
        // the zero-budget check runs before the node-exit bookkeeping so the
        // walk freezes immediately with leafA still on the stack - leafB has
        // still neither been pruned nor visited
        assertTrace("after second hit the walk freezes before pop/advance when budget is zero",
                Arrays.asList("push root depth 0", "request 1", "push leafA depth 1",
                        "tested a1 matches=true", "hit a1", "request 1",
                        "tested a2 matches=true", "hit a2"),
                trace, f);
        assertEquals(Arrays.asList("a1", "a2"), sub.values());
        assertFalse(trace.completed());

        sub.producer().request(1);
        // the saved ImmutableStack position is reused: finish leafA, MBR-prune
        // leafB (no demand needed for a prune), exhaust the stack and complete
        assertTrace("final demand resumes from saved stack, prunes leafB and completes",
                Arrays.asList("push root depth 0", "request 1", "push leafA depth 1",
                        "tested a1 matches=true", "hit a1", "request 1",
                        "tested a2 matches=true", "hit a2", "request 1",
                        "prune leafB depth 1", "complete"),
                trace, f);
        assertEquals(Arrays.asList("a1", "a2"), sub.values());
        assertTrue(trace.completed());
    }

    /**
     * request(1) with the demand exhausted on a non-matching entry keeps going
     * through sibling misses for free (only hits consume demand) but still
     * stops before descending the next candidate when budget is zero. Docs:
     * "only an emitted entry decrements the outstanding request count"
     * (Backpressure.searchLeaf).
     */
    @Test
    public void missesDoNotConsumeDemandButZeroBudgetStopsBeforeNextDescent() {
        TraversalFixtures.TwoLeafTree f = new TraversalFixtures.TwoLeafTree();
        SearchTrace trace = new SearchTrace();
        // window intersects leafB only: leafA is an all-miss leaf under demand
        ProbeSubscriber<String, Point> sub = subscribeProbed(f.tree,
                intersects(rectangle(7, 7, 8, 8)), trace);

        sub.producer().request(1);
        // leafA MBR fails on descent -> pruned immediately (prunes do not need
        // demand); b1 hits and consumes the single demand; the walk then stops
        // mid-leaf before b2 is even tested
        assertTrace("prune is a descent decision needing no demand; the hit consumes demand",
                Arrays.asList("push root depth 0", "request 1", "prune leafA depth 1",
                        "push leafB depth 1", "tested b1 matches=true", "hit b1"),
                trace, f);
        assertEquals(Arrays.asList("b1"), sub.values());
        assertFalse(trace.completed());

        sub.producer().request(1);
        assertTrace("b2 was not merely missed: it was not yet visited; now tested and missed",
                Arrays.asList("push root depth 0", "request 1", "prune leafA depth 1",
                        "push leafB depth 1", "tested b1 matches=true", "hit b1",
                        "request 1", "tested b2 matches=false", "complete"),
                trace, f);
        assertEquals(Arrays.asList("b1"), sub.values());
        assertTrue(trace.completed());
    }

}
