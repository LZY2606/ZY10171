package com.github.davidmoten.rtree;

import static com.github.davidmoten.rtree.geometry.Geometries.point;

import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import com.github.davidmoten.rtree.Context;
import com.github.davidmoten.rtree.Entry;
import com.github.davidmoten.rtree.Leaf;
import com.github.davidmoten.rtree.Node;
import com.github.davidmoten.rtree.NonLeaf;
import com.github.davidmoten.rtree.Entries;
import com.github.davidmoten.rtree.Factories;
import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.SelectorMinimalOverlapArea;
import com.github.davidmoten.rtree.SplitterQuadratic;
import com.github.davidmoten.rtree.geometry.Geometry;
import com.github.davidmoten.rtree.geometry.Point;
import com.github.davidmoten.rtree.internal.FactoryDefault;

/**
 * Deterministic small-tree fixtures for the query-path anatomy tests.
 *
 * <p>
 * Nodes are constructed directly through the package-visible
 * {@link FactoryDefault}/{@link Context} so that shape, child order and node
 * identity are fixed by the test rather than emerging from a sequence of
 * inserts. Every node gets a stable label used in failure messages.
 */
final class TraversalFixtures {

    private TraversalFixtures() {
        // prevent instantiation
    }

    /**
     * A two-leaf tree of points with disjoint, well-separated bounding boxes:
     *
     * <pre>
     *              root [0,10]
     *             /          \\
     *   leafA [0,3]       leafB [7,10]
     *   a1(0,0) a2(3,3)   b1(7,7) b2(10,10)
     * </pre>
     *
     * Every node in this fixture is a rectangle-geometry node (points carry
     * degenerate MBRs) so both {@code intersects} and {@code distance}
     * conditions behave identically on nodes and entries.
     */
    static final class TwoLeafTree {
        final Context<String, Point> context = context();
        final Leaf<String, Point> leafA;
        final Leaf<String, Point> leafB;
        final NonLeaf<String, Point> root;
        final RTree<String, Point> tree;

        TwoLeafTree() {
            Entry<String, Point> a1 = Entries.entry("a1", point(0, 0));
            Entry<String, Point> a2 = Entries.entry("a2", point(3, 3));
            Entry<String, Point> b1 = Entries.entry("b1", point(7, 7));
            Entry<String, Point> b2 = Entries.entry("b2", point(10, 10));
            leafA = (Leaf<String, Point>) FactoryDefault.<String, Point>instance()
                    .createLeaf(Arrays.asList(a1, a2), context);
            leafB = (Leaf<String, Point>) FactoryDefault.<String, Point>instance()
                    .createLeaf(Arrays.asList(b1, b2), context);
            List<Node<String, Point>> children = Arrays
                    .<Node<String, Point>>asList(leafA, leafB);
            root = (NonLeaf<String, Point>) FactoryDefault.<String, Point>instance()
                    .createNonLeaf(children, context);
            tree = RTree.create(java.util.Optional.of(root), 4, context);
            register(root, "root");
            register(leafA, "leafA");
            register(leafB, "leafB");
        }
    }

    /**
     * A single-leaf tree of points all on the line y=0, used to reason about
     * nearest ordering and equal distances without structural noise:
     *
     * <pre>
     *   root=leafL [0,4] : p0(0,0) p1(1,0) p2(2,0) p3(3,0) p4(4,0)
     * </pre>
     */
    static final class LineLeafTree {
        final Context<String, Point> context = context();
        final Leaf<String, Point> leaf;
        final RTree<String, Point> tree;

        LineLeafTree() {
            Entry<String, Point> p0 = Entries.entry("p0", point(0, 0));
            Entry<String, Point> p1 = Entries.entry("p1", point(1, 0));
            Entry<String, Point> p2 = Entries.entry("p2", point(2, 0));
            Entry<String, Point> p3 = Entries.entry("p3", point(3, 0));
            Entry<String, Point> p4 = Entries.entry("p4", point(4, 0));
            leaf = (Leaf<String, Point>) FactoryDefault.<String, Point>instance()
                    .createLeaf(Arrays.asList(p0, p1, p2, p3, p4), context);
            tree = RTree.create(java.util.Optional.of(leaf), 5, context);
            register(leaf, "leafL");
        }
    }

    private static final Map<Node<?, ?>, String> GLOBAL_LABELS =
            new IdentityHashMap<Node<?, ?>, String>();

    static String label(Node<?, ?> node) {
        String label = GLOBAL_LABELS.get(node);
        if (label != null) {
            return label;
        }
        return node.getClass().getSimpleName() + "@"
                + Integer.toHexString(System.identityHashCode(node));
    }

    static void register(Node<?, ?> node, String label) {
        GLOBAL_LABELS.put(node, label);
    }

    private static <S extends Geometry> Context<String, S> context() {
        return new Context<String, S>(1, 4, new SelectorMinimalOverlapArea(),
                new SplitterQuadratic(), Factories.<String, S>defaultFactory());
    }

}
