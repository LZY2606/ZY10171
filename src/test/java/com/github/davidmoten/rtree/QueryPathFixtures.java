package com.github.davidmoten.rtree;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Point;
import com.github.davidmoten.rtree.internal.FactoryDefault;

/**
 * Deterministic small in-memory R-trees for the query-path tests, assembled by
 * hand with {@code maxChildren=2} so their shape is known without depending on
 * selector or splitter outcomes.
 *
 * <pre>
 *             root  MBR [0,0 : 10,3]
 *            /    \
 *        leafA      leafB
 *     [0,0 : 2,1] [8,2 : 10,3]
 *      a(0,0)      d(8,2)
 *      b(2,1)      e(10,3)
 * </pre>
 */
final class QueryPathFixtures {

    static final Entry<String, Point> A = Entries.entry("a", Geometries.point(0, 0));
    static final Entry<String, Point> B = Entries.entry("b", Geometries.point(2, 1));
    static final Entry<String, Point> D = Entries.entry("d", Geometries.point(8, 2));
    static final Entry<String, Point> E = Entries.entry("e", Geometries.point(10, 3));

    private QueryPathFixtures() {
    }

    static Context<String, Point> context() {
        // Context requires maxChildren > 2; leaves still hold only the points
        // they are handed so the hand-built shape remains fully deterministic
        return new Context<String, Point>(1, 3, new SelectorRStar(), new SplitterQuadratic(),
                FactoryDefault.<String, Point>instance());
    }

    private static List<Entry<String, Point>> pair(Entry<String, Point> first,
            Entry<String, Point> second) {
        List<Entry<String, Point>> list = new ArrayList<Entry<String, Point>>();
        list.add(first);
        list.add(second);
        return Collections.unmodifiableList(list);
    }

    /** The fixed two-leaf tree plus direct node references. */
    static final class FixedTree {
        final RTree<String, Point> tree;
        final Node<String, Point> root;
        final Node<String, Point> leafA;
        final Node<String, Point> leafB;
        private final IdentityHashMap<Node<String, Point>, String> names =
                new IdentityHashMap<Node<String, Point>, String>();

        FixedTree(RTree<String, Point> tree, Node<String, Point> root,
                Node<String, Point> leafA, Node<String, Point> leafB) {
            this.tree = tree;
            this.root = root;
            this.leafA = leafA;
            this.leafB = leafB;
            names.put(root, "root");
            names.put(leafA, "leafA");
            names.put(leafB, "leafB");
        }

        String nameOf(Node<String, Point> node) {
            String name = names.get(node);
            return name != null ? name : "node@" + System.identityHashCode(node);
        }

        java.util.function.Function<Node<String, Point>, String> namer() {
            return this::nameOf;
        }
    }

    static FixedTree fixedTree() {
        Context<String, Point> context = context();
        Node<String, Point> leafLeft = context.factory().createLeaf(pair(A, B), context);
        Node<String, Point> leafRight = context.factory().createLeaf(pair(D, E), context);
        List<Node<String, Point>> children = new ArrayList<Node<String, Point>>();
        children.add(leafLeft);
        children.add(leafRight);
        Node<String, Point> rootNode = context.factory().createNonLeaf(children, context);
        RTree<String, Point> tree = RTree.create(Optional.of(rootNode), 4, context);
        return new FixedTree(tree, rootNode, leafLeft, leafRight);
    }

    /**
     * Four points equidistant (5 units) from the origin, two per leaf, so the
     * nearest comparator ties on every pair.
     */
    static FixedTree tieTree() {
        Entry<String, Point> w = Entries.entry("w", Geometries.point(3, 4));
        Entry<String, Point> x = Entries.entry("x", Geometries.point(4, -3));
        Entry<String, Point> y = Entries.entry("y", Geometries.point(-3, 4));
        Entry<String, Point> z = Entries.entry("z", Geometries.point(-4, -3));
        Context<String, Point> context = context();
        Node<String, Point> leafWX = context.factory().createLeaf(pair(w, x), context);
        Node<String, Point> leafYZ = context.factory().createLeaf(pair(y, z), context);
        List<Node<String, Point>> children = new ArrayList<Node<String, Point>>();
        children.add(leafWX);
        children.add(leafYZ);
        Node<String, Point> rootNode = context.factory().createNonLeaf(children, context);
        RTree<String, Point> tree = RTree.create(Optional.of(rootNode), 4, context);
        return new FixedTree(tree, rootNode, leafWX, leafYZ);
    }

    /**
     * Tree built through the public {@link RTree#add(Entry)} API. Adding a
     * point inside one leaf must recreate that branch but preserve the
     * identity of the untouched sibling (structural sharing).
     */
    static final class SharedTree {
        final RTree<String, Point> before;
        final RTree<String, Point> after;
        final Node<String, Point> leftLeaf;
        final Node<String, Point> rightLeaf;

        @SuppressWarnings("unchecked")
        SharedTree() {
            Context<String, Point> context = context();
            Node<String, Point> left = context.factory().createLeaf(pair(A, B), context);
            Node<String, Point> right = context.factory().createLeaf(pair(D, E), context);
            List<Node<String, Point>> children = new ArrayList<Node<String, Point>>();
            children.add(left);
            children.add(right);
            Node<String, Point> rootNode = context.factory().createNonLeaf(children, context);
            before = RTree.create(Optional.of(rootNode), 4, context);
            after = before.add(Entries.entry("c", Geometries.point(1, 1)));
            leftLeaf = left;
            rightLeaf = right;
        }

        static Set<Node<String, Point>> collectNodes(RTree<String, Point> tree) {
            Set<Node<String, Point>> set = Collections
                    .newSetFromMap(new IdentityHashMap<Node<String, Point>, Boolean>());
            collect(tree.root().get(), set);
            return set;
        }

        private static void collect(Node<String, Point> node, Set<Node<String, Point>> set) {
            set.add(node);
            if (node instanceof NonLeaf) {
                for (Node<String, Point> child : ((NonLeaf<String, Point>) node).children()) {
                    collect(child, set);
                }
            }
        }
    }

    static SharedTree sharedTree() {
        return new SharedTree();
    }
}
