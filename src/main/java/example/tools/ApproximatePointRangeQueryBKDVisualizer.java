package example.tools;

import java.text.SimpleDateFormat;
import java.util.*;

public class ApproximatePointRangeQueryBKDVisualizer {

    private static final SimpleDateFormat SDF = new SimpleDateFormat("HH:mm:ss");

    public static void main(String[] args) throws Exception {
        System.out.println("=== SIMPLE BKD TREE INTERSECT RIGHT VISUALIZATION ===\n");

        // Create a simple tree structure
        TreeNode root = createSampleTree();

        // Print the tree structure
        System.out.println("--- BKD Tree Structure ---");
        printTree(root, "", true);

        // Simulate intersectRight for different sizes
        System.out.println("\n--- IntersectRight Traversal (Size: 600) ---");
        simulateIntersectRight(root, 600);

        System.out.println("\n--- IntersectRight Traversal (Size: 1500) ---");
        simulateIntersectRight(root, 1500);

        System.out.println("\n--- IntersectRight Traversal (Size: 2100) ---");
        simulateIntersectRight(root, 2100);
    }

    private static TreeNode createSampleTree() {
        // Create a simple tree with timestamps from 10:00:00 to 10:30:00
        // Total 4096 docs distributed across 8 leaves (512 each)

        // Leaf nodes (each has 512 docs)
        TreeNode leaf1 = new TreeNode("10:00:00", "10:03:00", 512, true);
        TreeNode leaf2 = new TreeNode("10:03:00", "10:06:00", 512, true);
        TreeNode leaf3 = new TreeNode("10:06:00", "10:09:00", 512, true);
        TreeNode leaf4 = new TreeNode("10:09:00", "10:12:00", 512, true);
        TreeNode leaf5 = new TreeNode("10:12:00", "10:15:00", 512, true);
        TreeNode leaf6 = new TreeNode("10:15:00", "10:20:00", 512, true);
        TreeNode leaf7 = new TreeNode("10:20:00", "10:25:00", 512, true);
        TreeNode leaf8 = new TreeNode("10:25:00", "10:30:00", 512, true);

        // Level 2 nodes
        TreeNode left2 = new TreeNode("10:00:00", "10:06:00", 1024, false);
        left2.children.add(leaf1);
        left2.children.add(leaf2);

        TreeNode right2 = new TreeNode("10:06:00", "10:12:00", 1024, false);
        right2.children.add(leaf3);
        right2.children.add(leaf4);

        TreeNode left3 = new TreeNode("10:12:00", "10:20:00", 1024, false);
        left3.children.add(leaf5);
        left3.children.add(leaf6);

        TreeNode right3 = new TreeNode("10:20:00", "10:30:00", 1024, false);
        right3.children.add(leaf7);
        right3.children.add(leaf8);

        // Level 1 nodes
        TreeNode left1 = new TreeNode("10:00:00", "10:12:00", 2048, false);
        left1.children.add(left2);
        left1.children.add(right2);

        TreeNode right1 = new TreeNode("10:12:00", "10:30:00", 2048, false);
        right1.children.add(left3);
        right1.children.add(right3);

        // Root
        TreeNode root = new TreeNode("10:00:00", "10:30:00", 4096, false);
        root.children.add(left1);
        root.children.add(right1);

        return root;
    }

    private static void printTree(TreeNode node, String prefix, boolean isLast) {
        String nodeType = node.isLeaf ? "LEAF" : "NODE";
        String nodeInfo = String.format("%s [%s to %s] size=%d",
                nodeType, node.minTime, node.maxTime, node.size);

        System.out.println(prefix + (isLast ? "└── " : "├── ") + nodeInfo);

        if (!node.isLeaf) {
            for (int i = 0; i < node.children.size(); i++) {
                boolean childIsLast = (i == node.children.size() - 1);
                String childPrefix = prefix + (isLast ? "    " : "│   ");
                printTree(node.children.get(i), childPrefix, childIsLast);
            }
        }
    }

    private static void simulateIntersectRight(TreeNode root, int targetSize) {
        long[] docCount = {0};
        List<String> traversalPath = new ArrayList<>();
        int[] callCounter = {0};  // Track recursive call number

        System.out.println("\nSimulating collection of " + targetSize + " docs from highest timestamps...\n");

        // Create a PointTree-like wrapper to simulate the actual API
        PointTreeSimulator pointTree = new PointTreeSimulator(root);

        // This simulates the actual intersectRight algorithm
        intersectRight(pointTree, docCount, targetSize, traversalPath, "", 0, callCounter);

        // Print traversal path
        for (String step : traversalPath) {
            System.out.println(step);
        }

        System.out.println("\nTotal docs collected: " + docCount[0]);
    }

    private static void intersectRight(PointTreeSimulator pointTree, long[] docCount, int size,
                                       List<String> path, String indent, int depth, int[] callCounter) {
        int currentCallId = ++callCounter[0];
        String callInfo = String.format("[Call #%d, Depth %d]", currentCallId, depth);
        if (docCount[0] >= size) {
            path.add(indent + "✓ Reached target size, stopping traversal " + callInfo);
            return;
        }

        TreeNode currentNode = pointTree.getCurrentNode();
        String nodeInfo = String.format("[%s to %s] size=%d",
                currentNode.minTime, currentNode.maxTime, currentNode.size);

        if (currentNode.isLeaf) {
            // Leaf node - collect docs
            long docsToCollect = Math.min(512, size - docCount[0]);
            docCount[0] += docsToCollect;

            path.add(indent + "→ VISIT LEAF " + nodeInfo + " " + callInfo);
            path.add(indent + "  Collected " + docsToCollect + " docs (total: " + docCount[0] + ")");
        } else {
            // Internal node - this matches the actual intersectRight logic
            path.add(indent + "→ ENTER NODE " + nodeInfo + " " + callInfo);

            // if (pointTree.moveToChild() && docCount[0] < size)
            if (pointTree.moveToChild() && docCount[0] < size) {
                path.add(indent + "  moveToChild() → " + pointTree.getCurrentNode().minTime + "-" + pointTree.getCurrentNode().maxTime);

                // Move to the rightmost sibling using while loop
                // This matches the actual code: while (pointTree.moveToSibling()) {}
                int siblingMoves = 0;
                while (pointTree.moveToSibling()) {
                    siblingMoves++;
                    path.add(indent + "  moveToSibling() [move " + siblingMoves + "] → " + pointTree.getCurrentNode().minTime + "-" + pointTree.getCurrentNode().maxTime);
                }
                if (siblingMoves > 0) {
                    path.add(indent + "  (Moved to rightmost sibling after " + siblingMoves + " moves)");
                }

                // Process the current child (which is now the rightmost)
                path.add(indent + "  ↓ Recursively process current child (Call #" + (callCounter[0] + 1) + ")");
                intersectRight(pointTree, docCount, size, path, indent + "    ", depth + 1, callCounter);

                // pointTree.moveToParent()
                pointTree.moveToParent();
                path.add(indent + "  moveToParent() → back to " + nodeInfo);

                // if (docCount[0] < size)
                if (docCount[0] < size) {
                    path.add(indent + "  docCount < size, need more docs");

                    // pointTree.moveToChild()
                    if (pointTree.moveToChild()) {
                        path.add(indent + "  moveToChild() → " + pointTree.getCurrentNode().minTime + "-" + pointTree.getCurrentNode().maxTime);
                        path.add(indent + "  ↓ Process left child (Call #" + (callCounter[0] + 1) + ")");
                        intersectRight(pointTree, docCount, size, path, indent + "    ", depth + 1, callCounter);

                        // pointTree.moveToParent()
                        pointTree.moveToParent();
                        path.add(indent + "  moveToParent() → back to " + nodeInfo);
                    }
                }
            }

            path.add(indent + "← EXIT NODE " + nodeInfo + " " + callInfo);
        }
    }

    // Simulator class that mimics the PointTree API
    static class PointTreeSimulator {
        private TreeNode root;
        private Stack<TreeNode> nodeStack;
        private Stack<Integer> childIndexStack;

        PointTreeSimulator(TreeNode root) {
            this.root = root;
            this.nodeStack = new Stack<>();
            this.childIndexStack = new Stack<>();
            this.nodeStack.push(root);
        }

        TreeNode getCurrentNode() {
            return nodeStack.peek();
        }

        boolean moveToChild() {
            TreeNode current = nodeStack.peek();
            if (!current.isLeaf && !current.children.isEmpty()) {
                nodeStack.push(current.children.get(0));
                childIndexStack.push(0);
                return true;
            }
            return false;
        }

        boolean moveToSibling() {
            if (nodeStack.size() <= 1) {
                return false; // No parent, so no siblings
            }

            TreeNode parent = nodeStack.get(nodeStack.size() - 2);
            int currentIndex = childIndexStack.peek();

            if (currentIndex + 1 < parent.children.size()) {
                // Move to next sibling
                nodeStack.pop();
                childIndexStack.pop();
                nodeStack.push(parent.children.get(currentIndex + 1));
                childIndexStack.push(currentIndex + 1);
                return true;
            }
            return false;
        }

        boolean moveToParent() {
            if (nodeStack.size() > 1) {
                nodeStack.pop();
                childIndexStack.pop();
                return true;
            }
            return false;
        }
    }

    // Simple tree node class
    static class TreeNode {
        String minTime;
        String maxTime;
        int size;
        boolean isLeaf;
        List<TreeNode> children;

        TreeNode(String minTime, String maxTime, int size, boolean isLeaf) {
            this.minTime = minTime;
            this.maxTime = maxTime;
            this.size = size;
            this.isLeaf = isLeaf;
            this.children = new ArrayList<>();
        }
    }
}