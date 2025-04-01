package example.points;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.*;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class BKDTreeInspector {

    public static void main(String[] args) throws Exception {
        // Path for the index that has regression issues
        Path indexPathWithRegression = Paths.get("/home/ec2-user/OS_30/OS_30_27_Vanilla_968eafbd37ef0b864e887643c74291cc3e5ca0d0/nodes/0/indices/rOA5QvdyQ-2JYf3h0WZmFw/0/index");
        // Path indexPathWithoutRegression = Paths.get("/home/ec2-user/OS_30/OS_2191_1_2e4741fb45d1b150aaeeadf66d41445b23ff5982/nodes/0/indices/Id-ddMW1RTqSkiHeqTbCRA/0/index");
        String fieldName = "@timestamp";
        System.out.println("\n\n=== INSPECTING INDEX WITH REGRESSION ===");
        inspectIndex(indexPathWithRegression, fieldName);
    }

    private static void inspectIndex(Path indexPath, String fieldName) throws IOException {
        System.out.println("Inspecting BKD tree for path: " + indexPath);
        System.out.println("Field name: " + fieldName);

        try (FSDirectory directory = FSDirectory.open(indexPath);
             IndexReader reader = DirectoryReader.open(directory)) {

            for (LeafReaderContext leaf : reader.leaves()) {
                PointValues pointValues = leaf.reader().getPointValues(fieldName);
                if (pointValues == null) {
                    System.out.println("No point values for field '" + fieldName + "' in leaf: " + leaf);
                    continue;
                }

                System.out.println("\n=== Leaf: " + leaf + " ===");
                System.out.println("Total points: " + pointValues.size());
                System.out.println("Doc count: " + pointValues.getDocCount());

                // Calculate points per doc ratio
                double pointsPerDoc = (double) pointValues.size() / pointValues.getDocCount();
                System.out.println("Points per doc ratio: " + pointsPerDoc);

                // Analyze the min/max range
                byte[] minPackedValue = pointValues.getMinPackedValue();
                byte[] maxPackedValue = pointValues.getMaxPackedValue();

                long minTimestamp = LongPoint.decodeDimension(minPackedValue, 0);
                long maxTimestamp = LongPoint.decodeDimension(maxPackedValue, 0);

                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
                System.out.println("Min timestamp: " + minTimestamp + " (" + sdf.format(new Date(minTimestamp)) + ")");
                System.out.println("Max timestamp: " + maxTimestamp + " (" + sdf.format(new Date(maxTimestamp)) + ")");
                System.out.println("Range span: " + (maxTimestamp - minTimestamp) + " ms");

                // Analyze BKD tree structure
                analyzeBKDTree(pointValues);

            }
        }
    }


    private static void analyzeBKDTree(PointValues pointValues) throws IOException {
        PointValues.PointTree tree = pointValues.getPointTree();

        // Basic tree info
        int treeDepth = measureTreeDepth(tree.clone());
        System.out.println("\nBKD Tree Analysis:");
        System.out.println("Tree depth: " + treeDepth);

        // Calculate theoretical leaf nodes
        int maxLeaves = 1 << treeDepth;
        System.out.println("Max theoretical leaves: " + maxLeaves);

        // Count actual leaf nodes and analyze their balance
        LeafNodeStats stats = countLeafNodes(tree.clone(), 1, 0);
        System.out.println("Actual leaf nodes: " + stats.leafCount);
        System.out.println("Leaf node imbalance: " + (maxLeaves - stats.leafCount) + " missing leaves");
        System.out.println("Min leaf size: " + stats.minSize);
        System.out.println("Max leaf size: " + stats.maxSize);
        System.out.println("Avg leaf size: " + (stats.totalPoints / (double)stats.leafCount));
        System.out.println("Size ratio (max/min): " + (stats.minSize > 0 ? stats.maxSize / (double)stats.minSize : "N/A"));

        // Analyze node distribution per level
        Map<Integer, int[]> levelStats = new HashMap<>();
        collectLevelStats(tree.clone(), 1, 0, levelStats);

        System.out.println("\nLevel distribution:");
        for (int level = 0; level <= treeDepth; level++) {
            int[] stats2 = levelStats.getOrDefault(level, new int[]{0, 0});
            int nodeCount = stats2[0];
            long totalSize = stats2[1];
            double avgSize = nodeCount > 0 ? totalSize / (double)nodeCount : 0;

            System.out.printf("Level %d: %d nodes, avg size: %.2f%n",
                    level, nodeCount, avgSize);
        }

        // Analyze split value distribution - critical for understanding BKD tree balance
        System.out.println("\nLevel split values (sampling):");
        analyzeSplitValues(tree.clone(), 1, 0);
    }

    private static class LeafNodeStats {
        int leafCount = 0;
        long minSize = Long.MAX_VALUE;
        long maxSize = 0;
        long totalPoints = 0;
    }

    private static LeafNodeStats countLeafNodes(PointValues.PointTree tree, int nodeID, int level) throws IOException {
        LeafNodeStats stats = new LeafNodeStats();

        if (!tree.moveToChild()) {
            // This is a leaf node
            stats.leafCount = 1;
            long size = tree.size();
            stats.minSize = size;
            stats.maxSize = size;
            stats.totalPoints = size;
            return stats;
        }

        // Internal node - process children
        do {
            LeafNodeStats childStats = countLeafNodes(tree.clone(), nodeID * 2 + (tree.moveToSibling() ? 1 : 0), level + 1);
            stats.leafCount += childStats.leafCount;
            stats.minSize = Math.min(stats.minSize, childStats.minSize);
            stats.maxSize = Math.max(stats.maxSize, childStats.maxSize);
            stats.totalPoints += childStats.totalPoints;
        } while (tree.moveToSibling());

        return stats;
    }

    private static void collectLevelStats(PointValues.PointTree tree, int nodeID, int level, Map<Integer, int[]> levelStats) throws IOException {
        // Record this node's stats
        int[] stats = levelStats.computeIfAbsent(level, k -> new int[2]);
        stats[0]++; // increment node count
        stats[1] += tree.size(); // add size

        // Process children if this is not a leaf
        if (tree.moveToChild()) {
            do {
                collectLevelStats(tree.clone(), nodeID * 2 + (tree.moveToSibling() ? 1 : 0), level + 1, levelStats);
            } while (tree.moveToSibling());
            tree.moveToParent();
        }
    }

    private static void analyzeSplitValues(PointValues.PointTree tree, int nodeID, int level) throws IOException {
        if (level > 5) return; // Limit depth to avoid too much output

        byte[] minValue = tree.getMinPackedValue();
        byte[] maxValue = tree.getMaxPackedValue();
        long minTimestamp = LongPoint.decodeDimension(minValue, 0);
        long maxTimestamp = LongPoint.decodeDimension(maxValue, 0);

        System.out.printf("Node %d (Level %d): Range [%d to %d], Size: %d%n",
                nodeID, level, minTimestamp, maxTimestamp, tree.size());

        if (tree.moveToChild()) {
            // Get left child's max value to determine split point
            PointValues.PointTree leftChild = tree.clone();
            byte[] leftChildMax = leftChild.getMaxPackedValue();
            long splitValue = LongPoint.decodeDimension(leftChildMax, 0);

            System.out.printf("  Split value: %d%n", splitValue);

            // Calculate split ratio - critical for understanding imbalance
            long leftRange = splitValue - minTimestamp;
            long rightRange = maxTimestamp - splitValue;
            long totalRange = maxTimestamp - minTimestamp;

            if (totalRange > 0) {
                double leftRatio = leftRange / (double)totalRange;
                double rightRatio = rightRange / (double)totalRange;
                System.out.printf("  Split ratios: left=%.2f, right=%.2f%n", leftRatio, rightRatio);
            }

            // Recurse to children
            analyzeSplitValues(leftChild, nodeID * 2, level + 1);

            if (tree.moveToSibling()) {
                analyzeSplitValues(tree.clone(), nodeID * 2 + 1, level + 1);
            }
        }
    }

    private static int measureTreeDepth(PointValues.PointTree tree) throws IOException {
        if (!tree.moveToChild()) {
            return 0; // Leaf node
        }

        int maxDepth = 0;
        do {
            int childDepth = measureTreeDepth(tree.clone());
            maxDepth = Math.max(maxDepth, childDepth);
        } while (tree.moveToSibling());

        return maxDepth + 1;
    }
}
