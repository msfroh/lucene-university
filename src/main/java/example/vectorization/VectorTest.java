package example.vectorization;

import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

import java.util.concurrent.ThreadLocalRandom;

public class VectorTest {
    private static final VectorSpecies<Integer> INTEGER_VECTOR_SPECIES = VectorSpecies.ofPreferred(int.class);
    public static void main(String[] args) {
        int size = 20_0000;
        int a[] = new int[size];
        int b[] = new int[size];
        for (int i = 0; i < size; i++) {
            a[i] = ThreadLocalRandom.current().nextInt(10000);
            b[i] = ThreadLocalRandom.current().nextInt(10000);
        }

        for (int i = 0; i < size; i++) {
            int scalarSum = scalarMinSum(a, b);
            int vectorSum = vectorMinSum(a, b);
            if (scalarSum != vectorSum) {
                throw new IllegalStateException("Mismatch between vector sums");
            }
        }

        long start = System.nanoTime();
        for (int i = 0; i < size; i++) {
            scalarMinSum(a, b);
        }
        System.out.println("Scalar operations took " + (System.nanoTime() - start) + " ns");
        start = System.nanoTime();
        for (int i = 0; i < size; i++) {
            vectorMinSum(a, b);
        }
        System.out.println("Vector operations took " + (System.nanoTime() - start) + " ns");
    }


    private static int scalarMinSum(int[] a, int[] b) {
        int sum = 0;
        for (int i = 0; i < a.length; i++) {
            int min = Math.min(a[i], b[i]);
            sum += min;
        }
        return sum;
    }

    private static int vectorMinSum(int[] a, int[] b) {
        int i = 0;
        int sum = 0;
        for (; i < a.length; i+= INTEGER_VECTOR_SPECIES.length()) {
            IntVector va = IntVector.fromArray(INTEGER_VECTOR_SPECIES, a, i);
            IntVector vb = IntVector.fromArray(INTEGER_VECTOR_SPECIES, b, i);
            sum += va.min(vb).reduceLanes(VectorOperators.ADD);
        }
        // Scalar tail
        for (; i < a.length; i++) {
            int min = Math.min(a[i], b[i]);
            sum += min;
        }
        return sum;
    }

}
