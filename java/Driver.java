package main.java;

public class Driver {
  public static void main(String[] args) throws Exception {
    UnitMultiplication multiplication = new UnitMultiplication();
    UnitSum sum = new UnitSum();

    /**
     * args0: dir of transition.txt
     * args1: dir of PageRank.txt
     * args2: dir of unitMultiplication result
     * args3: times of convergence
     */
    String transitionMatrix = args[0];
    String prMatrix = args[1];
    String subPR = args[2];
    int counter = Integer.parseInt(args[3]);

    for (int i = 0; i < counter; i++) {
      // prMatrix & subPR varies times by times
      String[] args1 = {transitionMatrix, prMatrix + i, subPR + i};
      multiplication.main(args1);
      String[] args2 = {subPR + i, prMatrix + (i + 1)};
      sum.main(args2);
    }
  }
}
