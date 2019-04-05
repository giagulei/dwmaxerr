package gr.ntua.ece.cslab.multdimwavelets.thresholding.mr.budgreedy;

import gr.ntua.ece.cslab.multdimwavelets.nonstandard.HyperNode;
import gr.ntua.ece.cslab.multdimwavelets.nonstandard.NSWCoefficient;
import gr.ntua.ece.cslab.multdimwavelets.thresholding.greedy.BaseMultGreedyAbs;
import gr.ntua.ece.cslab.multdimwavelets.thresholding.structs.ErrorKeyWritable;
import gr.ntua.ece.cslab.multdimwavelets.thresholding.structs.ErrortreeNodeContents;
import gr.ntua.ece.cslab.multdimwavelets.thresholding.structs.MDErrorContainer;
import gr.ntua.ece.cslab.multdimwavelets.utils.WaveletConfProperties;

import java.io.IOException;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.mapreduce.Reducer;


public class BudgGreedyReducer extends
        Reducer<ErrorKeyWritable, NSWCoefficient, NSWCoefficient, NullWritable> {


    protected int count;
    protected int B;
    protected int[] subtreeIDs;
    protected int dim;

    private HashMap<Integer, HyperNode> rootTreeData;
    private HashMap<Integer, Queue<NSWCoefficient>> reserveList;
    private HashMap<Integer, ErrortreeNodeContents> rootTreeMap;
    private List<MDErrorContainer> rootTreeGreedyResult;
    private int keptForSure = 0;
    private int degreeOut;
    private int rootTreeSize;
    double minimumError;
    double lastKeptError;
    boolean cont = true;

    protected List<NSWCoefficient> synopsis;

    protected boolean flag;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        synopsis = new ArrayList<NSWCoefficient>();
        count = 0;

        Configuration conf = context.getConfiguration();
        dim = Integer.parseInt(conf.get(WaveletConfProperties.DIM));
        NSWCoefficient.initializeCoefs(dim);
        B = Integer.parseInt(conf.get(WaveletConfProperties.BUDGET));
        String subtrees = conf.get(WaveletConfProperties.SUBTREE_IDS);
        String[] subtreeRoots = subtrees.split(",");
        subtreeIDs = new int[subtreeRoots.length];
        for (int i = 0; i < subtreeRoots.length; i++) {
            subtreeIDs[i] = Integer.parseInt(subtreeRoots[i]);
        }

        flag = true;

        rootTreeData = new HashMap<>();
        reserveList = new HashMap<>();
        rootTreeMap = new HashMap<Integer, ErrortreeNodeContents>();
        URI[] cacheFiles = context.getCacheFiles();
        Path rootErrorTreePath = new Path(cacheFiles[0]);
        SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(rootErrorTreePath));
        HyperNode node = new HyperNode();
        while (reader.next(node, NullWritable.get())) {
            int hyperNodeIndex = node.getCoefficientNodes().get(0).getHyperNodeIndex();
            rootTreeMap.put(hyperNodeIndex, NSWCoefficient.getGreedyTreeNodeProperties(node, hyperNodeIndex));
        }
        reader.close();


        degreeOut = (int) Math.pow(2, dim);
        rootTreeSize = (rootTreeMap.size() - 1) * (degreeOut - 1) + 1;
        //run greedy stat and keep resulted combinations
        BaseMultGreedyAbs<ErrorKeyWritable, NSWCoefficient> greedy
                = new BaseMultGreedyAbs<ErrorKeyWritable, NSWCoefficient>(rootTreeMap, degreeOut, false);
        greedy.setB(rootTreeSize); // i need all the root tree, i.e., the coefs and not the hypernodes
        greedy.runGreedy();
        rootTreeGreedyResult = greedy.getSynopsis();


//		System.out.println("Root subtree last node: "+(subtreeIDs[0]-1));
//		System.out.println("Root tree size = "+rootTreeSize);
//		System.out.println("Budget = "+B);


    }


    @Override
    public void reduce(ErrorKeyWritable key, Iterable<NSWCoefficient> values, Context context)
            throws IOException, InterruptedException {

        Iterator<NSWCoefficient> iter = values.iterator();
        // i can release a maximum of rootTreeSize slots by thresholding the root tree
        while (count <= B + rootTreeSize && iter.hasNext()) {
            NSWCoefficient nextCoefficient = iter.next().clone();

            if (nextCoefficient.getHyperNodeIndex() >= subtreeIDs[0]) {
                // node not belonging in root sub tree
                if (!rootTreeData.containsKey(key.getSubtreeID())) {
                    // if root node for this subtree is not added yet to the array for the construction
                    // of the root subtree, then this node can be safely added to synopsis, as its error
                    // does not depend on whether we decide to keep or discard the corresponding root coefficient
                    context.write(nextCoefficient, NullWritable.get());
                    lastKeptError = nextCoefficient.getError();
                    keptForSure++;
                } else {
                    // the errors of these nodes will be affected by the possible deletion of the corresponding root coef
                    // and thus we temporarily move them to a "reserve" list
                    if (reserveList.containsKey(key.getSubtreeID())) {
                        reserveList.get(key.getSubtreeID()).add(nextCoefficient);
                    } else {
                        Queue<NSWCoefficient> subtreeResList = new LinkedList<>();
                        subtreeResList.add(nextCoefficient);
                        reserveList.put(key.getSubtreeID(), subtreeResList);
                    }
                }
            } else {
//				System.out.println("ID of node belonging in the root subtree: "+nextCoefficient.getHyperNodeIndex());
//				System.out.println("Came from subtree: "+key.getSubtreeID());
                // belonging to root subtree
                if (rootTreeData.containsKey(key.getSubtreeID())) {
                    rootTreeData.get(key.getSubtreeID()).addCoefficient(nextCoefficient);
                } else {
                    HyperNode hn = new HyperNode();
                    List<NSWCoefficient> coefList = new ArrayList<NSWCoefficient>();
                    hn.addCoefficient(nextCoefficient);
                    hn.setCoefficientNodes(coefList);
                    rootTreeData.put(key.getSubtreeID(), hn);
                }
            }


            //synopsis.add(nextCoefficient);
            count++;
        }


        if (flag && count > B + rootTreeSize) {
            //System.out.println("Time to test different root subtrees:");
            // make repeated update-check-merge to find the final synopsis.
            // if i keep all root subtree, no update is needed. just merge the reserve list


            //System.out.println("Last kept error = "+lastKeptError);
            minimumError = Integer.MAX_VALUE;
            List<MDErrorContainer> bestCombination = rootTreeGreedyResult; //all the errortree
            for (int i = 0; i <= rootTreeSize; i++) {
                List<MDErrorContainer> discardedRootCoefs = new ArrayList<>();
                for (int j = 0; j < i; j++) {
                    discardedRootCoefs.add(rootTreeGreedyResult.get(j));
                }
                //==================
//				System.out.println("Test for discarded from root Subtree: ");
//				for(MDErrorContainer md: discardedRootCoefs){System.out.println(md.getNodeIndex());}
                //==================
                double err = updateAndCheck(discardedRootCoefs);
                //System.out.println("error: "+err);
                //minimumError = (minimumError < err)? minimumError : err;
                if (err < minimumError) {
                    minimumError = err;
                    bestCombination = discardedRootCoefs;
                }
            }
            flag = false;
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        writeFinalErrorToHDFS("FINAL_ERROR", minimumError, context);
    }

    public void writeFinalErrorToHDFS(String synopsisErrorFile, double error, Context context) throws IllegalArgumentException, IOException {
        SequenceFile.Writer writer = SequenceFile.createWriter(context.getConfiguration(), Writer.file(new Path(synopsisErrorFile)),
                Writer.keyClass(DoubleWritable.class), Writer.valueClass(NullWritable.class));
        writer.append(new DoubleWritable(error), NullWritable.get());
        writer.close();
    }


    public double updateAndCheck(List<MDErrorContainer> discardedRootCoefs) {
        int keptFromRoot = rootTreeSize - discardedRootCoefs.size();
        int alreadyKept = keptForSure + keptFromRoot;

        HashMap<Integer, Queue<NSWCoefficient>> testList = new HashMap<>();
        for (Map.Entry<Integer, Queue<NSWCoefficient>> e : reserveList.entrySet()) {
            Queue<NSWCoefficient> nqueue = new LinkedList<>();
            for (NSWCoefficient c : e.getValue()) {
                nqueue.add(c);
            }
            testList.put(e.getKey(), nqueue);
        }

        // update test list

        for (Map.Entry<Integer, Queue<NSWCoefficient>> e : testList.entrySet()) {
            int key = e.getKey();
            //System.out.println("Subtree: " + e.getKey() + " key:" + key);
            for (MDErrorContainer dc : discardedRootCoefs) {
                double updateValue = dc.getValue();
                int dcIndex = dc.getHyperNodeIndex();
                int currentKey = key;

                int parent = currentKey / degreeOut;
                while (parent >= 0) {
                    int parentSignIndex = (currentKey != 1) ? currentKey % degreeOut : 0;
                    if (parent == dcIndex) {
                        //System.out.println("Discarded node: " + dcIndex + " with value " + updateValue);

                        updateValue *= (-1) * NSWCoefficient.signsPerChild[dc.getInternalNodeIndex()][parentSignIndex];
                        //System.out.println("Update tree by " + updateValue);
                        break;
                    }
                    if (parent == 0) break;
                    currentKey = parent;
                    parent /= degreeOut;
                }
                // update value is the signed error coming from the root choice.

                for (NSWCoefficient c : e.getValue()) {
                    c.minerror += updateValue;
                    c.maxerror += updateValue;
                    c.setError(Math.max(Math.abs(c.minerror), Math.abs(c.maxerror)));
                }
            }
        }

        CoefSubtree selectedCoef = findMostImportantCoef(testList);
        //double lastError = (keptForSure > 0)? lastKeptError : Integer.MAX_VALUE;
        double lastError = Integer.MAX_VALUE;
        while (selectedCoef != null && alreadyKept <= B) {
            lastError = selectedCoef.getCoef().getError();
            alreadyKept++;
            selectedCoef = findMostImportantCoef(testList);
        }

        //System.out.println("Finally Kept = "+alreadyKept);

        return lastError;
    }

    public CoefSubtree findMostImportantCoef(HashMap<Integer, Queue<NSWCoefficient>> clist) {
        double maxError = Integer.MIN_VALUE;
        int subtree = -1;
        for (Map.Entry<Integer, Queue<NSWCoefficient>> subtreeList : clist.entrySet()) {
            NSWCoefficient topCoefofSubtree = subtreeList.getValue().peek(); // most important coef of block
            if (topCoefofSubtree != null) {
                if (topCoefofSubtree.getError() > maxError) {
                    maxError = topCoefofSubtree.getError();
                    subtree = subtreeList.getKey();
                }
            } else {
                return null;
            }
        }
        NSWCoefficient selectedCoef = clist.get(subtree).poll();
        if (selectedCoef == null) return null;
        CoefSubtree coef = new CoefSubtree(selectedCoef, subtree);
        return coef;
    }

    protected class CoefSubtree {
        private NSWCoefficient coef;
        private int subtree;

        public CoefSubtree(NSWCoefficient coef, int subtree) {
            this.coef = coef;
            this.subtree = subtree;
        }

        /**
         * @return the coef
         */
        public NSWCoefficient getCoef() {
            return coef;
        }

        /**
         * @param coef the coef to set
         */
        public void setCoef(NSWCoefficient coef) {
            this.coef = coef;
        }

        /**
         * @return the subtree
         */
        public int getSubtree() {
            return subtree;
        }

        /**
         * @param subtree the subtree to set
         */
        public void setSubtree(int subtree) {
            this.subtree = subtree;
        }


    }


}
