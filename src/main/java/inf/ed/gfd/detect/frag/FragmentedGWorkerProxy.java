package inf.ed.gfd.detect.frag;

import inf.ed.gfd.structure.CrossingEdge;
import inf.ed.gfd.structure.GFD2;
import inf.ed.gfd.structure.Partition;
import inf.ed.gfd.structure.WorkUnit;
import inf.ed.grape.communicate.Worker;
import inf.ed.grape.communicate.Worker2Coordinator;
import inf.ed.grape.interfaces.Result;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.rmi.AccessException;
import java.rmi.RemoteException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Represents a thread which is used by the master to talk to workers and
 * vice-versa.
 * 
 * @author Yecol
 */

public class FragmentedGWorkerProxy implements Runnable, Worker2Coordinator {

	private static final long serialVersionUID = 1L;

	/** The worker. */
	private Worker worker;

	/** The coordinator. */
	private FragmentedGCoordinator coordinator;

	/** The thread */
	private Thread t;

	/** The num worker threads. */
	private int numWorkerThreads;

	/** The worker id. */
	String workerID;

	/** The partition list. */
	BlockingQueue<Partition> partitionList;

	/** The partition list. */
	BlockingQueue<Integer> partitionIDList;

	/** The total partitions. */
	private int totalPartitions = 0;

	static Logger log = LogManager.getLogger(FragmentedGWorkerProxy.class);

	/**
	 * Instantiates a new worker proxy.
	 */

	public FragmentedGWorkerProxy(Worker worker, String workerID, int numWorkerThreads,
			FragmentedGCoordinator coordinator) throws AccessException, RemoteException {
		this.worker = worker;
		this.workerID = workerID;
		this.numWorkerThreads = numWorkerThreads;
		this.coordinator = coordinator;
		partitionList = new LinkedBlockingQueue<Partition>();
		partitionIDList = new LinkedBlockingQueue<Integer>();
		t = new Thread(this);
		t.start();
	}

	@Override
	public void run() {
		Partition partition = null;
		while (true) {
			try {
				partition = partitionList.take();
				log.info("Partition taken");
				worker.addPartition(partition);
			} catch (RemoteException e) {
				log.fatal("Remote Exception received from the Worker " + workerID);
				coordinator.removeWorker(workerID);
			} catch (InterruptedException e) {
				log.fatal("InterruptedException: Removing Worker " + workerID + " from Master");
				coordinator.removeWorker(workerID);
			}
		}
	}

	/**
	 * Exit.
	 */
	public void exit() {
		try {
			t.interrupt();
		} catch (Exception e) {
			System.out.println("Worker Stopped");
		}
	}

	/**
	 * Gets the num threads.
	 * 
	 * @return the num threads
	 */
	public int getNumThreads() {
		return numWorkerThreads;
	}

	/**
	 * Halts the worker and prints the final solution.
	 */
	public void halt() throws RemoteException {
		this.restoreInitialState();
		worker.halt();
	}

	/**
	 * Adds the partition.
	 * 
	 * @param partition
	 *            the partition
	 */
	public void addPartition(Partition partition) {

		totalPartitions += 1;
		partitionList.add(partition);
	}

	/**
	 * Adds the partition.
	 * 
	 * @param partition
	 *            the partition
	 */
	public void addPartitionID(int partitionID) {
		totalPartitions += 1;
		partitionIDList.add(partitionID);
		try {
			worker.addPartitionID(partitionID);
		} catch (RemoteException e) {
			log.fatal("Remote Exception received from the Worker.");
			e.printStackTrace();
		}
	}

	/**
	 * Sets the worker partition info.
	 * 
	 * @param mapPartitionIdToWorkerId
	 *            the map partition id to worker id
	 * @param mapWorkerIdToWorker
	 *            the map worker id to worker
	 * @throws RemoteException
	 *             the remote exception
	 */
	public void setWorkerPartitionInfo(Map<Integer, Integer> vertexIdToPartitionId,
			Map<Integer, String> mapPartitionIdToWorkerId, Map<String, Worker> mapWorkerIdToWorker)
			throws RemoteException {
		worker.setWorkerPartitionInfo(totalPartitions, vertexIdToPartitionId,
				mapPartitionIdToWorkerId, mapWorkerIdToWorker);
	}

	/**
	 * Sets Query to
	 */

	public void setQueryWithGFD(List<GFD2> queries) throws RemoteException {
		worker.setQueryWithGFD2(queries);
	}

	public void setWorkUnitsAndPrefetchRequest(Set<WorkUnit> workload,
			Int2ObjectMap<IntSet> prefetchRequests, Set<CrossingEdge> crossingEdges)
			throws RemoteException {
		worker.setWorkUnitsAndPrefetchRequest(workload, prefetchRequests, crossingEdges);
	}

	public void loadWholeGraph(int partitionID) {
		try {
			worker.loadWholeGraph(partitionID);
		} catch (RemoteException e) {
			log.fatal("Remote Exception when loading whole graph.");
			e.printStackTrace();
			// give the partition back to Master
			coordinator.removeWorker(workerID);
			return;
		}
	}

	/**
	 * Gets the worker id.
	 * 
	 * @return the worker id
	 */
	public String getWorkerID() {
		return workerID;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see system.Worker2Master#register(system.Worker, java.lang.String, int)
	 */
	@Override
	public Worker2Coordinator register(Worker worker, String workerID, int numWorkerThreads)
			throws RemoteException {
		return null;
	}

	/**
	 * Restore initial state.
	 */
	private void restoreInitialState() {
		this.totalPartitions = 0;
	}

	/**
	 * Shutdowns the worker and exits
	 */
	@Override
	public void shutdown() {
		try {
			worker.shutdown();
		} catch (RemoteException e) {
			this.exit();
		}
	}

	@Override
	public void localComputeCompleted(String workerID, Set<String> activeWorkerIDs)
			throws RemoteException {
		this.coordinator.localComputeCompleted(workerID, activeWorkerIDs);
	}

	public void nextLocalCompute(long superstep) throws RemoteException {
		this.worker.nextLocalCompute(superstep);
	}

	public void processPartialResult() throws RemoteException {
		this.worker.processPartialResult();
	}

	@Override
	public void sendPartialWorkunitsAndBorderInfo(String workerID, Set<WorkUnit> partialWorkunits,
			Int2IntMap bordernodeBallSize, double findCandidateTime){
		// TODO Auto-generated method stub
		try {
			this.coordinator.receivePartialWorkunitsFromWorkers(workerID, partialWorkunits,
					bordernodeBallSize, findCandidateTime);
		} catch (Exception e) {
			log.trace(e);
			e.printStackTrace();
			File file = new File("error.txt");
			file.getParentFile().mkdirs();

			PrintWriter printWriter;
			try {
				printWriter = new PrintWriter(file);
				e.printStackTrace(printWriter);
			} catch (FileNotFoundException e1) {
				System.out.println("no file to record error.");
			}

		}
	}

	@Override
	public void sendPartialResult(String workerID, Map<Integer, Result> mapPartitionID2Result,
			double communicationData) throws RemoteException {
		// TODO Auto-generated method stub
		this.coordinator.receivePartialResults(workerID, mapPartitionID2Result, communicationData);
	}
}
