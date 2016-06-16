package inf.ed.gfd.detect.repl;

import inf.ed.gfd.detect.dist.ViolationResult;
import inf.ed.gfd.structure.GFD2;
import inf.ed.gfd.structure.WorkUnit;
import inf.ed.gfd.util.KV;
import inf.ed.gfd.util.Params;
import inf.ed.gfd.util.Stat;
import inf.ed.grape.communicate.Client2Coordinator;
import inf.ed.grape.communicate.Worker;
import inf.ed.grape.communicate.Worker2Coordinator;
import inf.ed.grape.interfaces.Result;
import inf.ed.graph.structure.Graph;
import inf.ed.graph.structure.OrthogonalEdge;
import inf.ed.graph.structure.OrthogonalGraph;
import inf.ed.graph.structure.adaptor.VertexOString;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.io.File;
import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The Class Coordinator.
 * 
 * @author yecol
 */
public class ReplicatedGCoordinator extends UnicastRemoteObject implements Worker2Coordinator,
		Client2Coordinator {

	private static final long serialVersionUID = 1L;

	/** The total number of worker threads. */
	private static AtomicInteger totalWorkerThreads = new AtomicInteger(0);

	/** The workerID to WorkerProxy map. */
	protected Map<String, ReplicatedGWorkerProxy> workerProxyMap = new ConcurrentHashMap<String, ReplicatedGWorkerProxy>();

	/** The workerID to Worker map. **/
	private Map<String, Worker> workerMap = new HashMap<String, Worker>();

	/** The partitionID to workerID map. **/
	private Map<Integer, String> partitionWorkerMap;

	/** The virtual vertexID to partitionID map. */
	private Map<Integer, Integer> virtualVertexPartitionMap;

	/** Set of Workers maintained for acknowledgement. */
	private Set<String> workerAcknowledgementSet = new HashSet<String>();

	/** Set of workers who will be active in the next super step. */
	private Set<String> activeWorkerSet = new HashSet<String>();

	/** Set of partial results. partitionID to Results **/
	private Map<Integer, Result> resultMap = new HashMap<Integer, Result>();

	/** The start time. */
	long wholeStartTime;
	long assignStartTime;
	long localStartTime;
	long firstPartialResultArrivalTime;
	boolean isFirstPartialResult = true;

	long superstep = 0;

	boolean firstTimeReceive = true;
	long receiveStart = 0;

	private String finalResultSuffix = "";

	private Graph<VertexOString, OrthogonalEdge> KB;

	static Logger log = LogManager.getLogger(ReplicatedGCoordinator.class);

	/**
	 * Instantiates a new coordinator.
	 * 
	 * @throws RemoteException
	 *             the remote exception
	 * @throws PropertyNotFoundException
	 *             the property not found exception
	 */
	public ReplicatedGCoordinator() throws RemoteException {
		super();

	}

	/**
	 * Gets the active worker set.
	 * 
	 * @return the active worker set
	 */
	public Set<String> getActiveWorkerSet() {
		return activeWorkerSet;
	}

	/**
	 * Sets the active worker set.
	 * 
	 * @param activeWorkerSet
	 *            the new active worker set
	 */
	public void setActiveWorkerSet(Set<String> activeWorkerSet) {
		this.activeWorkerSet = activeWorkerSet;
	}

	/**
	 * Registers the worker computation nodes with the master.
	 * 
	 * @param worker
	 *            Represents the {@link Setting2Worker.WorkerImpl Worker}
	 * @param workerID
	 *            the worker id
	 * @param numWorkerThreads
	 *            Represents the number of worker threads available in the
	 *            worker computation node
	 * @return worker2 master
	 * @throws RemoteException
	 *             the remote exception
	 */
	@Override
	public Worker2Coordinator register(Worker worker, String workerID, int numWorkerThreads)
			throws RemoteException {

		log.debug("Register");
		totalWorkerThreads.getAndAdd(numWorkerThreads);
		ReplicatedGWorkerProxy workerProxy = new ReplicatedGWorkerProxy(worker, workerID,
				numWorkerThreads, this);
		workerProxyMap.put(workerID, workerProxy);
		workerMap.put(workerID, worker);
		return (Worker2Coordinator) UnicastRemoteObject.exportObject(workerProxy, 0);
	}

	/**
	 * Gets the worker proxy map info.
	 * 
	 * @return Returns the worker proxy map info
	 */
	public Map<String, ReplicatedGWorkerProxy> getWorkerProxyMap() {
		return workerProxyMap;
	}

	/**
	 * Send worker partition info.
	 * 
	 * @throws RemoteException
	 *             the remote exception
	 */
	public void sendWorkerPartitionInfo() throws RemoteException {
		log.debug("sendWorkerPartitionInfo");
		for (Map.Entry<String, ReplicatedGWorkerProxy> entry : workerProxyMap.entrySet()) {
			ReplicatedGWorkerProxy workerProxy = entry.getValue();
			workerProxy.setWorkerPartitionInfo(virtualVertexPartitionMap, partitionWorkerMap,
					workerMap);
		}
	}

	/**
	 * The main method.
	 * 
	 * @param args
	 *            the arguments
	 * @throws Exception
	 *             the exception
	 */
	public static void main(String[] args) throws Exception {
		if (args.length < 3) {
			System.out.println("paras: config-file, n, opt");
			System.exit(0);
		}
		Params.CONFIG_FILENAME = args[0].trim();
		Params.N_PROCESSORS = Integer.parseInt(args[1].trim());
		Params.RUN_MODE = Integer.parseInt(args[2].trim());
		log.debug("PARAM_CONFIG_FILE = " + Params.CONFIG_FILENAME);
		log.debug("PARAM_N = " + Params.N_PROCESSORS);
		log.debug("PARAM_RUN_MODE = " + Params.RUN_MODE);
		log.debug("Processing REPLICATED graph = " + KV.DATASET);
		System.setSecurityManager(new SecurityManager());
		ReplicatedGCoordinator coordinator;
		Stat.getInstance().setting = KV.SETTING_REPLICATE;
		try {
			coordinator = new ReplicatedGCoordinator();
			Registry registry = LocateRegistry.createRegistry(KV.RMI_PORT);
			registry.rebind(KV.COORDINATOR_SERVICE_NAME, coordinator);
			log.info("instance is bound to " + KV.RMI_PORT + " and ready.");
			SimpleDateFormat simpleDateFormat = new SimpleDateFormat("MMddHHmm");
			coordinator.finalResultSuffix = simpleDateFormat.format(new Date());
		} catch (RemoteException e) {
			ReplicatedGCoordinator.log.error(e);
			e.printStackTrace();
		}
	}

	/**
	 * Halts all the workers and prints the final solution.
	 * 
	 * @throws RemoteException
	 *             the remote exception
	 */
	public void halt() throws RemoteException {
		log.debug("Worker Proxy Map " + workerProxyMap);

		for (Map.Entry<String, ReplicatedGWorkerProxy> entry : workerProxyMap.entrySet()) {
			ReplicatedGWorkerProxy workerProxy = entry.getValue();
			workerProxy.halt();
		}

		// healthManager.exit();
		// long endTime = System.currentTimeMillis();
		// log.info("Time taken: " + (endTime - startTime) + " ms");
		// Restore the system back to its initial state
		restoreInitialState();
	}

	/**
	 * Restore initial state of the system.
	 */
	private void restoreInitialState() {
		this.activeWorkerSet.clear();
		this.workerAcknowledgementSet.clear();
		this.partitionWorkerMap.clear();
		this.superstep = 0;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Runnable#run()
	 */

	/**
	 * Removes the worker.
	 * 
	 * @param workerID
	 *            the worker id
	 */
	public void removeWorker(String workerID) {
		workerProxyMap.remove(workerID);
		workerMap.remove(workerID);
	}

	/**
	 * Gets the partition worker map.
	 * 
	 * @return the partition worker map
	 */
	public Map<Integer, String> getPartitionWorkerMap() {
		return partitionWorkerMap;
	}

	/**
	 * Sets the partition worker map.
	 * 
	 * @param partitionWorkerMap
	 *            the partition worker map
	 */
	public void setPartitionWorkerMap(Map<Integer, String> partitionWorkerMap) {
		this.partitionWorkerMap = partitionWorkerMap;
	}

	/**
	 * Defines a deployment convenience to stop each registered.
	 * 
	 * @throws RemoteException
	 *             the remote exception {@link system.Worker Worker} and then
	 *             stops itself.
	 */

	@Override
	public void shutdown() throws RemoteException {
		// if (healthManager != null)
		// healthManager.exit();
		for (Map.Entry<String, ReplicatedGWorkerProxy> entry : workerProxyMap.entrySet()) {
			ReplicatedGWorkerProxy workerProxy = entry.getValue();
			try {
				workerProxy.shutdown();
			} catch (Exception e) {
				continue;
			}
		}
		java.util.Date date = new java.util.Date();
		log.info("goes down now at :" + new Timestamp(date.getTime()));
		System.exit(0);
	}

	/**
	 * Assign distributed partitions to workers, assuming graph has been
	 * partitioned and distributed.
	 */
	public void distributedLoadWholeGraph() {

		log.info("begin distributed load whole graph.");

		partitionWorkerMap = new HashMap<Integer, String>();
		int partitionID = 0;

		for (Map.Entry<String, ReplicatedGWorkerProxy> entry : workerProxyMap.entrySet()) {

			ReplicatedGWorkerProxy workerProxy = entry.getValue();

			activeWorkerSet.add(entry.getKey());
			log.info("assign partition " + partitionID + " to worker " + workerProxy.getWorkerID());

			// Compute the number of partitions to assign
			int numThreads = workerProxy.getNumThreads();

			log.info("Worker " + workerProxy.getWorkerID() + " with " + numThreads
					+ " threads got " + numThreads + " partition.");

			partitionWorkerMap.put(partitionID, workerProxy.getWorkerID());
			workerProxy.loadWholeGraph(partitionID);
			partitionID++;
		}
	}

	/**
	 * Start super step.
	 * 
	 * @throws RemoteException
	 *             the remote exception
	 */
	public void nextLocalCompute() throws RemoteException {
		log.info("next local compute. superstep = " + superstep);

		firstTimeReceive = true;
		this.workerAcknowledgementSet.clear();
		this.workerAcknowledgementSet.addAll(this.activeWorkerSet);

		for (String workerID : this.activeWorkerSet) {
			this.workerProxyMap.get(workerID).nextLocalCompute(superstep);
		}
		this.activeWorkerSet.clear();
	}

	@Override
	public synchronized void localComputeCompleted(String workerID, Set<String> activeWorkerIDs)
			throws RemoteException {

		if (firstTimeReceive) {
			firstTimeReceive = false;
			firstPartialResultArrivalTime = System.currentTimeMillis();
		}
		log.info("receive acknowledgement from worker " + workerID + "\n saying activeWorkers: "
				+ activeWorkerIDs.toString());

		// in synchronised model, coordinator receive activeWorkers and
		// prepare for next round of local computations.
		// this.activeWorkerSet.addAll(activeWorkerIDs);
		this.workerAcknowledgementSet.remove(workerID);

		if (this.workerAcknowledgementSet.size() == 0) {
			Stat.getInstance().finishGapTime = (System.currentTimeMillis() - firstPartialResultArrivalTime) * 1.0 / 1000;
			superstep++;
			if (activeWorkerSet.size() != 0)
				nextLocalCompute();
			else {
				finishLocalCompute();
			}
		}
	}

	public void finishLocalCompute() throws RemoteException {

		this.resultMap.clear();

		this.workerAcknowledgementSet.clear();
		this.workerAcknowledgementSet.addAll(this.workerProxyMap.keySet());

		for (Map.Entry<String, ReplicatedGWorkerProxy> entry : workerProxyMap.entrySet()) {
			ReplicatedGWorkerProxy workerProxy = entry.getValue();
			workerProxy.processPartialResult();
		}

		log.info("finish local compute. with round = " + superstep);

	}

	public synchronized void receivePartialResults(String workerID,
			Map<Integer, Result> mapPartitionID2Result) {

		log.debug("receive partitial result from " + workerID);

		for (Entry<Integer, Result> entry : mapPartitionID2Result.entrySet()) {
			resultMap.put(entry.getKey(), entry.getValue());
		}

		this.workerAcknowledgementSet.remove(workerID);

		if (this.workerAcknowledgementSet.size() == 0) {

			log.info("assemble the final result");
			Stat.getInstance().localDetectViolationTime = (System.currentTimeMillis() - localStartTime) * 1.0 / 1000;

			try {

				ViolationResult finalResult = new ViolationResult();
				finalResult.assemblePartialResults(resultMap.values());
				finalResult.writeToFile(KV.OUTPUT_DIR + "/FINAL" + "-ReplG-P"
						+ this.workerMap.size() + "-" + finalResultSuffix + ".dat");
				Stat.getInstance().totalViolation = finalResult.getViolations().size();

			} catch (Exception e) {
				e.printStackTrace();
			}

			Stat.getInstance().totalTime = (System.currentTimeMillis() - wholeStartTime) * 1.0 / 1000;
			log.info(Stat.getInstance().getInfo());
			log.debug("Done!");
		}
	}

	public void preProcess() throws RemoteException {

		wholeStartTime = System.currentTimeMillis();

		KB = new OrthogonalGraph<VertexOString>(VertexOString.class);
		KB.loadGraphFromVEFile(KV.GRAPH_FILE_PATH, true);
		distributedLoadWholeGraph();

		process();
	}

	@Override
	public void process() throws RemoteException {
		// begin process.
		List<GFD2> queries = readGFDFromDir();
		log.info("load " + queries.size() + " gfds from: " + KV.QUERY_DIR_PATH);
		Stat.getInstance().getInputFilesLocalAndDistributedTime = (System.currentTimeMillis() - wholeStartTime) * 1.0 / 1000;

		long findCandidateStart = System.currentTimeMillis();
		for (GFD2 gfd : queries) {
			gfd.findCandidates(this.KB);
		}
		log.info("found all the candidates.");
		Stat.getInstance().findCandidatesTime = (System.currentTimeMillis() - findCandidateStart) * 1.0 / 1000;

		assignStartTime = System.currentTimeMillis();
		// read gfds from query dir

		// using bPar, assign jobs with work unit.
		if (Params.RUN_MODE == Params.VAR_OPT) {
			assignQueriesbPar(queries);
		} else {
			assignQueriesRandom(queries);
		}

		Stat.getInstance().jobAssignmentTime = (System.currentTimeMillis() - assignStartTime) * 1.0 / 1000;
		localStartTime = System.currentTimeMillis();

		nextLocalCompute();
	}

	public List<GFD2> readGFDFromDir() {
		List<GFD2> ret = new LinkedList<GFD2>();
		File dir = new File(KV.QUERY_DIR_PATH);
		List<File> files = (List<File>) FileUtils.listFiles(dir, TrueFileFilter.INSTANCE, null);
		for (File file : files) {
			try {
				GFD2 gfd = new GFD2();
				gfd.setID(file.getName());
				gfd.readFromFile(file.getCanonicalPath());
				ret.add(gfd);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		Stat.getInstance().totalGfdCount = ret.size();
		return ret;
	}

	public void assignQueriesNaive(List<GFD2> queries) throws RemoteException {
		log.debug("distribute Query to workers.");
		int i = 0;
		int m = workerProxyMap.size();
		for (Map.Entry<String, ReplicatedGWorkerProxy> entry : workerProxyMap.entrySet()) {
			List<GFD2> assignment = new ArrayList<GFD2>();
			for (int j = 0; j < queries.size(); j++) {
				if (j % m == i) {
					assignment.add(queries.get(j));
				}
			}
			if (assignment.size() != 0) {
				ReplicatedGWorkerProxy workerProxy = entry.getValue();
				workerProxy.setQueryWithGFD(assignment);
			}
			i++;
		}
		log.debug("distribute Query to workers round-dis");
	}

	public void assignQueriesRandom(List<GFD2> queries) throws RemoteException {

		log.debug("begin assign random.");

		Set<ReplicatedGWorkUnit> workLoads = new HashSet<ReplicatedGWorkUnit>();

		int i = 0;

		for (GFD2 gfd : queries) {

			log.debug("estimate " + gfd.getID() + ", with " + gfd.getCandidates().get(0).size()
					+ " cands, " + (i++) + "/" + queries.size());

			if (gfd.isConnected()) {
				for (int pivot : gfd.getCandidates().keySet()) {
					for (int cand : gfd.getCandidates().get(pivot)) {
						ReplicatedGWorkUnit wu = new ReplicatedGWorkUnit(gfd.getID(), pivot, cand);
						wu.estimateET(KB, queries);
						workLoads.add(wu);
					}
				}
			} else {
				// gfd is not connected;
				// we fix two pivot as 0 and 10;
				int pivot1 = 0;
				int pivot2 = 10;

				for (int cand1 : gfd.getCandidates().get(pivot1)) {
					for (int cand2 : gfd.getCandidates().get(pivot2)) {

						// only if cand1 not equals to cand2 and they satisfy
						// the condition then build a new workunit.
						if (cand1 != cand2 && gfd.verify2Candidate(cand1, cand2, this.KB)) {
							ReplicatedGWorkUnit wu = new ReplicatedGWorkUnit(gfd.getID(), pivot1,
									cand1, pivot2, cand2);
							wu.estimateET(KB, queries);
							workLoads.add(wu);
						}
					}
				}
			}
		}

		int machineNum = workerProxyMap.size();

		Int2ObjectMap<Set<ReplicatedGWorkUnit>> assignment = new Int2ObjectOpenHashMap<Set<ReplicatedGWorkUnit>>();
		Random r = new Random();

		for (ReplicatedGWorkUnit wu : workLoads) {
			int assignedMachine = r.nextInt(machineNum);
			if (!assignment.containsKey(assignedMachine)) {
				assignment.put(assignedMachine, new HashSet<ReplicatedGWorkUnit>());
			}
			assignment.get(assignedMachine).add(wu);
		}

		Stat.getInstance().totalWorkUnit = workLoads.size();
		log.debug("finished assigment.");

		for (int machineID : assignment.keySet()) {
			// here machineID = partitionID
			String workerID = partitionWorkerMap.get(machineID);
			ReplicatedGWorkerProxy workerProxy = workerProxyMap.get(workerID);
			workerProxy.setQueryWithWorkUnit(assignment.get(machineID), queries);
			log.info("now sent RANDOM assigment for machine " + machineID + " on " + workerID);

		}
	}

	public void assignQueriesbPar(List<GFD2> queries) throws RemoteException {

		log.debug("begin assign bpar.");

		PriorityQueue<ReplicatedGWorkUnit> workLoads = new PriorityQueue<ReplicatedGWorkUnit>();

		int i = 0;

		for (GFD2 gfd : queries) {

			log.debug("estimate " + gfd.getID() + ", with " + gfd.getCandidates().get(0).size()
					+ " cands, " + (i++) + "/" + queries.size());

			if (gfd.isConnected()) {
				for (int pivot : gfd.getCandidates().keySet()) {
					for (int cand : gfd.getCandidates().get(pivot)) {
						ReplicatedGWorkUnit wu = new ReplicatedGWorkUnit(gfd.getID(), pivot, cand);
						wu.estimateET(KB, queries);
						if (wu.getET() != 0) {
							workLoads.add(wu);
						}
					}
				}
			} else {
				// gfd is not connected;
				// we fix two pivot as 0 and 10;
				int pivot1 = 0;
				int pivot2 = 10;

				for (int cand1 : gfd.getCandidates().get(pivot1)) {
					for (int cand2 : gfd.getCandidates().get(pivot2)) {

						// only if cand1 not equals to cand2 and they satisfy
						// the condition then build a new workunit.
						if (cand1 != cand2 && gfd.verify2Candidate(cand1, cand2, this.KB)) {
							ReplicatedGWorkUnit wu = new ReplicatedGWorkUnit(gfd.getID(), pivot1,
									cand1, pivot2, cand2);
							wu.estimateET(KB, queries);
							if (wu.getET() != 0) {
								workLoads.add(wu);
							}
						}
					}
				}
			}
		}

		bPar bParInstance = new bPar();
		int machineNum = workerProxyMap.size();

		Stat.getInstance().totalWorkUnit = workLoads.size();

		Int2ObjectMap<Set<ReplicatedGWorkUnit>> assignment = bParInstance.makespan(machineNum,
				workLoads);
		log.debug("finished assigment.");

		for (int machineID : assignment.keySet()) {
			// here machineID = partitionID
			String workerID = partitionWorkerMap.get(machineID);
			ReplicatedGWorkerProxy workerProxy = workerProxyMap.get(workerID);
			workerProxy.setQueryWithWorkUnit(assignment.get(machineID), queries);
			log.info("now sent BPAR assigment for machine " + machineID + " on " + workerID);

		}
	}

	@Override
	public void postProcess() throws RemoteException {
		// TODO Auto-generated method stub

	}

	@Override
	public void sendPartialResult(String workerID, Map<Integer, Result> mapPartitionID2Result,
			double communicationData) throws RemoteException {
	}

	@Override
	public void sendPartialWorkunitsAndBorderInfo(String workerID, Set<WorkUnit> partialWorkunits,
			Int2IntMap bordernodeBallSize, double findCandidateTime) throws RemoteException {
		// TODO Auto-generated method stub

	}

}
