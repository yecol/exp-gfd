package inf.ed.gfd.detect.frag;

import inf.ed.gfd.detect.dist.ViolationResult;
import inf.ed.gfd.structure.CrossingEdge;
import inf.ed.gfd.structure.GFD2;
import inf.ed.gfd.structure.WorkUnit;
import inf.ed.gfd.util.KV;
import inf.ed.gfd.util.Params;
import inf.ed.gfd.util.Stat;
import inf.ed.grape.communicate.Client2Coordinator;
import inf.ed.grape.communicate.Worker;
import inf.ed.grape.communicate.Worker2Coordinator;
import inf.ed.grape.interfaces.Result;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;

import java.io.File;
import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.carrotsearch.sizeof.RamUsageEstimator;

/**
 * The Class Coordinator.
 * 
 * @author yecol
 */
public class FragmentedGCoordinator extends UnicastRemoteObject implements Worker2Coordinator,
		Client2Coordinator {

	private static final long serialVersionUID = 1L;

	/** The total number of worker threads. */
	private static AtomicInteger totalWorkerThreads = new AtomicInteger(0);

	/** The workerID to WorkerProxy map. */
	protected Map<String, FragmentedGWorkerProxy> workerProxyMap = new ConcurrentHashMap<String, FragmentedGWorkerProxy>();

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
	long localStartTime;
	long firstPartialResultArrivalTime;
	boolean isFirstPartialResult = true;

	long superstep = 0;

	private String finalResultSuffix = "";

	/** for project gfd **/
	private Int2ObjectMap<IntSet> allBorderVertices = new Int2ObjectOpenHashMap<IntSet>();
	private Int2IntMap borderBallSize = new Int2IntOpenHashMap();
	private Set<WorkUnit> workunits = new HashSet<WorkUnit>();
	private Int2ObjectMap<String> allVertices = new Int2ObjectOpenHashMap<String>();
	private Int2ObjectMap<Set<CrossingEdge>> mapBorderNodesAsSource = new Int2ObjectOpenHashMap<Set<CrossingEdge>>();
	private Int2ObjectMap<Set<CrossingEdge>> mapBorderNodesAsTarget = new Int2ObjectOpenHashMap<Set<CrossingEdge>>();
	private Int2IntMap allborderBallSize = new Int2IntOpenHashMap();

	// private Graph<VertexOString, OrthogonalEdge> KB;

	static Logger log = LogManager.getLogger(FragmentedGCoordinator.class);

	/**
	 * Instantiates a new coordinator.
	 * 
	 * @throws RemoteException
	 *             the remote exception
	 * @throws PropertyNotFoundException
	 *             the property not found exception
	 */
	public FragmentedGCoordinator() throws RemoteException {
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

		log.debug("Worker " + workerID + " registered and ready to work!");
		totalWorkerThreads.getAndAdd(numWorkerThreads);
		FragmentedGWorkerProxy workerProxy = new FragmentedGWorkerProxy(worker, workerID,
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
	public Map<String, FragmentedGWorkerProxy> getWorkerProxyMap() {
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
		for (Map.Entry<String, FragmentedGWorkerProxy> entry : workerProxyMap.entrySet()) {
			FragmentedGWorkerProxy workerProxy = entry.getValue();
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
		log.debug("Processing FRAGMENTED graph = " + KV.DATASET);
		System.setSecurityManager(new SecurityManager());
		FragmentedGCoordinator coordinator;
		Stat.getInstance().setting = KV.SETTING_FRAGMENT;
		try {
			coordinator = new FragmentedGCoordinator();
			Registry registry = LocateRegistry.createRegistry(KV.RMI_PORT);
			registry.rebind(KV.COORDINATOR_SERVICE_NAME, coordinator);
			log.info("Coordinator listen to " + KV.RMI_PORT + " and ready to work!");
			SimpleDateFormat simpleDateFormat = new SimpleDateFormat("MMddHHmm");
			coordinator.finalResultSuffix = simpleDateFormat.format(new Date());
		} catch (RemoteException e) {
			FragmentedGCoordinator.log.error(e);
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

		for (Map.Entry<String, FragmentedGWorkerProxy> entry : workerProxyMap.entrySet()) {
			FragmentedGWorkerProxy workerProxy = entry.getValue();
			workerProxy.halt();
		}
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
		for (Map.Entry<String, FragmentedGWorkerProxy> entry : workerProxyMap.entrySet()) {
			FragmentedGWorkerProxy workerProxy = entry.getValue();
			try {
				workerProxy.shutdown();
			} catch (Exception e) {
				continue;
			}
		}
		System.exit(0);
	}

	/**
	 * Assign distributed partitions to workers, assuming graph has been
	 * partitioned and distributed.
	 */
	public void distributedLoadWholeGraph() {
		throw new IllegalArgumentException("No distribute whole graph in this setting.");
	}

	public void assignDistributedPartitions() {

		log.info("begin assign distributed partitions.");
		partitionWorkerMap = new HashMap<Integer, String>();
		// Assign partitions to workers in the ratio of the number of worker
		// threads that each worker has.
		assert workerProxyMap.size() == Params.N_PROCESSORS;

		int partitionID = 0;
		for (Map.Entry<String, FragmentedGWorkerProxy> entry : workerProxyMap.entrySet()) {

			FragmentedGWorkerProxy workerProxy = entry.getValue();
			activeWorkerSet.add(entry.getKey());
			log.debug("assign partition " + partitionID + " to worker " + workerProxy.getWorkerID());
			partitionWorkerMap.put(partitionID, workerProxy.getWorkerID());
			workerProxy.addPartitionID(partitionID);
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
		log.info("---------------next begin super step " + superstep + "---------------");
		this.workerAcknowledgementSet.clear();
		this.workerAcknowledgementSet.addAll(this.activeWorkerSet);

		isFirstPartialResult = true;

		for (String workerID : this.activeWorkerSet) {
			this.workerProxyMap.get(workerID).nextLocalCompute(superstep);
		}
		this.activeWorkerSet.clear();
	}

	@Override
	public synchronized void localComputeCompleted(String workerID, Set<String> activeWorkerIDs)
			throws RemoteException {

		log.info("receive acknowledgement from worker " + workerID + "\n saying activeWorkers: "
				+ activeWorkerIDs.toString());

		if (isFirstPartialResult) {
			isFirstPartialResult = false;
			firstPartialResultArrivalTime = System.currentTimeMillis();
		}

		this.activeWorkerSet.addAll(activeWorkerIDs);
		this.workerAcknowledgementSet.remove(workerID);

		if (this.workerAcknowledgementSet.size() == 0) {

			Stat.getInstance().finishGapTime = (System.currentTimeMillis() - firstPartialResultArrivalTime) * 1.0 / 1000;

			superstep++;
			if (superstep < 3) {
				log.info("superstep =" + superstep + ", manually active all worker.");
				for (Map.Entry<String, FragmentedGWorkerProxy> entry : workerProxyMap.entrySet()) {
					activeWorkerSet.add(entry.getKey());
				}
			}
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

		for (Map.Entry<String, FragmentedGWorkerProxy> entry : workerProxyMap.entrySet()) {
			FragmentedGWorkerProxy workerProxy = entry.getValue();
			workerProxy.processPartialResult();
		}
	}

	public synchronized void receivePartialResults(String workerID,
			Map<Integer, Result> mapPartitionID2Result, double communicationData) {

		Stat.getInstance().communicationData += communicationData;

		for (Entry<Integer, Result> entry : mapPartitionID2Result.entrySet()) {
			resultMap.put(entry.getKey(), entry.getValue());
		}

		this.workerAcknowledgementSet.remove(workerID);

		if (this.workerAcknowledgementSet.size() == 0) {

			Stat.getInstance().localDetectViolationTime = (System.currentTimeMillis() - localStartTime) * 1.0 / 1000;
			log.info("begin to assemble the final result.");

			try {

				ViolationResult finalResult = new ViolationResult();
				finalResult.assemblePartialResults(resultMap.values());
				finalResult.writeToFile(KV.OUTPUT_DIR + "/FINAL" + "-FragG-P"
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

	private void loadBorderVerticesInformation() {

		// load all vertices;
		try {
			File vertexFile = new File(KV.GRAPH_FILE_PATH + ".v");
			LineIterator it = FileUtils.lineIterator(vertexFile, "UTF-8");
			try {
				while (it.hasNext()) {
					String[] elements = it.nextLine().split("\t");
					if (elements != null && elements.length == 2) {
						allVertices.put(Integer.parseInt(elements[0]), elements[1]);
					}
				}
			} finally {
				LineIterator.closeQuietly(it);
			}
			log.debug("all vertices loaded in Sc.");
		} catch (IOException e) {
			log.error("load all vertex file failed.");
			e.printStackTrace();
		}

		// load border vertices;
		try {
			File borderVertexFile = new File(KV.GRAPH_FILE_PATH + ".p2bv");
			LineIterator it = FileUtils.lineIterator(borderVertexFile, "UTF-8");
			try {
				while (it.hasNext()) {
					String[] elements = it.nextLine().split("\t");
					if (elements != null) {
						int partitionID = Integer.parseInt(elements[0]);
						int borderNodeID = Integer.parseInt(elements[1]);
						if (!allBorderVertices.containsKey(partitionID)) {
							allBorderVertices.put(partitionID, new IntOpenHashSet());
						}
						allBorderVertices.get(partitionID).add(borderNodeID);
					}
				}
			} finally {
				LineIterator.closeQuietly(it);
			}
			log.debug("all border vertices loaded in Sc.");
		} catch (IOException e) {
			log.error("load border file failed.");
			e.printStackTrace();
		}

		try {
			File crossingEdgeFile = new File(KV.GRAPH_FILE_PATH + ".cross");
			LineIterator it = FileUtils.lineIterator(crossingEdgeFile, "UTF-8");
			int linecount = 0;
			try {
				while (it.hasNext()) {
					linecount++;
					String[] elements = it.nextLine().split("\t");
					if (elements != null) {
						int sourceID = Integer.parseInt(elements[0]);
						int edgeType = Integer.parseInt(elements[1]);
						int targetID = Integer.parseInt(elements[2]);
						CrossingEdge ce = new CrossingEdge(sourceID, edgeType, targetID);
						if (!mapBorderNodesAsSource.containsKey(ce.source)) {
							mapBorderNodesAsSource.put(ce.source,
									new ObjectOpenHashSet<CrossingEdge>());
						}
						mapBorderNodesAsSource.get(ce.source).add(ce);
						if (!mapBorderNodesAsTarget.containsKey(ce.target)) {
							mapBorderNodesAsTarget.put(ce.target,
									new ObjectOpenHashSet<CrossingEdge>());
						}
						mapBorderNodesAsTarget.get(ce.target).add(ce);
					}
				}
			} finally {
				LineIterator.closeQuietly(it);
			}
			log.info("all crossing edges loaded in Sc, size = " + linecount);
		} catch (IOException e) {
			log.error("load crossing file failed.");
			e.printStackTrace();
		}
	}

	private void sendPartitionInfo() throws RemoteException {
		for (Map.Entry<String, FragmentedGWorkerProxy> entry : workerProxyMap.entrySet()) {
			FragmentedGWorkerProxy workerProxy = entry.getValue();
			workerProxy.setWorkerPartitionInfo(null, partitionWorkerMap, workerMap);
		}
	}

	public void preProcess() throws RemoteException {

		wholeStartTime = System.currentTimeMillis();
		// read border nodes;
		loadBorderVerticesInformation();

		// distributed partition;
		assignDistributedPartitions();
		sendPartitionInfo();

		Stat.getInstance().getInputFilesLocalAndDistributedTime = (System.currentTimeMillis() - wholeStartTime) * 1.0 / 1000;

		List<GFD2> queries = readGFDFromDir();
		log.info("load " + queries.size() + " gfds from: " + KV.QUERY_DIR_PATH);

		this.workerAcknowledgementSet.clear();
		this.workerAcknowledgementSet.addAll(this.activeWorkerSet);

		sendGFDs2Workers(queries);
	}

	private void buildAllBorderVertices() {
		// build for all borderVertices, including:
		// 1. border nodes as target/source with crossing edges.
		// 2. border nodes with self balls
		for (int borderNode : this.borderBallSize.keySet()) {
			// add crossing edge;
			int wholeBallSize = this.borderBallSize.get(borderNode);
			if (this.mapBorderNodesAsSource.containsKey(borderNode)) {
				wholeBallSize += this.mapBorderNodesAsSource.get(borderNode).size();
			}
			if (this.mapBorderNodesAsTarget.containsKey(borderNode)) {
				wholeBallSize += this.mapBorderNodesAsTarget.get(borderNode).size();
			}
			this.allborderBallSize.put(borderNode, wholeBallSize);
		}
	}

	private void estimateCommunicationCost() {
		System.out.println("begin to estimate. total workunits = " + workunits.size());
		double t1 = 0;
		double t2 = 0;
		int i = 0;
		long start = 0;
		for (WorkUnit wu : workunits) {
			i++;
			if (i % 100 == 0) {
				log.debug("i=" + i + ", t1/t2=" + t1 + "/" + t2);
			}
			start = System.currentTimeMillis();
			wu.estimateCC(wu.originParititon, this.allborderBallSize);
			t1 += (System.currentTimeMillis() - start) * 1.0 / 1000;
			start = System.currentTimeMillis();
			wu.buildPrefetchRequest(this.allVertices, this.mapBorderNodesAsSource,
					this.mapBorderNodesAsTarget, this.allBorderVertices);
			t2 += (System.currentTimeMillis() - start) * 1.0 / 1000;
		}
	}

	private void assignWorkunitsToWorkers() throws RemoteException {

		log.debug("begin assign work units to workers.");

		long assignStartTime = System.currentTimeMillis();

		int machineNum = workerProxyMap.size();
		Stat.getInstance().totalWorkUnit = workunits.size();

		Int2ObjectMap<Set<WorkUnit>> assignment = new Int2ObjectOpenHashMap<Set<WorkUnit>>();
		Int2ObjectMap<Int2ObjectMap<IntSet>> prefetchRequest = new Int2ObjectOpenHashMap<Int2ObjectMap<IntSet>>();
		Int2ObjectMap<Set<CrossingEdge>> crossingEdges = new Int2ObjectOpenHashMap<Set<CrossingEdge>>();
		Random r = new Random();

		log.debug("should be very quick");
		for (WorkUnit wu : workunits) {
			int assignedMachine = r.nextInt(machineNum);
			if (!assignment.containsKey(assignedMachine)) {
				assignment.put(assignedMachine, new HashSet<WorkUnit>());
			}
			assignment.get(assignedMachine).add(wu);

			if (!prefetchRequest.containsKey(assignedMachine)) {
				prefetchRequest.put(assignedMachine, new Int2ObjectOpenHashMap<IntSet>());
			}
			prefetchRequest.get(assignedMachine).putAll(wu.prefetchRequest);

			if (!crossingEdges.containsKey(assignedMachine)) {
				crossingEdges.put(assignedMachine, new ObjectOpenHashSet<CrossingEdge>());
			}
			crossingEdges.get(assignedMachine).addAll(wu.transferCrossingEdge);
		}
		log.debug("job assignment finished. begin to dispatch.");

		for (int machineID : assignment.keySet()) {
			String workerID = partitionWorkerMap.get(machineID);
			FragmentedGWorkerProxy workerProxy = workerProxyMap.get(workerID);
			workerProxy.setWorkUnitsAndPrefetchRequest(assignment.get(machineID),
					prefetchRequest.get(machineID), crossingEdges.get(machineID));
			int prefetchSize = 0;
			for (int key : prefetchRequest.get(machineID).keySet()) {
				prefetchSize += prefetchRequest.get(machineID).get(key).size();
			}
			log.debug("now sent RANDOM assigment for machine " + machineID + " on " + workerID
					+ " and prefetch request size = " + prefetchSize + ", crossing size = "
					+ crossingEdges.get(machineID).size());
			Stat.getInstance().crossingEdgeData += RamUsageEstimator.sizeOf(crossingEdges
					.get(machineID));
		}

		localStartTime = System.currentTimeMillis();
		Stat.getInstance().jobAssignmentTime = (localStartTime - assignStartTime) * 1.0 / 1000;
	}

	public synchronized void receivePartialWorkunitsFromWorkers(String workerID,
			Set<WorkUnit> partialWorkunits, Int2IntMap partialBorderBallSize,
			double findCandidateTime) {

		log.debug("receive partitial workunits from " + workerID + ", size = "
				+ partialWorkunits.size() + ", ballsizemap = " + partialBorderBallSize.size()
				+ ", findCandidateTime = " + findCandidateTime);

		if (findCandidateTime > Stat.getInstance().findCandidatesTime) {
			Stat.getInstance().findCandidatesTime = findCandidateTime;
		}

		this.borderBallSize.putAll(partialBorderBallSize);
		this.workunits.addAll(partialWorkunits);

		this.workerAcknowledgementSet.remove(workerID);
		this.activeWorkerSet.add(workerID);

		if (this.workerAcknowledgementSet.size() == 0) {
			log.debug("got all the paritial workunits, begin to assemble.");
			buildAllBorderVertices();
			log.debug("finishbuildborder.");
			estimateCommunicationCost();
			log.debug("finishestimate.");
			try {
				log.debug("begin to process.");
				process();
			} catch (RemoteException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void process() throws RemoteException {
		assignWorkunitsToWorkers();
		nextLocalCompute();
	}

	private List<GFD2> readGFDFromDir() {
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

	public void sendGFDs2Workers(List<GFD2> queries) throws RemoteException {
		for (Map.Entry<String, FragmentedGWorkerProxy> entry : workerProxyMap.entrySet()) {
			FragmentedGWorkerProxy workerProxy = entry.getValue();
			workerProxy.setQueryWithGFD(queries);
		}
	}

	@Override
	public void postProcess() throws RemoteException {
	}

	@Override
	public void sendPartialResult(String workerID, Map<Integer, Result> mapPartitionID2Result,
			double communicationData) throws RemoteException {
	}

	@Override
	public void sendPartialWorkunitsAndBorderInfo(String workerID, Set<WorkUnit> partialWorkunits,
			Int2IntMap bordernodeBallSize, double findCandidateTime) {
	}
}
