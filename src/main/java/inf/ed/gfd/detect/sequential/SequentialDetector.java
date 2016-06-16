package inf.ed.gfd.detect.sequential;

import inf.ed.gfd.structure.GFD2;
import inf.ed.graph.structure.Graph;
import inf.ed.graph.structure.OrthogonalEdge;
import inf.ed.graph.structure.OrthogonalGraph;
import inf.ed.graph.structure.adaptor.TypedEdge;
import inf.ed.graph.structure.adaptor.VertexOString;
import inf.ed.graph.structure.adaptor.VertexString;
import inf.ed.isomorphism.VF2IsomorphismEnumInspector;
import it.unimi.dsi.fastutil.ints.Int2IntMap;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.carrotsearch.sizeof.RamUsageEstimator;

public class SequentialDetector {

	static Logger log = LogManager.getLogger(SequentialDetector.class);

	static String PARAM_KB_FILE;
	static String PARAM_QUERY_DIR;
	static int PARAM_CANDIDATES_LIMIT = Integer.MAX_VALUE;
	static int MAX_THREAD_ISOMORPHISM = Integer.MAX_VALUE;

	public static void main(String[] args) {

		if (args.length < 4) {
			System.out.println("param error: KB_FILE, QUERY_FILE_DIR, CAND_LMT, ISO_THREAD_LIMIT");
			System.exit(0);
		}

		long start = System.currentTimeMillis();

		PARAM_KB_FILE = args[0];
		PARAM_QUERY_DIR = args[1];
		PARAM_CANDIDATES_LIMIT = Integer.parseInt(args[2]);
		MAX_THREAD_ISOMORPHISM = Integer.parseInt(args[3]);

		Graph<VertexOString, OrthogonalEdge> KB = new OrthogonalGraph<VertexOString>(
				VertexOString.class);
		KB.loadGraphFromVEFile(PARAM_KB_FILE, true);
		System.out.print("Graph size in memory:" + RamUsageEstimator.sizeOf(KB));

		File dir = new File(PARAM_QUERY_DIR);
		List<File> files = (List<File>) FileUtils.listFiles(dir, TrueFileFilter.INSTANCE, null);
		for (File file : files) {
			try {
				GFD2 gfd = new GFD2();
				gfd.setID(file.getName());
				gfd.readFromFile(file.getCanonicalPath());

				if (gfd.isConnected()) {
					processConnectedGFD(gfd, KB);
				} else {
					processDisconnectedGFD(gfd, KB);
				}

			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		double elipseTime = (System.currentTimeMillis() - start) * 1.0 / 1000;
		log.info("STAT: -total elipse time = " + elipseTime);

	}

	static public void processConnectedGFD(GFD2 gfd, Graph<VertexOString, OrthogonalEdge> KB) {

		assert gfd.isConnected() == true;
		long start = System.currentTimeMillis();

		log.info("STAT- begin process " + gfd.getID());
		VF2IsomorphismEnumInspector<VertexString, TypedEdge, VertexOString, OrthogonalEdge> inspector = new VF2IsomorphismEnumInspector<VertexString, TypedEdge, VertexOString, OrthogonalEdge>(
				gfd.getPattern(), 0, KB, gfd.getCandidates().get(0));
		inspector.setThreadNumLimit(MAX_THREAD_ISOMORPHISM);

		List<Int2IntMap> results = inspector.findIsomorphic();

		double t0 = (System.currentTimeMillis() - start) * 1.0 / 1000;

		log.info("STAT- isomorphism using " + t0 + "s, for " + gfd.getCandidates().get(0).size()
				+ " candidates. avg = " + t0 / gfd.getCandidates().get(0).size());

		PrintWriter writer;
		try {
			writer = new PrintWriter("CSResult-" + gfd.getID(), "UTF-8");

			for (Int2IntMap r : results) {
				String line = "";
				for (int i = 0; i < r.size(); i++) {
					line += r.get(i) + "\t";
				}
				writer.println(line);
			}
			writer.flush();
			writer.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}

		log.info(
				"CSResult-" + gfd.getID() + " has write to file. result size =  " + results.size());
	}

	static public void processDisconnectedGFD(GFD2 gfd, Graph<VertexOString, OrthogonalEdge> KB) {

		assert gfd.isConnected() == false;
		long start = System.currentTimeMillis();

		log.info("STAT- begin process " + gfd.getID());
		VF2IsomorphismEnumInspector<VertexString, TypedEdge, VertexOString, OrthogonalEdge> inspector1 = new VF2IsomorphismEnumInspector<VertexString, TypedEdge, VertexOString, OrthogonalEdge>(
				gfd.getPatterns().get(0), 0, KB, gfd.getCandidates().get(0));
		inspector1.setThreadNumLimit(MAX_THREAD_ISOMORPHISM);

		List<Int2IntMap> results1 = inspector1.findIsomorphic();

		double t1 = (System.currentTimeMillis() - start) * 1.0 / 1000;

		log.info("STAT- isomorphism for component 1 using " + t1 + "s, for "
				+ gfd.getCandidates().get(0).size() + " candidates. avg = "
				+ t1 / gfd.getCandidates().get(0).size());
		log.info("STAT- enumerate results1 " + results1.size() + " matches.");

		start = System.currentTimeMillis();

		VF2IsomorphismEnumInspector<VertexString, TypedEdge, VertexOString, OrthogonalEdge> inspector2 = new VF2IsomorphismEnumInspector<VertexString, TypedEdge, VertexOString, OrthogonalEdge>(
				gfd.getPatterns().get(1), 10, KB, gfd.getCandidates().get(10));
		inspector2.setThreadNumLimit(MAX_THREAD_ISOMORPHISM);

		List<Int2IntMap> results2 = inspector2.findIsomorphic();

		double t2 = (System.currentTimeMillis() - start) * 1.0 / 1000;

		log.info("STAT- isomorphism for component 2 using " + t2 + "s, for "
				+ gfd.getCandidates().get(0).size() + " candidates. avg = "
				+ t2 / gfd.getCandidates().get(0).size());
		log.info("STAT- enumerate results2 " + results2.size() + " matches.");

		start = System.currentTimeMillis();
		int vio = gfd.verify2MatchList(results1, results2, KB);

		log.info("STAT- find " + vio + " violations using "
				+ (System.currentTimeMillis() - start) * 1.0 / 1000 + "s.");
	}
}
