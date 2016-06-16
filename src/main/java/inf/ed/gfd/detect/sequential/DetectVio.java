package inf.ed.gfd.detect.sequential;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

public class DetectVio {

	/* testing the lastname */

	static Logger log = LogManager.getLogger(DetectVio.class);

	static String KB_VFILE;
	static String RESUTL_FILE;

	public static void main(String[] args) {

		if (args.length < 2) {
			System.out.println("param error: KB_VFILE, RESULT_FILE");
			System.exit(0);
		}

		KB_VFILE = args[0];
		RESUTL_FILE = args[1];

		Int2ObjectMap<String> vertices = new Int2ObjectOpenHashMap<String>();

		File vertexFile = new File(KB_VFILE);
		try {
			LineIterator it = FileUtils.lineIterator(vertexFile, "UTF-8");
			try {
				while (it.hasNext()) {
					String eles[] = it.next().split("\t");
					if (eles.length == 2) {
						vertices.put(Integer.parseInt(eles[0]), eles[1]);
					}
				}
			} finally {
				LineIterator.closeQuietly(it);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		

		log.debug("graph vertex loaded. with size = " + vertices.size());

		PrintWriter writer;
		File resultFile = new File(RESUTL_FILE);
		int violationSize = 0;

		HashSet<String> dup = new HashSet<String>();

		try {

			writer = new PrintWriter("VIO-" + RESUTL_FILE, "UTF-8");
			LineIterator it = FileUtils.lineIterator(resultFile, "UTF-8");
			try {
				while (it.hasNext()) {

					boolean flag = false;

					String splits[] = it.next().split("\t");
					int n0 = Integer.parseInt(splits[0]);
					int n1 = Integer.parseInt(splits[1]);
					int n2 = Integer.parseInt(splits[2]);
					String node0 = vertices.get(n0).toLowerCase();
					String node1 = vertices.get(n1).toLowerCase();
					String node2 = vertices.get(n2).toLowerCase();

					String s1 = node0 + node1 + node2;
					String s2 = node1 + node0 + node2;
					if (dup.contains(s1) || dup.contains(s2)) {
						continue;
					}

					dup.add(s1);
					dup.add(s2);

					String pLNs[] = node2.split("<|>|_|-");

					for (int i = 0; i < pLNs.length; i++) {
						if (pLNs[i].length() != 0 && !pLNs[i].startsWith("(")) {
							flag = (node0.contains(pLNs[i]) || node1.contains(pLNs[i]));
							if (flag == true) {
								break;
							}
						}
					}

					if (flag) {
						// writer.println(node0 + "\t+\t" + node1 + "\t=>\t" +
						// node2);
					} else {
						violationSize += 1;
						writer.println(
								node0 + "\t+\t" + node1 + "\t=>\t" + node2 + "\t!!!violation!!!");
					}

				}

				writer.flush();
				writer.close();

			} finally {
				LineIterator.closeQuietly(it);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		log.debug("violation size = " + violationSize);

	}

}
