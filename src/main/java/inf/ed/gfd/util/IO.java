package inf.ed.gfd.util;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class IO {
	//
	static Logger log = LogManager.getLogger(IO.class);

	static public Map<Integer, Integer> loadInt2IntMapFromFile(String filename) throws IOException {

		HashMap<Integer, Integer> retMap = new HashMap<Integer, Integer>();

		log.info("loading map from " + filename + " with stream scanner.");

		long startTime = System.currentTimeMillis();

		FileInputStream fileInputStream = null;
		Scanner sc = null;

		fileInputStream = new FileInputStream(filename);
		sc = new Scanner(fileInputStream, "UTF-8");
		while (sc.hasNextInt()) {
			int key = sc.nextInt();
			int value = sc.nextInt();
			retMap.put(key, value);
		}

		if (fileInputStream != null) {
			fileInputStream.close();
		}
		if (sc != null) {
			sc.close();
		}

		log.info(filename + " loaded to map. with size =  " + retMap.size() + ", using "
				+ (System.currentTimeMillis() - startTime) + " ms");

		return retMap;
	}

	static public IntSet loadIntSetFromFile(String filename) {

		IntSet retSet = new IntOpenHashSet();

		log.info("loading set from " + filename + " with stream scanner.");

		long startTime = System.currentTimeMillis();

		FileInputStream fileInputStream = null;
		Scanner sc = null;

		try {
			fileInputStream = new FileInputStream(filename);
			sc = new Scanner(fileInputStream, "UTF-8");
			while (sc.hasNextInt()) {
				retSet.add(sc.nextInt());
			}
			if (fileInputStream != null) {
				fileInputStream.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		if (sc != null) {
			sc.close();
		}

		log.info(filename + " loaded to set. with size =  " + retSet.size() + ", using "
				+ (System.currentTimeMillis() - startTime) + " ms");

		return retSet;
	}

	static public <K, V> void writeMapToFile(Map<K, V> map, String filename) {

		log.info("writing map to " + filename + "");

		long startTime = System.currentTimeMillis();

		PrintWriter writer;
		try {
			writer = new PrintWriter(filename, "UTF-8");

			for (Entry<K, V> entry : map.entrySet()) {
				writer.println(entry.getKey() + "\t" + entry.getValue());
			}

			writer.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}

		log.info(filename + " write to file. map size =  " + map.size() + ", using "
				+ (System.currentTimeMillis() - startTime) + " ms");
	}
}
