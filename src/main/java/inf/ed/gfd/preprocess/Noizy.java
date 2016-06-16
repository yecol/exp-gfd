package inf.ed.gfd.preprocess;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Noizy {

	public static void main(String[] args) {
		String datafile, nodemap, predictmap, querydir, outfile;
		if (args.length < 6) {
			System.out.println("para:datafile, nodefile, edgetypemap, querydir, rate, outfilename");
			System.exit(0);
		}
		datafile = args[0];
		nodemap = args[1];
		predictmap = args[2];
		querydir = args[3];
		Double rate = Double.parseDouble(args[4]);
		outfile = args[5];
		// 输入所需要的各个参数

		System.out.println("load objects.");
		BufferedReader br1 = read(nodemap);
		Map<String, String> mapVertexID2Value = new HashMap<String, String>();
		mapVertexID2Value = transform1(br1);

		int totalNodes = mapVertexID2Value.size();
		// 读入subject,object的映射文件，并处理成map

		System.out.println("load predict.");
		BufferedReader br2 = read(predictmap);
		Map<String, String> mapEdgeTypeID2Value = new HashMap<String, String>();
		mapEdgeTypeID2Value = transform2(br2);
		// 读入predict的映射文件，并处理成map2

		System.out.println("loaded files.");

		File queryDir = new File(querydir);

		String reg1 = "^e";
		String reg2 = "^%Cand";
		String reg3 = "^[0-9]";
		Pattern p1 = Pattern.compile(reg1);
		Pattern p2 = Pattern.compile(reg2);
		Pattern p3 = Pattern.compile(reg3);
		// 用正则表达式表示出信息的匹配

		Map<String, ArrayList<String>> mapU2Candidates = new HashMap<String, ArrayList<String>>();
		// 准备存储以%Cand后的数为键值，以其后出现的多个值组成的集合为值的maps

		String num = null;// %Cand后的值
		String line = "";

		for (File f : queryDir.listFiles()) {
			if (f.isFile()) {
				try {
					BufferedReader br3 = read(f.getCanonicalPath());
					// 读入查找文件
					line = br3.readLine();
					while (line != null) {
						Matcher m2 = p2.matcher(line);
						if (!m2.find()) {
							line = br3.readLine();
						} else {
							num = null;
							String[] tmp = line.split("\\s+");
							num = tmp[1];

							ArrayList<String> list = new ArrayList<String>();

							while ((line = br3.readLine()) != null) {// 当没有到文件末尾，也没遇见下一个Cand时就继续读，将值添加到list
								Matcher m3 = p3.matcher(line);
								if (m3.find()) {
									list.add(line);
								}
								m2 = p2.matcher(line);
								if (m2.find()) {
									break;
								}
							}

							mapU2Candidates.put(num, list);

						}

					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				// 至此，经过遍历整个查询文件，maps处理完毕，接下来进行下一次对查询文件的读取

				try {

					BufferedReader again = read(f.getCanonicalPath());
					String data = "";
					while ((data = again.readLine()) != null) {

						Matcher m1 = p1.matcher(data);
						if (m1.find()) {
							String subject = null;
							String object = null;
							String predict = null;
							String[] tmp1 = data.split("\\s+");
							subject = tmp1[1];
							predict = tmp1[2];
							object = tmp1[3];
							String predictOut = mapEdgeTypeID2Value.get(predict);

							BufferedWriter bw = write(outfile);
							for (String uID : mapU2Candidates.keySet()) {
								ArrayList<String> candidates = mapU2Candidates.get(uID);
								int sum = candidates.size();
								if (uID.equals(subject)) {
									for (int i = 0; i < sum; i++) {
										if (Math.random() <= rate) {
											// String subOut = mapVertexID2Value
											// .get(candidates.get(i));

											Random r = new Random();
											int randKey = r.nextInt(totalNodes);
											while (!mapVertexID2Value.keySet().contains(
													String.valueOf(randKey))) {
												randKey = r.nextInt(totalNodes);
											}
											// String objOut =
											// mapVertexID2Value.get(String
											// .valueOf(randKey));
											String outstring = candidates.get(i) + "\t" + predict
													+ "\t" + String.valueOf(randKey);
											// System.out.println(end);
											bw.write(outstring);
											bw.newLine();
										}
									}

								} else if (uID.equals(object)) {
									for (int i = 0; i < sum; i++) {
										if (Math.random() <= rate) {
//											String objOut = mapVertexID2Value
//													.get(candidates.get(i));

											Random r = new Random();
											int randKey = r.nextInt(totalNodes);
											while (!mapVertexID2Value.keySet().contains(
													String.valueOf(randKey))) {
												randKey = r.nextInt(totalNodes);
											}
											// String subOut =
											// mapVertexID2Value.get(String
											// .valueOf(randKey));
											String outstring = String.valueOf(randKey) + "\t"
													+ predict + "\t" + candidates.get(i);
											// System.out.println(end);
											bw.write(outstring);
											bw.newLine();
										}
									}

								}
							}
							bw.close();
						} else {
						}
					}
				} catch (IOException e) {
					e.printStackTrace();
				}

			}
		}

	}

	static BufferedReader read(String fileName) {
		FileInputStream inputStream = null;
		try {
			inputStream = new FileInputStream(fileName);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		InputStreamReader iReader = null;
		try {
			iReader = new InputStreamReader(inputStream, "utf-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		BufferedReader br = new BufferedReader(iReader);
		return br;
	}

	static Map<String, String> transform1(BufferedReader br) {
		Map<String, String> map = new HashMap<String, String>();
		String data = "";
		String key = "", value = "";
		try {
			int i = 0;
			while ((data = br.readLine()) != null) {
				i++;
				String[] temp = data.split("\t");
				if (temp.length != 2) {
					System.out.println("error");
				}
				key = temp[0];
				value = temp[1];
				map.put(key, value);
				if (i < 50) {
					System.out.println(key + ":" + value);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return map;
	}

	static Map<String, String> transform2(BufferedReader br) {
		Map<String, String> map = new HashMap<String, String>();
		String data = "";
		String key = "", value = "";
		try {
			int i = 0;
			while ((data = br.readLine()) != null) {
				i++;

				String[] temp = data.split("\t");
				if (temp.length != 2) {
					System.out.println("error");
				}
				key = temp[1];
				value = temp[0];
				map.put(key, value);
				if (i < 50) {
					System.out.println(key + ":" + value);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return map;
	}

	static BufferedWriter write(String filename) {
		FileOutputStream fileOutputStream = null;
		try {
			fileOutputStream = new FileOutputStream(filename, true);
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		}
		OutputStreamWriter oWriter = new OutputStreamWriter(fileOutputStream);
		BufferedWriter bw = new BufferedWriter(oWriter);
		return bw;

	}

}
