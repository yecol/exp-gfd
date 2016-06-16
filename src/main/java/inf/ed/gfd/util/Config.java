package inf.ed.gfd.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.Properties;

public class Config {

	/** The static configuration instance */
	private static Config config;
	/** Represents the properties */
	private static Properties properties;
	/** File object containing properties */
	private static File propertiesFile;
	/** file last modified */
	private static long lastModified;

	/**
	 * Constructs the props instance
	 * 
	 * @param propertiesFilePath
	 *            Represents the file path of the properties file
	 */
	private Config(String propertiesFilePath) {
		properties = new Properties();
		propertiesFile = new File(propertiesFilePath);
		loadProperties();
	}

	public String determineDataset() {
		if (propertiesFile.getName().contains("pokec")) {
			return KV.DATASET_POKEC;
		}
		if (propertiesFile.getName().contains("yago")) {
			return KV.DATASET_YAGO;
		}
		if (propertiesFile.getName().contains("dbpedia")) {
			return KV.DATASET_DBPEDIA;
		} else {
			return "OTHER";
		}
	}

	/**
	 * Gets the instance of the props
	 * 
	 * @return returns the instance of the props
	 */
	public static synchronized Config getInstance() {
		if (config == null) {
			try {
				File self = null;
				self = new File(Config.class.getProtectionDomain().getCodeSource().getLocation()
						.toURI().getPath());
				config = new Config(self.getParent().toString() + "/" + Params.CONFIG_FILENAME);
			} catch (URISyntaxException e) {
				e.printStackTrace();
			}
		}
		return config;
	}

	/**
	 * Checks if the properties file has been modified after it was loaded.
	 */
	private static void checkProperties() {
		if (propertiesFile.lastModified() > lastModified) {
			loadProperties();
		}
	}

	/**
	 * Loads the properties file
	 */
	private static void loadProperties() {
		lastModified = System.currentTimeMillis();
		InputStream in = null;
		try {
			in = new FileInputStream(propertiesFile);
			properties.load(in);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				in.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Gets the string property for the given key
	 * 
	 * @param key
	 *            represents the property member
	 * @return Returns the string property for the given key
	 * @throws PropertyNotFoundException
	 */
	public String getStringProperty(String key) throws PropertyNotFoundException {
		checkProperties();
		String value = properties.getProperty(key);
		if (value == null)
			throw new PropertyNotFoundException(key);
		return value;
	}

	/**
	 * Gets the integer property for the given key
	 * 
	 * @param key
	 *            represents the property member
	 * @return Returns the integer property for the given key
	 * @throws PropertyNotFoundException
	 */
	public Integer getIntProperty(String key) throws PropertyNotFoundException {
		checkProperties();
		String value = properties.getProperty(key);
		if (value == null)
			throw new PropertyNotFoundException(key);
		return Integer.parseInt(value);
	}

	/**
	 * Gets the double property for the given key
	 * 
	 * @param key
	 *            represents the property member
	 * @return Returns the double property for the given key
	 * @throws PropertyNotFoundException
	 */
	public Double getDoubleProperty(String key) throws PropertyNotFoundException {
		checkProperties();
		String value = properties.getProperty(key);
		if (value == null)
			throw new PropertyNotFoundException(key);
		return Double.parseDouble(value);
	}

	/**
	 * Gets the double property for the given key
	 * 
	 * @param key
	 *            represents the property member
	 * @return Returns the double property for the given key
	 * @throws PropertyNotFoundException
	 */
	public Boolean getBooleanProperty(String key) throws PropertyNotFoundException {
		checkProperties();
		String value = properties.getProperty(key);
		if (value == null)
			throw new PropertyNotFoundException(key);
		return Boolean.parseBoolean(value);
	}

	/**
	 * Gets the long property for the given key
	 * 
	 * @param key
	 *            represents the property member
	 * @return Returns the long property for the given key
	 * @throws PropertyNotFoundException
	 */
	public Long getLongProperty(String key) throws PropertyNotFoundException {
		checkProperties();
		String value = properties.getProperty(key);
		if (value == null)
			throw new PropertyNotFoundException(key);
		return Long.parseLong(value);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Config config = Config.getInstance();
		try {
			System.out.println(config.getStringProperty("LOG_DIR"));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	class PropertyNotFoundException extends Exception {
		/** The Constant serialVersionUID. */
		private static final long serialVersionUID = 1L;

		/**
		 * Constructs the property not found exception
		 * 
		 * @param propertyKey
		 *            Represents the invalid property key
		 */
		public PropertyNotFoundException(String propertyKey) {
			super(propertyKey + " is not found in props file");
		}
	}

}
