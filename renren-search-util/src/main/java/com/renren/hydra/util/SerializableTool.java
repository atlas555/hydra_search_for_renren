package com.renren.hydra.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.log4j.Logger;

public class SerializableTool {
//Object => byte[], Object必须继承Serializable
	private static Logger logger = Logger.getLogger(SerializableTool.class);
	
	public static byte[] objectToBytes(Object obj) {
		try {
			ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
			ObjectOutputStream os = new ObjectOutputStream(byteStream);
			os.writeObject(obj);

			return byteStream.toByteArray();
		} catch (Exception e) {
			logger.error(e);
			return null;
		}
	}

	// byte[] => Object, Object必须继承Serializable
	public static Object bytesToObject(byte[] data) {
		try {
			ByteArrayInputStream byteStream = new ByteArrayInputStream(data);
			ObjectInputStream os = new ObjectInputStream(byteStream);
			return os.readObject();
		} catch (Exception e) {
			logger.error(e);
			return null;
		}
	}
}
