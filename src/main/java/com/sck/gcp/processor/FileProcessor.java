package com.sck.gcp.processor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.XML;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.multipart.MultipartFile;

@Component
public class FileProcessor {

	private static final Logger LOGGER = LoggerFactory.getLogger(FileProcessor.class);

	public String readFile() {
		String fileName = "xml/product.xml";
		FileProcessor app = new FileProcessor();
		InputStream is = app.getFileFromResourceAsStream(fileName);
		return inputStreamToString(is);
	}

	private InputStream getFileFromResourceAsStream(String fileName) {
		// The class loader that loaded the class
		ClassLoader classLoader = getClass().getClassLoader();
		InputStream inputStream = classLoader.getResourceAsStream(fileName);
		// the stream holding the file content
		if (inputStream == null) {
			throw new IllegalArgumentException("file not found! " + fileName);
		} else {
			return inputStream;
		}

	}

	private String inputStreamToString(InputStream is) {
		StringBuilder sb = new StringBuilder();
		try (InputStreamReader streamReader = new InputStreamReader(is, StandardCharsets.UTF_8);
				BufferedReader reader = new BufferedReader(streamReader)) {
			String line;
			while ((line = reader.readLine()) != null) {
				sb.append(line).append("\n");
			}
		} catch (IOException e) {
			LOGGER.error("IOException:", e);
		}
		return sb.toString();
	}

	public String readFile(MultipartFile uploadfile) {
		InputStream inputStream = getFileFromRequestAsStream(uploadfile);

		return inputStreamToString(inputStream);

	}

	private InputStream getFileFromRequestAsStream(MultipartFile uploadfile) {
		InputStream inputStream = null;
		try {
			inputStream = uploadfile.getInputStream();

		} catch (IOException e) {
			LOGGER.error("IOException:", e);
		}
		if (inputStream == null) {
			throw new IllegalArgumentException("file not found! " + uploadfile.getName());
		} else {
			return inputStream;
		}
	}

	public String convertToJSONL(String xmlString) {
		StringBuilder builder = new StringBuilder();
		String jsonPrettyPrintString = null;

		try {
			JSONObject xmlJSONObj = XML.toJSONObject(xmlString);
			/*
			 * JSONObject exportTime = xmlJSONObj.getJSONObject("ExportTime"); JSONObject
			 * exportContext = xmlJSONObj.getJSONObject("ExportContext"); JSONObject
			 * contextID = xmlJSONObj.getJSONObject("ContextID"); JSONObject workspaceID =
			 * xmlJSONObj.getJSONObject("WorkspaceID"); JSONObject useContextLocale =
			 * xmlJSONObj.getJSONObject("UseContextLocale");
			 */
			JSONArray jsonProducts = xmlJSONObj.getJSONObject("Products").getJSONArray("Product");

			jsonProducts.forEach(i -> builder.append(i.toString()).append("\n"));
			jsonPrettyPrintString = builder.toString();
			// jsonProducts.toString();//xmlJSONObj.toString(PRETTY_PRINT_INDENT_FACTOR);
			LOGGER.info(jsonPrettyPrintString);
		} catch (JSONException je) {
			LOGGER.error("JSONException occurred on converting to json: ", je);
		}
		return jsonPrettyPrintString;
	}
}
