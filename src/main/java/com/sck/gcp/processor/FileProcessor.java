package com.sck.gcp.processor;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;

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
		StringBuilder sb = new StringBuilder();
		try (BufferedReader br = Files
				.newBufferedReader(Paths.get(ClassLoader.getSystemResource("product.xml").toURI()))) {
			// read line by line
			String line;
			while ((line = br.readLine()) != null) {
				sb.append(line).append("\n");
			}

		} catch (IOException e) {
			LOGGER.error("IOException:", e);
		} catch (URISyntaxException e) {
			LOGGER.error("URISyntaxException:", e);
		}
		return sb.toString();
	}
	
	public String readFile(MultipartFile uploadfile) {
		StringBuilder sb = new StringBuilder();
		try {
			// read line by line
			byte[] bytes = uploadfile.getBytes();
            BufferedOutputStream stream = new BufferedOutputStream(new FileOutputStream(new File(uploadfile.getName())));
            stream.write(bytes);
            stream.close();
            sb.append(stream);
		} catch (IOException e) {
			LOGGER.error("IOException:", e);
		} 
		
		return sb.toString();
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
