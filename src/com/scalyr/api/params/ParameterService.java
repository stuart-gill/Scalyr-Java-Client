/* Scalyr client library
 * Copyright (c) 2011 Scalyr
 * All rights reserved
 */

package com.scalyr.api.params;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import com.scalyr.api.ScalyrException;
import com.scalyr.api.ScalyrNetworkException;
import com.scalyr.api.json.JSONObject;
import com.scalyr.api.json.JSONParser;

/**
 * Encapsulates the raw HTTP-level API to the parameter service.
 */
public class ParameterService {
  /**
   * Server URL, including a trailing slash.
   */
  private final String serverAddress;
  
  /**
   * API token we pass in all requests to the server.
   */
  private final String apiToken;
  
  /**
   * Construct a ParameterService object to communicate with the server at the specified address.
   * 
   * @param serverAddress Server URL, e.g. http://api.scalyr.com. Trailing slash is optional.
   * @param apiToken The API authorization token to use when communicating with the server. (If you need
   *     to use multiple api tokens, construct a separate ParameterService instance for each.)
   */
  public ParameterService(String serverAddress, String apiToken) {
    if (!serverAddress.endsWith("/"))
      serverAddress += "/";
    
    this.serverAddress = serverAddress;
    this.apiToken = apiToken;
  }
  
  /**
   * Return a ParameterFileFactory that retrieves files via this ParameterService instance.
   * 
   * @param cacheDir If not null, then we store a copy of each retrieved file in this directory.
   *     On subsequent calls to createFactory, we use the stored files to initialize our state until
   *     each file can be re-fetched from the server. This prevents parameter file reads from
   *     stalling.
   */
  public ParameterFileFactory createFactory(final File cacheDir) {
    return new ParameterFileFactory(){
      @Override protected ParameterFile createFileReference(String filePath) {
        return new HostedParameterFile(ParameterService.this, filePath, cacheDir);
      }};
  }
  
  /**
   * Retrieve a parameter file.
   * 
   * @param path The file path, e.g. "/params.txt". Must begin with a slash.
   * @param expectedVersion Should normally be null. If not null, equal to the file's current version
   *     number, then the content field will be omitted from the response. Saves bandwidth when
   *     you are merely checking to see whether a file has changed since a previous known version.
   * @param waitTime If not null, and expectedVersion is not null, and the file matches expectedVersion,
   *     then this request will not complete until the file is modified or waitTime seconds have
   *     elapsed. (Note: this is not honored absolutely -- sometimes, the server may return a result
   *     prior to waitTime even if the file has not changed.)
   * 
   * @return The response from the server. See {@link scalyr.com/paramJsonApi}.
   * 
   * @throws ScalyrException
   * @throws ScalyrNetworkException
   */
  public String getFile(String path, Long expectedVersion, Integer waitTime)
      throws ScalyrException, ScalyrNetworkException {
    JSONObject parameters = new JSONObject();
    parameters.put("token", apiToken);
    parameters.put("path", path);
    if (expectedVersion != null)
      parameters.put("expectedVersion", expectedVersion);
    
    if (waitTime != null)
      parameters.put("waitTime", waitTime);
    
    return invokeApi("getFile", parameters);
  }
  
  /**
   * Create, update, or delete a parameter file.
   * 
   * @param path The file path, e.g. "/params.txt". Must begin with a slash.
   * @param expectedVersion Should normally be null. If not null, and not equal to the file's current version,
   *     then the file will not be modified. Enables atomic read/modify/write operations.
   * @param content New content for the file. Pass null if passing deleteFile = true.
   * @param deleteFile True to delete the file, false to create/update it.
   * 
   * @return The response from the server. See {@link scalyr.com/paramJsonApi}.
   * 
   * @throws ScalyrException
   * @throws ScalyrNetworkException
   * 
   * {
   *   status:  "success",
   * }
   * 
   * {
   *   status: "versionMismatch" // if expectedVersion was not null and did not match the file's current version
   * }
   * 
   * {
   *   status:  "error", // if the request is somehow incorrect
   *   message: "[a human-readable message]"
   * }
   * 
   * {
   *   status:  "serverError", // if the server experienced an internal error while processing the request
   *   message: "[a human-readable message]"
   * }
   */
  public String putFile(String path, Long expectedVersion, String content, boolean deleteFile)
      throws ScalyrException, ScalyrNetworkException {
    JSONObject parameters = new JSONObject();
    parameters.put("token", apiToken);
    parameters.put("path", path);
    if (expectedVersion != null)
      parameters.put("expectedVersion", expectedVersion);
    
    if (deleteFile)
      parameters.put("deleteFile", true);
    else
      parameters.put("content", content);
    
    return invokeApi("putFile", parameters);
  }
  
  /**
   * Retrieve a list of all parameter files.
   * 
   * @return The response from the server. See {@link scalyr.com/paramJsonApi}.
   * 
   * @throws ScalyrException
   * @throws ScalyrNetworkException
   * 
   * Note that paths are complete (absolute) pathnames, beginning with a slash, and are sorted
   * lexicographically.
   * 
   * {
   *   status:     "success",
   *   paths:      ["...", "...", "..."]
   * }
   * 
   * {
   *   status:  "error", // if the request is somehow incorrect
   *   message: "[a human-readable message]"
   * }
   * 
   * {
   *   status:  "serverError", // if the server experienced an internal error while processing the request
   *   message: "[a human-readable message]"
   * }
   */
  public String listFiles() 
      throws ScalyrException, ScalyrNetworkException {
    JSONObject parameters = new JSONObject();
    parameters.put("token", apiToken);
    
    return invokeApi("listFiles", parameters);
  }

  /**
   * Invoke serverAddress/methodName on the server, sending the specified parameters as the request
   * body. Parse the response as JSON.
   */
  protected String invokeApi(String methodName, JSONObject parameters) {
    HttpURLConnection connection = null;  
    try {
      // Send the request.
      URL url = new URL(serverAddress + methodName);
      connection = (HttpURLConnection) url.openConnection();
      connection.setRequestMethod("POST");
      connection.setUseCaches(false);
      connection.setDoInput(true);
      // connection.setConnectTimeout(timeoutMs);
      // connection.setReadTimeout(timeoutMs);
      
      byte[] requestBody = parameters.toJSONString().getBytes();
      connection.setRequestProperty("Content-Type", "application/json");
      connection.setRequestProperty("Content-Length", "" + requestBody.length);
      connection.setDoOutput(true);
      
      DataOutputStream output = new DataOutputStream(connection.getOutputStream());
      output.write(requestBody);
      output.flush();
      output.close();
      
      // Retrieve the response.
      int responseCode = connection.getResponseCode();
      
      InputStream input = connection.getInputStream();
      BufferedReader reader = new BufferedReader(new InputStreamReader(input, "UTF-8"));
      String responseText = readEntireStream(reader);
      reader.close();
      
      if (responseCode != 200) {
        // TODO: log StringUtil.noisyTruncate(response.responseBody.trim(), 1000));
        // also do this in the "Malformed response" case
        throw new ScalyrNetworkException("Scalyr server returned error code " + responseCode);
      }
      
      Object responseJson = new JSONParser().parse(responseText);
      if (responseJson instanceof JSONObject)
        return responseText;
      else
        throw new ScalyrException("Malformed response from Scalyr server");
    } catch (Exception ex) {
      if (ex instanceof SocketTimeoutException)
        throw new ScalyrNetworkException("Timeout while communicating with Scalyr server", ex);
      else
        throw new ScalyrNetworkException("Error while communicating with Scalyr server", ex);
    } finally {
      if (connection != null)
        connection.disconnect(); 
    }
  }
  
  private String readEntireStream(Reader reader) throws IOException {
    StringBuilder sb = new StringBuilder();
    char[] buffer = new char[4096];
    while (true) {
      int count = reader.read(buffer, 0, buffer.length);
      if (count <= 0)
        break;
      sb.append(buffer, 0, count);
    }
    return sb.toString();
  }
  
  /**
   * Executor used to invoke API operations asynchronously.
   * 
   * TODO: see http://stackoverflow.com/questions/1014528/asynchronous-http-client-for-java for better ways
   * to perform asynchronous requests. Might also think about adding batch support to the API, so that we don't
   * need multiple outstanding requests to the server. If we don't eliminate this executor, then do tweak it to
   * properly name its threads.
   */
  static final Executor asyncApiExecutor = Executors.newCachedThreadPool();
}
