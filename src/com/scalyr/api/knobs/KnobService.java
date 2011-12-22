/*
 * Scalyr client library
 * Copyright 2011 Scalyr, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.scalyr.api.knobs;

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
import java.nio.charset.Charset;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import com.scalyr.api.ScalyrException;
import com.scalyr.api.ScalyrNetworkException;
import com.scalyr.api.TuningConstants;
import com.scalyr.api.internal.Logging;
import com.scalyr.api.json.JSONObject;
import com.scalyr.api.json.JSONParser;

/**
 * Encapsulates the raw HTTP-level API to the Knobs service.
 */
public class KnobService {
  private static final Charset utf8 = Charset.forName("UTF-8");
  
  /**
   * Server URLs, each including a trailing slash. Synchronize access.
   */
  private String[] serverAddresses;
  
  /**
   * Random number generator used to randomize server choices and retry
   * intervals. Synchronize access.
   */
  private Random random = new Random();
  
  /**
   * API token we pass in all requests to the server.
   */
  private final String apiToken;
  
  /**
   * Construct a KnobService.
   * 
   * @param apiToken The API authorization token to use when communicating with the server. (If you need
   *     to use multiple api tokens, construct a separate KnobService instance for each.)
   */
  public KnobService(String apiToken) {
    this.apiToken = apiToken;
    
    setServerAddress(
        "https://api1.scalyr.com," +
    		"https://api2.scalyr.com," +
    		"https://api3.scalyr.com," +
    		"https://api4.scalyr.com");
  }
  
  /**
   * Specify the URL of the Scalyr server, e.g. https://api.scalyr.com. Trailing slash is optional.
   * <p>
   * You may specify multiple addresses, separated by commas. If multiple addresses are specified,
   * the KnobService will choose one as it sees fit, and retry failed requests on a different
   * server.
   * <p>
   * You should only call this method if using a staging server or other non-production instance
   * of the Scalyr service. Otherwise, we automatically default to the production Scalyr service.
   * It is best to call this method at most once, before issuing any requests through this
   * KnobService object.
   * 
   * @return this KnobService object.
   */
  public synchronized KnobService setServerAddress(String value) {
    serverAddresses = value.split(",");
    for (int i = 0; i < serverAddresses.length; i++) {
      String s = serverAddresses[i];
      s = s.trim();
      if (!s.endsWith("/"))
        s += "/";
      
      serverAddresses[i] = s;
    }
    
    return this;
  }
  
  /**
   * Return a ConfigurationFileFactory that retrieves files via this KnobService instance.
   * 
   * @param cacheDir If not null, then we store a copy of each retrieved file in this directory.
   *     On subsequent calls to createFactory, we use the stored files to initialize our state until
   *     each file can be re-fetched from the server. This prevents configuration file reads from
   *     stalling. If this directory does not exist, we create it.
   */
  public ConfigurationFileFactory createFactory(final File cacheDir) {
    if (cacheDir != null && !cacheDir.exists())
      cacheDir.mkdirs();
    
    return new ConfigurationFileFactory(){
      @Override protected ConfigurationFile createFileReference(String filePath) {
        return new HostedConfigurationFile(KnobService.this, filePath, cacheDir);
      }};
  }
  
  /**
   * Retrieve a configurtion file.
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
   * @return The response from the server. See {@link scalyr.com/httpApi}.
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
   * Create, update, or delete a configuration file.
   * 
   * @param path The file path, e.g. "/params.txt". Must begin with a slash.
   * @param expectedVersion Should normally be null. If not null, and not equal to the file's current version,
   *     then the file will not be modified. Enables atomic read/modify/write operations.
   * @param content New content for the file. Pass null if passing deleteFile = true.
   * @param deleteFile True to delete the file, false to create/update it.
   * 
   * @return The response from the server. See {@link scalyr.com/httpApi}.
   * 
   * @throws ScalyrException
   * @throws ScalyrNetworkException
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
   * Retrieve a list of all configuration files.
   * 
   * @return The response from the server. See {@link scalyr.com/httpApi}.
   * 
   * @throws ScalyrException
   * @throws ScalyrNetworkException
   * 
   * Note that paths are complete (absolute) pathnames, beginning with a slash, and are sorted
   * lexicographically.
   */
  public String listFiles() 
      throws ScalyrException, ScalyrNetworkException {
    JSONObject parameters = new JSONObject();
    parameters.put("token", apiToken);
    
    return invokeApi("listFiles", parameters);
  }

  /**
   * Invoke methodName on a selected server, sending the specified parameters as the request
   * body. Parse the response as JSON.
   * <p>
   * If "retriable" error (e.g. a network error) occurs, we retry on another server. We continue
   * retrying until a non-retriable error occurs, there are no more servers to try, or a reasonable
   * deadline (TuningConstants.MAXIMUM_RETRY_PERIOD_MS) expires.
   * 
   * @throws ScalyrException
   * @throws ScalyrNetworkException
   */
  protected String invokeApi(String methodName, JSONObject parameters) {
    // Produce a shuffled copy of the server addresses, so that load is distributed
    // across the servers.
    int N = serverAddresses.length;
    String[] shuffled = new String[N];
    System.arraycopy(serverAddresses, 0, shuffled, 0, N);
           
    for (int i = 0; i < N - 1; i++) {
      int j = i + random.nextInt(N - i);
      String temp = shuffled[i];
      shuffled[i] = shuffled[j];
      shuffled[j] = temp;
    }
    
    // Try the operation on each server in turn.
    long startTime = System.currentTimeMillis();
    int serverIndex = 0;
    while (true) {
      String serverAddress = shuffled[serverIndex];
      try {
        return invokeApiOnServer(serverAddress, methodName, parameters);
      } catch (ScalyrNetworkException ex) {
        // Fall into the loop and retry the operation on the next server.
        // If there are no more servers, or our deadline has expired, then
        // rethrow the exception.
        serverIndex++;
        
        long elapsed = System.currentTimeMillis() - startTime;
        
        if (serverIndex >= N || elapsed >= TuningConstants.MAXIMUM_RETRY_PERIOD_MS)
          throw ex;
        
        Logging.info("invokeApi: " + methodName + " failed on " + serverAddress + "; will retry", ex);
      }
    }
  }

  /**
   * Invoke serverAddress/methodName on the server, sending the specified parameters as the request
   * body. Parse the response as JSON.
   * 
   * @throws ScalyrException
   * @throws ScalyrNetworkException
   */
  protected String invokeApiOnServer(String serverAddress, String methodName, JSONObject parameters) {
    HttpURLConnection connection = null;  
    try {
      // Send the request.
      URL url = new URL(serverAddress + methodName);
      connection = (HttpURLConnection) url.openConnection();
      connection.setRequestMethod("POST");
      connection.setUseCaches(false);
      connection.setDoInput(true);
      connection.setConnectTimeout(TuningConstants.HTTP_CONNECT_TIMEOUT_MS);
      connection.setReadTimeout(TuningConstants.MAXIMUM_RETRY_PERIOD_MS);
      
      byte[] requestBody = parameters.toJSONString().getBytes(utf8);
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
  static final Executor asyncApiExecutor = Executors.newCachedThreadPool(new ThreadFactory(){
    private final AtomicInteger threadNumber = new AtomicInteger(1);
    @Override public Thread newThread(Runnable runnable) {
      Thread t = new Thread(runnable, "Scalyr " + threadNumber.getAndIncrement());
      t.setDaemon(true);
      return t;
    }});
}
