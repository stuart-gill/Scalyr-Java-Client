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

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

import com.scalyr.api.internal.Logging;
import com.scalyr.api.internal.ScalyrUtil;

/**
 * ConfigurationFile implementation that reads from a file in the local filesystem.
 */
public class LocalConfigurationFile extends ConfigurationFile {
  private final File rootDir;
  
  /**
   * Filesystem location of the file we read from.
   */
  private final File file;
  
  /**
   * Incremented on each change to the file (the initial state counts as a change). Reset to 0 if
   * the file does not exist. Used to generate version stamps for FileState.
   */
  private int versionCounter = 0;
  
  /**
   * File's lastModified value when we most recently fetched it, or null if we've never (successfully)
   * fetched it. For a non-existent file, we store 0.
   */
  private Long lastModified;
  
  /**
   * File's byte length when we most recently fetched it, or null if we've never (successfully)
   * fetched it. For a non-existent file, we store -1.
   */
  private Long fileLen;
  
  /**
   * File content when we last read it, or null if the file did not exist.
   */
  private String fileContent;
  
  /**
   * Number of successive fetchFileState executions which have found the file to be unchanged.
   */
  private long unchangedInARow = 0;
  
  /**
   * Timestamp of the most recent transition from unchangedInARow = 0 to unchangedInARow > 0.
   * Undefined if unchangedInARow is 0.  
   */
  private Long unchangedStartTime = null;
  
  /**
   * Timer used to poll for file changes. Shared across all files. 
   */
  private static Timer pollTimer;
  
  /**
   * Task used to poll for changes to this file.
   */
  private final TimerTask pollTask;
  
  /**
   * Construct a LocalConfigurationFile.
   * 
   * @param rootDir Directory to which filePath is relative.
   * @param filePath Path/name for this file.
   * @param pollIntervalMs How often to check for changes to the file (in milliseconds).
   */
  LocalConfigurationFile(File rootDir, String filePath, int pollIntervalMs) {
    super(filePath);
    
    this.rootDir = rootDir;
    
    // Fetch the file's initial state.
    file = new File(rootDir, removeLeadingSlash(filePath));
    fetchFileState(true);
    
    // Kick off a timer task to periodically check for changes to the file.
    synchronized (LocalConfigurationFile.class) {
      if (pollTimer == null)
        pollTimer = new Timer();
    }
    
    pollTask = new TimerTask(){
      @Override public void run() {
        try {
          fetchFileState(false);
        } catch (Exception ex) {
          Logging.warn("Error reading local configuration file [" + file.getAbsolutePath() + "]", ex);
        }
      }};
    pollTimer.schedule(pollTask, pollIntervalMs, pollIntervalMs);
  }
  
  @Override public synchronized void close() {
    pollTask.cancel();
    super.close();
  }
  
  @Override public String toString() {
    return "<configuration file \"" + pathname + "\" in filesystem \"" + rootDir.getAbsolutePath() + "\">";
  }
  
  private static String removeLeadingSlash(String filePath) {
    if (filePath.startsWith("/"))
      return filePath.substring(1);
    else
      return filePath;
  }
  
  /**
   * Read the file's latest state, and update the ConfigurationFile as appropriate.
   */
  private void fetchFileState(boolean initialFetch) {
    // Fetch the file's current metadata.
    long newLastModified;
    long newLength;
    if (file.exists()) {
      newLastModified = file.lastModified();
      newLength = file.length();
    } else {
      newLastModified = 0;
      newLength = -1;
    }
    
    // If the metadata hasn't changed since our last poll, exit. Except, don't exit unless the file has
    // been "not changed" at least twice in a row across at least two seconds. Otherwise, if there are
    // two changes in the same second, and the second change doesn't modify the file's length, we might miss
    // it.
    if (lastModified != null && lastModified == newLastModified && fileLen != null && fileLen == newLength &&
        unchangedInARow >= 2 && System.currentTimeMillis() >= unchangedStartTime + 2000) {
      updateStalenessBound(0);
      return;
    }
    
    lastModified = newLastModified;
    fileLen      = newLength;
    
    // Retrieve the file's content, and record a new FileState.
    String newFileContent;
    if (fileLen >= 0) {
      try {
        newFileContent = ScalyrUtil.readFileContent(file);
      } catch (UnsupportedEncodingException ex) {
        Logging.warn("Error reading file [" + file.getAbsolutePath() + "]", ex);
        return;
      } catch (IOException ex) {
        Logging.warn("Error reading file [" + file.getAbsolutePath() + "]", ex);
        return;
      }
    } else {
      newFileContent = null;
    }
    
    updateStalenessBound(0);
    if (!initialFetch && ScalyrUtil.equals(fileContent, newFileContent)) {
      if (unchangedInARow == 0)
        unchangedStartTime = System.currentTimeMillis();
        
      unchangedInARow++;
      return;
    }
      
    unchangedInARow = 0;
    fileContent = newFileContent;
    
    if (fileLen >= 0) {
      // Note that Java doesn't provide a way to access the file's creation date, so we report it as being
      // equal to the last modification date.
      versionCounter++;
      setFileState(new FileState(versionCounter, newFileContent, new Date(newLastModified), new Date(newLastModified)));
    } else {
      versionCounter = 0;
      
      setFileState(new FileState(0, null, null, null));
    }
  }
}
