/* Scalyr client library
 * Copyright (c) 2011 Scalyr
 * All rights reserved
 */

package com.scalyr.api.params;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.scalyr.api.Callback;
import com.scalyr.api.ScalyrDeadlineException;
import com.scalyr.api.internal.Logging;
import com.scalyr.api.json.JSONObject;
import com.scalyr.api.json.JSONParser;
import com.scalyr.api.json.ParseException;

/**
 * Abstract interface for a parameter file.
 * <p>
 * Represents a particular pathname in a particular file repository.
 */
public abstract class ParameterFile {
  /**
   * Our pathname within the repository.
   */
  protected final String pathname;
  
  /**
   * The most recently observed state of this file. If we have not yet retrieved any version of
   * the file, holds null.
   */
  protected FileState fileState = null;
  
  /**
   * The result of parsing fileState.content as JSON. Null if fileState or fileState.content are
   * null. Will also be null if we haven't yet attempted to parse the current file.
   */
  private JSONObject cachedJson = null;
  
  /**
   * Parallels cachedJson. If fileState.content is not a valid JSON file, this field will hold an
   * exception object. Will be null if we haven't yet attempted to parse the current file.
   */
  private BadParameterFileException cachedJsonError = null;
  
  /**
   * All callbacks which have been registered to be notified when the file state changes.
   */
  private Set<Callback<ParameterFile>> updateListeners = new HashSet<Callback<ParameterFile>>();
  
  /**
   * Time when we last heard a definitive update from the server (or other base repository) regarding
   * the state of this file, or null if we never have. Allows us to derive a (very conservative) upper
   * bound on the staleness of our cached copy of the file.
   */
  private Long timeOfLastPoll = null;

  /**
   * Return a ParameterFileFactory that retrieves files from the local filesystem.
   * 
   * @param rootDir Directory in which parameter files are located. Pathnames are interpreted
   *     relative to this directory.
   * @param pollIntervalMs How often to check for changes to the filesystem, in milliseconds.
   */
  public static ParameterFileFactory makeLocalFileFactory(final File rootDir, final int pollIntervalMs) {
    return new ParameterFileFactory(){
      @Override protected ParameterFile createFileReference(String filePath) {
        return new LocalParameterFile(rootDir, filePath, pollIntervalMs);
      }
    };
  }
  
  protected ParameterFile(String pathname) {
    this.pathname = pathname;
  }
  
  /**
   * Return an upper bound, in milliseconds, on the "staleness" of our local view of the file.
   * <p>
   * This is a very conservative upper bound -- the actual staleness is rarely more than a second
   * or two. If we have not yet retrieved an initial copy of the file, returns null.
   */
  public synchronized Long getStalenessBound() {
    if (timeOfLastPoll == null)
      return null;
    
    return System.currentTimeMillis() - timeOfLastPoll;
  }
  
  protected synchronized void updateStalenessBound(long slopMs) {
    timeOfLastPoll = System.currentTimeMillis() + slopMs;
  }
  
  /**
   * Replace our current FileState with the new value, and notify everyone who needs notifying. If
   * the new FileState has an older verson than our current state, do nothing.
   */
  protected void setFileState(FileState value) {
    List<Callback<ParameterFile>> listenerSnapshot;
    
    synchronized (this) {
      if (fileState != null && value.version != 0 && fileState.version >= value.version) {
        return;
      }
      
      fileState = value;
      cachedJson = null;
      cachedJsonError = null;
      noteNewState();
      notifyAll();
    
      listenerSnapshot = new ArrayList<Callback<ParameterFile>>(updateListeners);
    }
    
    for (Callback<ParameterFile> updateListener : listenerSnapshot) {
      updateListener.run(this);
    }
  }
  
  /**
   * Invoked whenever setFileState receives a truly new (not out-of-order) update to the file state.
   */
  protected void noteNewState() {
  }
  
  /**
   * Return the pathname for this ParameterFile.
   */
  public String getPathname() {
    return pathname;
  }
  
  /**
   * True if we have fetched at least one version of the file, i.e. if get() will not block.
   */
  public synchronized boolean hasState() {
    return fileState != null;
  }
  
  /**
   * Return most recently observed state of the file.
   * 
   * If we have not yet managed to fetch the file, block until it can be fetched.
   */
  public synchronized FileState get() {
    blockUntilValue(null, null);
    
    return fileState;
  }
  
  /**
   * Return most recently observed state of the file.
   * <p>
   * If we have not yet managed to fetch the file, block until it can be retrieved or the
   * specified time interval elapses. If the time interval elapses, we throw a
   * ScalyrDeadlineException.
   * 
   * @param timeoutInMs Maximum time to block, in milliseconds. Null means no timeout (wait
   *     indefinitely).
   * 
   * @throws ScalyrDeadlineException
   */
  public synchronized FileState getWithTimeout(Long timeoutInMs) throws ScalyrDeadlineException {
    return getWithTimeout(timeoutInMs, timeoutInMs);
  }
  
  /**
   * Return the most recently observed state of the file.
   * <p>
   * If we have not yet managed to fetch the file, this will block until it can be retrieved
   * r the specified time interval elapses. If the time interval elapses, we throw a
   * ScalyrDeadlineException.
   * 
   * @param timeoutInMs Maximum time to block, in milliseconds. Null means no timeout (wait
   *     indefinitely).
   * @param undecayedTimeout If timeoutInMs is exceeded, we report undecayedTimeout as the deadline
   *     in the exception message. This is used when the caller waited for a while before invoking
   *     getWithTimeout. timeoutInMs is the *remaining* amount of time to wait, but undecayedTimeout
   *     indicates the full amount of time that was actually available for the file to be fetched.
   * 
   * @throws ScalyrDeadlineException
   */
  public synchronized FileState getWithTimeout(Long timeoutInMs, Long undecayedTimeout)
      throws ScalyrDeadlineException {
    blockUntilValue(timeoutInMs, undecayedTimeout);
    
    return fileState;
  }
  
  /**
   * Return the most recently observed state of the file's content, parsed as a JSON object.
   * <p>
   * If we have not yet managed to fetch the file, block until it can be retrieved. If the file does
   * not exist, return null. If the file is not in JSON format, throw BadParameterFileException.
   * 
   * @throws ScalyrDeadlineException
   */
  synchronized JSONObject getAsJson() throws BadParameterFileException {
    return getAsJsonWithTimeout(null, null);
  }
  
  /**
   * ONLY FOR USE BY INTERNAL SCALYR TESTS; DO NOT CALL THIS METHOD.
   */
  public JSONObject getAsJsonForTesting() throws BadParameterFileException {
    return getAsJson();
  }
  
  /**
   * Like {@link #getAsJson()}, but supports deadlines, as with {@link #getWithTimeout(Long)}.
   * 
   * @throws BadParameterFileException
   * @throws ScalyrDeadlineException
   */
  synchronized JSONObject getAsJsonWithTimeout(Long timeoutInMs)
      throws BadParameterFileException, ScalyrDeadlineException {
    return getAsJsonWithTimeout(timeoutInMs, timeoutInMs);
  }
  
  /**
   * Like {@link #getAsJson()}, but supports deadlines, as with {@link #getWithTimeout(Long, Long)}.
   * 
   * @throws BadParameterFileException
   * @throws ScalyrDeadlineException
   */
  synchronized JSONObject getAsJsonWithTimeout(Long timeoutInMs, Long undecayedTimeout)
      throws BadParameterFileException, ScalyrDeadlineException {
    FileState currentState = getWithTimeout(timeoutInMs, undecayedTimeout);
    
    if (currentState.content == null)
      return null;
    
    if (cachedJsonError != null)
      throw new BadParameterFileException(cachedJsonError); // We clone the exception to snapshot the current stack.
    
    if (cachedJson == null) {
      Object parsed;
      try {
        if (currentState.content.length() == 0)
          parsed = new JSONObject(); // treat an empty file as an empty object, i.e. as {}
        else
          parsed = new JSONParser().parse(new StringReader(currentState.content));
      } catch (IOException ex) {
        cachedJsonError = new BadParameterFileException("IOException while parsing JSON file", ex);
        Logging.warn("Error parsing [" + this + "] as JSON", cachedJsonError);
        throw cachedJsonError;
      } catch (ParseException ex) {
        cachedJsonError = new BadParameterFileException("File is not in valid JSON format");
        Logging.warn("Error parsing [" + this + "] as JSON", cachedJsonError);
        throw cachedJsonError;
      }
      if (parsed instanceof JSONObject) {
        cachedJson = (JSONObject) parsed;
      } else {
        cachedJsonError = new BadParameterFileException("File is not in valid JSON format or does not have" +
        		" an object at the top level (does not begin with an open-brace)");
        Logging.warn("Error parsing [" + this + "] as JSON", cachedJsonError);
        throw cachedJsonError;
      }
    }
    
    return cachedJson;
  }
  
  /**
   * Block until we've retrieved the file's initial state from the server.
   * 
   * If timeoutInMs is not null, and the file has still not been retrieved after (approximately) that
   * many milliseconds, we throw a ScalyrDeadlineException.
   * 
   * undecayedTimeout is similar to timeoutInMs, but represents the amount of time that was available
   * at the beginning of some higher-level operation. Thus, it is either equal to timeoutInMs, or has
   * been reduced by the time that elapsed before this blockUntilValue called. Used only in error reporting.
   * 
   * Caller must hold a lock on "this".
   */
  private void blockUntilValue(Long timeoutInMs, Long undecayedTimeout) {
    if (fileState != null)
      return;
    
    long startNanos = System.nanoTime();
    long deadlineTime = (timeoutInMs != null) ? (System.currentTimeMillis() + timeoutInMs) : 0;  
    
    while (fileState == null) {
      try {
        if (timeoutInMs != null) {
          long msRemaining = deadlineTime - System.currentTimeMillis();
          if (msRemaining > 0)
            this.wait(msRemaining);
          else
            throw new ScalyrDeadlineException("Initial fetch of parameter file [" +
            		pathname + "] from the server",
            		undecayedTimeout);
        } else {
          this.wait();
        }
      } catch (InterruptedException ex) {
        throw new RuntimeException(ex);
      }
    }
  }
  
  /**
   * Register a callback to be invoked whenever the file's state changes.
   * <p>
   * If the callback was already registered, we do nothing (it remains registered).
   */
  public synchronized void addUpdateListener(Callback<ParameterFile> updateListener) {
    updateListeners.add(updateListener);
  }
  
  /**
   * De-register a callback.
   * <p>
   * If the callback was not registered, we do nothing.
   */
  public synchronized void removeUpdateListener(Callback<ParameterFile> updateListener) {
    updateListeners.remove(updateListener);
  }
  
  /**
   * Encapsulates information about the state of a parameter file.
   */
  public static class FileState {
    /**
     * Version number for this snapshot of the file. Deleted files have version 0. May be undefined for
     * some ParameterFile implementations.
     */
    protected final long version;
    
    /**
     * File content. Deleted files have null content.
     */
    public final String content;
    
    /**
     * Date/time when the file was created. Null for a deleted file.
     */
    public final Date creationDate;
    
    /**
     * Date/time when the file was last modified. Null for a deleted file.
     */
    public final Date modificationDate;
    
    public FileState(long version, String content, Date creationDate, Date modificationDate) {
      this.version = version;
      this.content = content;
      this.creationDate = creationDate;
      this.modificationDate = modificationDate;
    }
  }
}
