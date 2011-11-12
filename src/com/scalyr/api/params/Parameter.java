/* Scalyr client library
 * Copyright (c) 2011 Scalyr
 * All rights reserved
 */

package com.scalyr.api.params;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.scalyr.api.Callback;
import com.scalyr.api.Converter;
import com.scalyr.api.ScalyrDeadlineException;
import com.scalyr.api.internal.Logging;
import com.scalyr.api.internal.ScalyrUtil;
import com.scalyr.api.json.JSONObject;

/**
 * Encapsulates a specific entry in a JSON-format parameter file.
 * <p>
 * It is generally best to use a type-specific subclass of Parameter, such as
 * Parameter.Integer or Parameter.String, rather than casting the result of
 * Parameter.get() yourself. This is because Java casting does not know how to
 * convert between numeric types (e.g. Double -> Integer), and so simple casting
 * is liable to throw ClassCastException.
 */
public class Parameter {
  /**
   * Files in which we look for the value. We use the first file that defines our key.
   */
  private final ParameterFile[] files;
  
  /**
   * The key we look for in each file.
   */
  private final java.lang.String key;
  
  /**
   * Value used if no file defines the key.
   */
  private final Object defaultValue;
  
  /**
   * False until we first retrieve a value from the parameter file.
   */
  private boolean hasValue;
  
  /**
   * The most recently observed value. Undefined if hasValue is false.
   */
  private Object value;
  
  /**
   * Callback used to listen for changes in the underlying file, or null if we are not
   * currently listening. We listen if and only if updateListeners is nonempty.
   */
  private Callback<ParameterFile> fileListener;
  
  /**
   * All callbacks which have been registered to be notified when our value changes.
   */
  private Set<Callback<Parameter>> updateListeners = new HashSet<Callback<Parameter>>();
  
  /**
   * @param key The key to look for (a fieldname of the top-level JSON object in the file).
   * @param defaultValue Value to return from {@link #get()} if the file does not exist or does not
   *     contain the key.
   * @param files The files in which we look for the value. We use the first file that
   *     defines the specified key.
   */
  public Parameter(java.lang.String key, Object defaultValue, ParameterFile ... files) {
    this.files = files;
    this.key = key;
    this.defaultValue = defaultValue;
  }
  
  /**
   * Return a value from the first parameter file which contains the specified key.
   * <p>
   * Ignore any files which do not exist. If none of the files contain the key, return defaultValue.
   * If any file has not yet been retrieved from the server, we block until it can be retrieved.
   * 
   * @param valueKey A key into the top-level object in that file.
   * @param defaultValue Value to return if the file does not exist or does not contain the key.
   * @param files The files in which we search for the value.
   */
  public static Object get(java.lang.String valueKey, Object defaultValue, ParameterFile ... files) {
    return new Parameter(valueKey, defaultValue, files).get();
  }
  
  /**
   * Like {@link #get(java.lang.String, Object, ParameterFile[])}, but converts the value to an Integer.
   */
  public static java.lang.Integer getInteger(java.lang.String valueKey, java.lang.Integer defaultValue, ParameterFile ... files) {
    return Converter.toInteger(get(valueKey, defaultValue, files));
  }
  
  /**
   * Like {@link #get(java.lang.String, Object, ParameterFile[])}, but converts the value to an Long.
   */
  public static java.lang.Long getLong(java.lang.String valueKey, java.lang.Long defaultValue, ParameterFile ... files) {
    return Converter.toLong(get(valueKey, defaultValue, files));
  }
  
  /**
   * Like {@link #get(java.lang.String, Object, ParameterFile[])}, but converts the value to a Double.
   */
  public static java.lang.Double getDouble(java.lang.String valueKey, java.lang.Double defaultValue, ParameterFile ... files) {
    return Converter.toDouble(get(valueKey, defaultValue, files));
  }
  
  /**
   * Like {@link #get(java.lang.String, Object, ParameterFile[])}, but converts the value to a Boolean.
   */
  public static java.lang.Boolean getBoolean(java.lang.String valueKey, java.lang.Boolean defaultValue, ParameterFile ... files) {
    return Converter.toBoolean(get(valueKey, defaultValue, files));
  }
  
  /**
   * Like {@link #get(java.lang.String, Object, ParameterFile[])}, but converts the value to a String.
   */
  public static java.lang.String getString(java.lang.String valueKey, java.lang.String defaultValue, ParameterFile ... files) {
    return Converter.toString(get(valueKey, defaultValue, files));
  }
  
  /**
   * Return the value at the specified key in our file.
   * 
   * If the file does not exist or does not contain the key, return our default value.
   * If the file has not yet been retrieved from the server, we block until it can be retrieved.
   */
  public Object get() {
    return getWithTimeout(null);
  }
  
  /**
   * Like get(), but if the file has not yet been retrieved from the server, and the specified time
   * interval elapses before the file is retrieved from the server, throw a ScalyrDeadlineException.
   * 
   * @throws ScalyrDeadlineException
   */
  public Object getWithTimeout(java.lang.Long timeoutInMs) throws ScalyrDeadlineException {
    long entryTime = (timeoutInMs != null) ? System.currentTimeMillis() : 0;
    
    Object newValue = defaultValue;
    for (ParameterFile file : files) {
      JSONObject parsedFile;
      try {
        if (timeoutInMs != null) {
          long elapsed = Math.max(System.currentTimeMillis() - entryTime, 0);
          parsedFile = file.getAsJsonWithTimeout(timeoutInMs - elapsed, timeoutInMs);
        } else {
          parsedFile = file.getAsJson();
        }
      } catch (BadParameterFileException ex) {
        parsedFile = null;
        
        Logging.info("Parameter: ignoring file [" + file + "]: it does not contain valid JSON");
      }
      
      if (parsedFile != null && parsedFile.containsKey(key)) {
        newValue = parsedFile.get(key);
        break;
      }
    }
    
    synchronized (this) {
      Object oldValue = value;
      boolean hadValue = hasValue;
      
      value = newValue;
      hasValue = true;
      
      if (!hadValue || !ScalyrUtil.equals(value, oldValue)) {
        List<Callback<Parameter>> listenerSnapshot = new ArrayList<Callback<Parameter>>(updateListeners);
        for (Callback<Parameter> updateListener : listenerSnapshot) {
          updateListener.run(this);
        }
      }
      
      return value;
    }
  }
  
  /**
   * Register a callback to be invoked whenever our value changes.
   */
  public synchronized void addUpdateListener(Callback<Parameter> updateListener) {
    if (updateListeners.size() == 0) {
      fileListener = new Callback<ParameterFile>(){
        @Override public void run(ParameterFile updatedParameterFile) {
          if (allFilesHaveValues())
            get();
        }};
      for (ParameterFile file : files)
        file.addUpdateListener(fileListener);
    }
    updateListeners.add(updateListener);
  }
  
  protected synchronized boolean allFilesHaveValues() {
    for (ParameterFile file : files)
      if (!file.hasState())
        return false;
    
    return true;
  }

  /**
   * De-register a callback. If the callback was not registered, we do nothing.
   */
  public synchronized void removeUpdateListener(Callback<Parameter> updateListener) {
    updateListeners.remove(updateListener);
    
    if (updateListeners.size() == 0) {
      for (ParameterFile file : files)
        file.removeUpdateListener(fileListener);
      fileListener = null;
    }
  }
  
  /**
   * Subclass of Parameter which is specialized for Integer values.
   */
  public static class Integer extends Parameter {
    public Integer(java.lang.String valueKey, java.lang.Integer defaultValue, ParameterFile ... files) {
      super(valueKey, defaultValue, files);
    }
    
    @Override public java.lang.Integer get() {
      return Converter.toInteger(super.get());
    }
    
    @Override public java.lang.Integer getWithTimeout(java.lang.Long timeoutInMs) throws ScalyrDeadlineException {
      return Converter.toInteger(super.getWithTimeout(timeoutInMs));
    }
  }
  
  /**
   * Subclass of Parameter which is specialized for Long values.
   */
  public static class Long extends Parameter {
    public Long(java.lang.String valueKey, java.lang.Long defaultValue, ParameterFile ... files) {
      super(valueKey, defaultValue, files);
    }
    
    @Override public java.lang.Long get() {
      return Converter.toLong(super.get());
    }
    
    @Override public java.lang.Long getWithTimeout(java.lang.Long timeoutInMs) throws ScalyrDeadlineException {
      return Converter.toLong(super.getWithTimeout(timeoutInMs));
    }
  }
  
  /**
   * Subclass of Parameter which is specialized for Double values.
   */
  public static class Double extends Parameter {
    public Double(java.lang.String valueKey, java.lang.Double defaultValue, ParameterFile ... files) {
      super(valueKey, defaultValue, files);
    }
    
    @Override public java.lang.Double get() {
      return Converter.toDouble(super.get());
    }
    
    @Override public java.lang.Double getWithTimeout(java.lang.Long timeoutInMs) throws ScalyrDeadlineException {
      return Converter.toDouble(super.getWithTimeout(timeoutInMs));
    }
  }
  
  /**
   * Subclass of Parameter which is specialized for Boolean values.
   */
  public static class Boolean extends Parameter {
    public Boolean(java.lang.String valueKey, java.lang.Boolean defaultValue, ParameterFile ... files) {
      super(valueKey, defaultValue, files);
    }
    
    @Override public java.lang.Boolean get() {
      return Converter.toBoolean(super.get());
    }
    
    @Override public java.lang.Boolean getWithTimeout(java.lang.Long timeoutInMs) throws ScalyrDeadlineException {
      return Converter.toBoolean(super.getWithTimeout(timeoutInMs));
    }
  }
  
  /**
   * Subclass of Parameter which is specialized for String values.
   */
  public static class String extends Parameter {
    public String(java.lang.String valueKey, java.lang.String defaultValue, ParameterFile ... files) {
      super(valueKey, defaultValue, files);
    }
    
    @Override public java.lang.String get() {
      return Converter.toString(super.get());
    }
    
    @Override public java.lang.String getWithTimeout(java.lang.Long timeoutInMs) throws ScalyrDeadlineException {
      return Converter.toString(super.getWithTimeout(timeoutInMs));
    }
  }
}
