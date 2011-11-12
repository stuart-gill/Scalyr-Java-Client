/* Scalyr client library
 * Copyright (c) 2011 Scalyr
 * All rights reserved
 */

package com.scalyr.api.params;

import java.util.HashMap;
import java.util.Map;

/**
 * Factory which encapsulates a collection of parameter files, accessible by pathname.
 */
public abstract class ParameterFileFactory {
  /**
   * Maps pathname to ParameterFile instance. Used to avoid creating multiple ParameterFiles
   * for the same file.
   * 
   * Access synchronized on "this".
   * 
   * TODO: use a finalizer to remove entries from fileMap when they're no longer referenced.
   * For this to work, we'll need to use weak references in fileMap.
   */
  private final Map<String, ParameterFile> fileMap = new HashMap<String, ParameterFile>();
  
  /**
   * Return an object representing the specified pathname in this collection.
   * <p>
   * Note that we always return an object, even if no file currently exists at the
   * specified pathname.
   */
  public synchronized ParameterFile getFile(String pathname) {
    // If we already have a ParameterFile object for this path, return it. Otherwise,
    // create one.
    ParameterFile file = fileMap.get(pathname);
    if (file == null) {
      file = createFileReference(pathname);
      fileMap.put(pathname, file);
    }
    
    return file;
  }
  
  /**
   * Create a ParameterFile object for the file at the given path in this repository.
   */
  protected abstract ParameterFile createFileReference(String filePath);
}
