/* Scalyr client library
 * Copyright (c) 2011 Scalyr
 * All rights reserved
 */

package com.scalyr.api.json;

import java.io.IOException;
import java.io.Writer;

public interface JSONStreamAware {
  /**
   * write JSON string to out.
   */
  void writeJSONString(Writer out) throws IOException;
}
