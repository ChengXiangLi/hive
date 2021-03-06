/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.hplsql;

import java.util.ArrayList;

/**
 * HPL/SQL block scope
 */
public class Scope {
  
  // Types  
  public enum Type { FILE, BEGIN_END, LOOP, HANDLER, ROUTINE };
  
  // Local variables
  ArrayList<Var> vars = new ArrayList<Var>();
  // Condition handlers
  ArrayList<Handler> handlers = new ArrayList<Handler>();
  
  Scope parent;
  Type type;
  
  Scope(Type type) {
    this.parent = null;
    this.type = type;
  }

  Scope(Scope parent, Type type) {
    this.parent = parent;
    this.type = type;
  }
  
  /**
   * Add a local variable
   */
  void addVariable(Var var) {
    vars.add(var);
  }
  
  /**
   * Add a condition handler
   */
  void addHandler(Handler handler) {
    handlers.add(handler);
  }
  
  /**
   * Get the parent scope
   */
  Scope getParent() {
    return parent;
  }
}
