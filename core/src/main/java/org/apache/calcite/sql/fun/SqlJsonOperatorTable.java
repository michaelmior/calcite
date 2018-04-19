/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.sql.fun;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;

/**
 * Implementation of {@link org.apache.calcite.sql.SqlOperatorTable} containing
 * JSON functions.
 */
public class SqlJsonOperatorTable extends ReflectiveSqlOperatorTable {
  //~ Static fields/initializers ---------------------------------------------

  /**
   * The JSON operator table.
   */
  private static SqlJsonOperatorTable instance;

  /**
   * The <code>ISJSON</code> function.
   */
  public static final SqlFunction ISJSON =
      new SqlFunction(
          "ISJSON",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.BOOLEAN,
          null,
          null,
          SqlFunctionCategory.JSON);

  //~ Methods ----------------------------------------------------------------

  /**
   * Returns the JSON operator table, creating it if necessary.
   */
  public static synchronized SqlJsonOperatorTable instance() {
    if (instance == null) {
      // Creates and initializes the JSON operator table.
      // Uses two-phase construction, because we can't initialize the
      // table until the constructor of the sub-class has completed.
      instance = new SqlJsonOperatorTable();
      instance.init();
    }
    return instance;
  }
}

// End SqlJsonOperatorTable.java
