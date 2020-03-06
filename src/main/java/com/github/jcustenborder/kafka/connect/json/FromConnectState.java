/**
 * Copyright © 2020 Jeremy Custenborder (jcustenborder@gmail.com)
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
package com.github.jcustenborder.kafka.connect.json;

import org.apache.kafka.common.header.Header;

class FromConnectState {
  final Header header;
  final FromConnectVisitor visitor;


  private FromConnectState(Header header, FromConnectVisitor visitor) {
    this.header = header;
    this.visitor = visitor;
  }

  public static FromConnectState of(Header header, FromConnectVisitor visitor) {
    return new FromConnectState(header, visitor);
  }
}
