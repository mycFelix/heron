// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package org.apache.heron.integration_test.core;

import org.apache.heron.api.bolt.BaseRichBolt;

// We keep this since we want to be consistent with earlier framework to reuse test topologies
public abstract class BaseBatchBolt extends BaseRichBolt implements IBatchBolt {
  private static final long serialVersionUID = 7380672976877532671L;
}