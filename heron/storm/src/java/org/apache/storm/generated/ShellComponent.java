//  Copyright 2016 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License
package org.apache.storm.generated;

/**
 * Created by Felix on 16/10/24.
 */
public class ShellComponent {
  private String executionCommand;
  private String script;

  public ShellComponent(String execution_command, String script) {
    this.executionCommand = execution_command;
    this.script = script;
  }

  public String getExecutionCommand() {
    return executionCommand;
  }

  public void setExecutionCommand(String execution_command) {
    this.executionCommand = execution_command;
  }

  public String getScript() {
    return script;
  }

  public void setScript(String script) {
    this.script = script;
  }
}
