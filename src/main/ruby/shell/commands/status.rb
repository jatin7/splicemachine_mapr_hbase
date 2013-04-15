#
# Copyright 2010 The Apache Software Foundation
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

module Shell
  module Commands
    class Status < Command
      def help
        return <<-EOF
Show cluster status. Can be 'summary', 'simple', or 'detailed'. The
default is 'summary'. Examples:

  hbase> status
  hbase> status 'simple'
  hbase> status 'summary'
  hbase> status 'detailed'
EOF
      end

      def command(format = 'summary')
        if m7admin.m7_available? && m7admin.is_m7_default?
          puts "This client is configured to use MapR tables only. HBase status is not available."
        else
          admin.status(format)
        end
        if m7admin.m7_available?
          puts "MapR cluster status can be viewed using the 'maprcli dashboard info' command or the UI."
        end
      end
    end
  end
end
