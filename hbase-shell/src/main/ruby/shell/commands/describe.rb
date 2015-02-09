#
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
    class Describe < Command
      def help
        return <<-EOF
Describe the named table. For example:
  hbase> describe 't1'
  hbase> describe 'ns1:t1'

Alternatively, you can use the abbreviated 'desc' for the same thing.
  hbase> desc 't1'
  hbase> desc 'ns1:t1'
EOF
      end

      def command(table)
        now = Time.now

        desc = admin.get_table_descriptor(table)
        column_families = desc.getColumnFamilies()
        attrs = desc.getValues()
        config = desc.getConfiguration()

        formatter.header(["Table " + table.to_s + " is " + if admin.enabled?(table) then "ENABLED" else "DISABLED" end])
        # Attributes
        table_info = "TABLE_ATTRIBUTES => {"
        unless attrs.empty?
          attrs_str = ""
          attrs.each do |key, value|
            attrName = org.apache.hadoop.hbase.util.Bytes.toString(key.get, key.getOffset, key.getLength)
            attrValue = org.apache.hadoop.hbase.util.Bytes.toString(value.get, value.getOffset, value.getLength)
            if attrName == "UUID"
              attrValue = org.apache.hadoop.hbase.util.Bytes.toStringBinary(value.get, value.getOffset, value.getLength);
            end
            attrs_str << ", #{attrName} => '#{attrValue}'"
          end
          table_info << attrs_str[2..-1]
        end
        table_info << "}"
        formatter.row([table_info], true)
        # Configuration
        unless config.empty?
          table_config = "TABLE_CONFIGURATION => {"
          config_str = ""
          config.each do |key, value|
            config_str << ", #{key} => '#{value}'"
          end
          table_config << config_str[2..-1]
          table_config << "}"
          formatter.row([table_config], true)
        end
        # Column Families
        formatter.header([ "COLUMN FAMILIES DESCRIPTION" ])
        column_families.each do |column_family|
          formatter.row([ column_family.to_s ], true)
        end
        formatter.footer(now)
      end
    end
  end
end
