#!/usr/bin/env ruby

# Verify that all source files have the license header.

LICENSE = <<EOF
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
EOF

LICENSE_REGEX = Regexp.new("^#{Regexp.quote(LICENSE)}",Regexp::MULTILINE)


def file_has_license?(file)
  LICENSE_REGEX =~ File.read(file)
end

def find_unlicensed_files(dir)
  Dir["#{dir}/**/*.java"].find_all do |file|
    not file_has_license? file
  end
end

files_without_license = []

source_dirs = ["src","test"]

source_dirs.each do |dir|
  files_without_license.concat find_unlicensed_files(dir)
end

if files_without_license.size > 0
  puts "The following file are missing the license header:"
  files_without_license.each { |file| puts "  #{file}"}
  exit false
else
  puts "All source files have the license header"
end
