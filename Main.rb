gem 'parallel'
require 'parallel'
require_relative 'map_reduce'
require 'json'
require 'time'
require 'thwait'


start = Time.now

files = %w(employees2.csv)

threads = []

begin
  Parallel.each(files, in_threads: 1) do |file_name|

    data_types_order = MapReduce::Mapper.new("EMPLOYEES",
                                             file_name, "salary == 1000").map

    MapReduce::Shuffler.new(file_name, "salary", data_types_order).memory_shuffle
  end

end

=begin
files.each do |file_name|

    data_types_order = MapReduce::Mapper.new("EMPLOYEES",
                                             file_name, "salary == 1000").map

    # MapReduce::Shuffler.new("employees1.csv", "department_id", data_types_order).shuffle
end
=end

# ThreadsWait.all_waits(threads)


puts Time.now - start