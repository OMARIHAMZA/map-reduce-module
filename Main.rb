gem 'parallel'
require 'parallel'
require_relative 'map_reduce'
require 'json'
require 'time'
require 'thwait'


start = Time.now

files = %w(employees.csv)

threads = []

begin
  Parallel.each(files, in_threads: 1) do |file_name|


    data_types_order, data_members = MapReduce::Mapper.new("EMPLOYEES",
                                             file_name, "EMPLOYEE_NAME == \"Hamza\"").map


    MapReduce::Shuffler.new(file_name, "EMPLOYEE_NAME", data_types_order).memory_shuffle

    puts MapReduce::Reducer.new(file_name, "EMPLOYEE_NAME", data_types_order, data_members).reduce

  end

end


puts Time.now - start