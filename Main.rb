gem 'parallel'
require 'parallel'
require 'json'
require 'time'
require 'thwait'
require 'fileutils'
require_relative 'map_reduce'
require_relative 'enumerable'
require_relative 'execution_plan_utilities'

start = Time.now

files = %w(employees.csv)

threads = []

begin

  join_type = "null"
  grouping_columns = []

  ordering_columns = []

  selection_columns = []

  records = []
  employees_table_location = ExecutionPlanUtilities.get_table_location("employees")
  employees_csv_files = ExecutionPlanUtilities.get_csv_files(employees_table_location)
  employees_file_index = 0
  employees_pos = 0

  until employees_file_index == employees_csv_files.length

    employees_line, employees_file_index, employees_pos = ExecutionPlanUtilities.read_record(employees_table_location, employees_csv_files, employees_file_index, employees_pos)

    records << employees_line.chomp if true

  end

  aggregation_columns = [{:function=>:COUNT,:index=>-1},{:function=>:COUNT,:index=>ExecutionPlanUtilities::get_column_index("employees", "salary"),:type=>:INT,:distinct=>:DISTINCT},]

  grouping_columns << ExecutionPlanUtilities::get_column_index("employees", "salary") + 0

  records.sort_by!{|record| [ ]}
  unless selection_columns.empty?
    records.map!{|record| record.split(",").values_at(*selection_columns).join(",")}
  end

  records.uniq! if false
  mapper_file_name = MapReduce::Mapper.new.map(records, grouping_columns, aggregation_columns)

  shuffler_file_name = MapReduce::Shuffler.new(mapper_file_name).shuffle

  MapReduce::Reducer.new(shuffler_file_name, grouping_columns, aggregation_columns).reduce

  puts records

=begin
  input_file_name = MapReduce::Mapper.new.map(records, aggregation_columns)

  MapReduce::Reducer.new(input_file_name, aggregation_columns).reduce_without_shuffle
=end

=begin
  Parallel.each(files, in_threads: 1) do |file_name|


    data_types_order, data_members = MapReduce::Mapper.new("EMPLOYEES",
                                                           file_name, "SALARY > 1000").map


    MapReduce::Shuffler.new(file_name, "SALARY", data_types_order).memory_shuffle

    puts MapReduce::Reducer.new(file_name, "SALARY", data_types_order, data_members).reduce

  end
=end

end


puts Time.now - start