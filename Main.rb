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

  aggregation_columns = [{:function=>:AVG,:index=>ExecutionPlanUtilities::get_column_index("employees", "salary")},{:function=>:MAX,:index=>ExecutionPlanUtilities::get_column_index("employees", "salary")},{:function=>:MIN,:index=>ExecutionPlanUtilities::get_column_index("employees", "employee_id")},]

  input_file_name = MapReduce::Mapper.new.map(records, aggregation_columns)

  MapReduce::Reducer.new(input_file_name, aggregation_columns).reduce_without_shuffle

  if aggregation_columns.empty?

    selection_columns << 0 + ExecutionPlanUtilities::get_column_index("avg(employees", "salary)")
    selection_columns << 0 + ExecutionPlanUtilities::get_column_index("max(employees", "salary)")
    selection_columns << 0 + ExecutionPlanUtilities::get_column_index("min(employees", "salary)")

  end
  records.sort_by!{|record| [ ]}
  unless selection_columns.empty?
    records.map!{|record| record.split(",").values_at(*selection_columns).join(",")}
  end

  records.uniq! if false

  puts records

  puts aggregation_columns.to_s

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