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

begin


  grouping_columns = []
  ordering_columns = []
  selection_columns = []
  records = []

  join_type = "null"
  employees_table_location = ExecutionPlanUtilities.get_table_location("employees")
  employees_csv_files = ExecutionPlanUtilities.get_csv_files(employees_table_location)
  employees_file_index = 0
  employees_pos = 0

  until employees_file_index == employees_csv_files.length

    employees_line, employees_file_index, employees_pos = ExecutionPlanUtilities.read_record(employees_table_location, employees_csv_files, employees_file_index, employees_pos)

    records << employees_line.chomp if true

  end

  having_conditions = [{:function=>:COUNT,:index=>1,:condition=>"<6",:function_after_condition=>false,:type=>:INT},]
  aggregation_columns = [{:function=>:COUNT,:index=>ExecutionPlanUtilities::get_column_index("employees", "employee_id"),:type=>:INT,:distinct=>nil},]

  grouping_columns << ExecutionPlanUtilities::get_column_index("employees", "department_id") + 0


  if aggregation_columns.empty?

    selection_columns << 0 + ExecutionPlanUtilities::get_column_index("count(employees", "employee_id)")
    selection_columns << 0 + ExecutionPlanUtilities::get_column_index("employees", "department_id")

  end
  records.sort_by!{|record| [ ]}
  unless selection_columns.empty?
    records.map!{|record| record.split(",").values_at(*selection_columns).join(",")}
  end

  records.uniq! if false
  unless aggregation_columns.empty?

    mapper_file_name = MapReduce::Mapper.new.map(records, grouping_columns, aggregation_columns)

    shuffler_file_name = MapReduce::Shuffler.new(mapper_file_name).shuffle

    MapReduce::Reducer.new(shuffler_file_name, grouping_columns, aggregation_columns, having_conditions).reduce

  end

  puts records

end

puts Time.now - start
