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

  join_type = "JOIN"
  grouping_columns = []

  ordering_columns = []

  selection_columns = []

  records = []
  employees_1_department_id_index=  ExecutionPlanUtilities::get_column_index("employees", "department_id")
  departments_0_department_id_index=  ExecutionPlanUtilities::get_column_index("departments", "department_id")
  employees_1_table_location = ExecutionPlanUtilities.get_table_location("employees")
  employees_1_csv_files = ExecutionPlanUtilities.get_csv_files(employees_1_table_location)
  employees_1_file_index = 0
  employees_1_pos = 0

  until employees_1_file_index == employees_1_csv_files.length

    employees_1_line, employees_1_file_index, employees_1_pos = ExecutionPlanUtilities.read_record(employees_1_table_location, employees_1_csv_files, employees_1_file_index, employees_1_pos)
    record_1 = "" + employees_1_line.chomp

    employees_1_joined_flag = false
    departments_0_table_location = ExecutionPlanUtilities.get_table_location("departments")
    departments_0_csv_files = ExecutionPlanUtilities.get_csv_files(departments_0_table_location)
    departments_0_file_index = 0
    departments_0_pos = 0

    until departments_0_file_index == departments_0_csv_files.length

      departments_0_line, departments_0_file_index, departments_0_pos = ExecutionPlanUtilities.read_record(departments_0_table_location, departments_0_csv_files, departments_0_file_index, departments_0_pos)
      record_0 = record_1 + "," + departments_0_line.chomp

      departments_0_joined_flag = false
      if employees_1_line.split(",")[employees_1_department_id_index].strip.to_i==departments_0_line.split(",")[departments_0_department_id_index].strip.to_i

        departments_0_joined_flag = true

        records <<  record_0

      end


    end


    records << employees_1_line.chomp + ("," * 3) if !employees_1_joined_flag and join_type.start_with?("LEFT")

    records << (("," * 3) + employees_1_line.chomp) if !employees_1_joined_flag and join_type.start_with?("RIGHT")

  end

  aggregation_columns = [{:function=>:MAX,:index=>ExecutionPlanUtilities::get_column_index("employees", "salary")},{:function=>:SUM,:index=>ExecutionPlanUtilities::get_column_index("employees", "salary")},{:function=>:MAX,:index=>ExecutionPlanUtilities::get_column_index("employees", "department_id")},]

  grouping_columns << ExecutionPlanUtilities::get_column_index("departments", "department_name") + 4


  if aggregation_columns.empty?

    selection_columns << 0 + ExecutionPlanUtilities::get_column_index("max(employees", "salary)")
    selection_columns << 0 + ExecutionPlanUtilities::get_column_index("sum(employees", "salary)")
    selection_columns << 0 + ExecutionPlanUtilities::get_column_index("max(employees", "department_id)")

  end
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