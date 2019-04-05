module MapReduce

  DATA_TYPES_FILE_PATH = "C:\\Users\\ASUS\\Documents\\GitHub\\pl-sql-compiler\\output.json"
  MAPPER_SUFFIX = "mapper_result_"

  class Mapper


    def initialize(table_name, file_name, where_condition = "true")

      @table_name = table_name
      @file_name = file_name
      @where_condition = ""
      get_resources(where_condition)

    end

    def get_resources (where_condition)
      @json_array = JSON.parse File.read(MapReduce::DATA_TYPES_FILE_PATH)
      @json_array.map do |entry|

        if entry["name"].casecmp?(@table_name)
          @field_terminator = entry["field_terminator"]
          @data_members = entry["members"]
          break
        end

      end

      @data_types_order = []

      @data_members.each do |entry|
        @data_types_order << entry["name"]
      end

      @data_types_order.each_with_index do |attr, index|
        column_data_type = @data_members[index]["type"]
        conversion = column_data_type.casecmp?("int") ? ".to_i" : ""
        +(column_data_type.casecmp?("float") ? ".to_f" : "")

        where_condition.gsub! /#{attr.to_s}/i, "attributes[#{index}]" + conversion;
      end

      puts where_condition
      @where_condition = where_condition
    end

    private :get_resources

    def map

      result_file = File.open(MapReduce::MAPPER_SUFFIX + @file_name, "w")


      File.foreach(@file_name) do |line|


        attributes = line.split(@field_terminator).each(&:strip!)


        result_file.write line if eval(@where_condition)

      end

      result_file.close

    end

    class Shuffler

    end

    class Reducer

    end

  end

end

=begin
        [column, condition, value]
         age, <, 20

        if condition == "<"
          column.value < value
        end


=end

