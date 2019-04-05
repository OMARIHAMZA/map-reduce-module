module MapReduce

  MAPPER_SUFFIX = "mapper_result_"

  class Mapper


    def initialize(file_name, data_types_order, field_terminator = ",", where_condition = "")

      @file_name = file_name
      @data_types_order = data_types_order
      @field_terminator = field_terminator
      @where_condition = where_condition

    end

    def map

      result_file = File.open(MapReduce::MAPPER_SUFFIX + @file_name, "w")

      File.foreach(@file_name) do |line|

        result_file.write line

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

