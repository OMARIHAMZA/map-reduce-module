gem 'parallel'
require 'parallel'
require 'stringio'

module MapReduce

  DATA_TYPES_FILE_PATH = "C:\\Users\\ASUS\\Documents\\GitHub\\pl-sql-compiler\\output.json"
  MAPPER_SUFFIX = "mapper_result_"
  SHUFFLER_SUFFIX = "shuffler_result_"

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

        where_condition.gsub! /#{attr.to_s}/i, "attributes[#{index}]" + conversion
      end

      @where_condition = where_condition


    end

    private :get_resources

    def map

      result_file = File.open(MapReduce::MAPPER_SUFFIX + @file_name, "w")


      File.foreach(@file_name) do |line|


        attributes = line.split(@field_terminator)


        result_file.write line.gsub(@field_terminator, ',') if eval(@where_condition)

      end

      result_file.close

      [@data_types_order, @data_members]

    end
  end

  class Shuffler


    def initialize(file_name, key, data_types_order)

      @file_name = file_name
      @key = key
      @data_types_order = data_types_order

    end

    def shuffle

      value_index = @data_types_order.index(@key.upcase)

      shuffling_threads = []

      File.foreach(@file_name) do |line|

        shuffling_threads << Thread.new {

          attributes = line.split(",")

          line_key = attributes[value_index]

          current_file_name = SHUFFLER_SUFFIX + line_key

          f = File.new(current_file_name, "a")

          f.write(line)

          f.close
        }

      end

      ThreadsWait.all_waits(shuffling_threads)

    end

    def memory_shuffle

      puts @key

      value_index = @data_types_order.index(@key.upcase)

      file_content = File.read(@file_name)

      files = Hash.new("")

      file_content.each_line do |line|

        attributes = line.split(",")

        line_key = attributes[value_index]

        current_file_name = SHUFFLER_SUFFIX + line_key

        files[current_file_name] = files[current_file_name] + line

      end

      file_threads = []

      files.each_pair do |key, value|

        file_threads << Thread.new {

          current_file = File.open(key, "w")

          current_file.write value

          current_file.close

        }

      end

      ThreadsWait.all_waits(file_threads)

    end

  end


  class Reducer

    def initialize(file_name, key, data_types_order, data_members)

      @file_name = file_name
      @key = key
      @data_types_order = data_types_order
      @data_members = data_members

    end

    def reduce


      summarize_result = {
          :sum => nil,
          :max => nil,
          :min => nil,
          :mean => nil,
          :q1 => nil,
          :q3 => nil,
          :mode => nil,
          :count => nil
      }

      value_index = @data_types_order.index(@key.upcase)
      value_data_type = nil

      @data_members.each_with_index do |value, index|

        if index == value_index

          value_data_type = value["type"]

        end

      end


      values = []

      File.foreach(@file_name) do |line|

        attributes = line.split(",")

        line_key = attributes[value_index]

        values << case value_data_type
                  when "STRING";
                    line_key
                  when "INT";
                    line_key.to_i
                  when "FLOAT";
                    line_key.to_f
                  end

      end


      summarize_result[:count] = values.count

      if value_data_type != "STRING"

        values.sort!
        puts "Values: " + values.to_s
        summarize_result[:sum] = values.sum
        summarize_result[:max] = values.last
        summarize_result[:min] = values.first
        summarize_result[:mean] = summarize_result[:sum] / values.count
        summarize_result[:mode] = values[(values.size / 2).floor]
        summarize_result[:q1] = values[(values.size / 4).floor]
        summarize_result[:q3] = values[((values.count + values.size / 2) / 2).floor]

      end

      summarize_result

    end


  end


end
