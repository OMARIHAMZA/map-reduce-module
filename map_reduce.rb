gem 'parallel'
require 'parallel'
require 'stringio'
require_relative 'enumerable'

module MapReduce

  DATA_TYPES_FILE_PATH = "C:\\Users\\ASUS\\Documents\\GitHub\\pl-sql-compiler\\output.json"
  MAPPER_RESULT_FILE = "mapper_result.txt"
  REDUCER_RESULT_FILE = "reducer_result.txt"
  SHUFFLER_SUFFIX = "shuffler_result_"

  class Mapper

    def initialize

    end

    def map(records, aggregation_columns)

      result_file = File.open(MapReduce::MAPPER_RESULT_FILE, "w")

      aggregation_columns.each do |entry|

        result_file.puts records.map {|record| record.split(",")[entry[:index]]}.join(",")

      end

      result_file.close

      MapReduce::MAPPER_RESULT_FILE

    end

  end

  class Shuffler


    def initialize


    end

    def shuffle


    end

    def memory_shuffle

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

          current_file = File.open(key.chomp, "w")

          current_file.write value

          current_file.close

        }

      end

      ThreadsWait.all_waits(file_threads)

    end

  end


  class Reducer

    def initialize(input_file, aggregation_columns)

      @input_file = input_file
      @aggregation_columns = aggregation_columns

    end

    def reduce_without_shuffle

      line_number = 0

      result_file = File.open(MapReduce::REDUCER_RESULT_FILE, "w")

      File.foreach(@input_file) do |line|



        result_file.puts case @aggregation_columns[line_number][:function]

        when :SUM
          line.split(",").map(&:to_i).sum

        when :MAX
          line.split(",").map(&:to_i).max

        when :MIN
          line.split(",").map(&:to_i).min

        when :AVG
          line.split(",").map(&:to_i).avg

        when :STDEV
          line.split(",").map(&:to_i).stdev

        when :VARIANCE
          line.split(",").map(&:to_i).variance

        end


        line_number += 1

      end

      result_file.close

    end

    def reduce

=begin

      summarize_result = {

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
        summarize_result[:sum] = values.sum
        summarize_result[:max] = values.last
        summarize_result[:min] = values.first
        summarize_result[:avg] = summarize_result[:sum] / values.count
        summarize_result[:mode] = values[(values.size / 2).floor]
        summarize_result[:q1] = values[(values.size / 4).floor]
        summarize_result[:q3] = values[((values.count + values.size / 2) / 2).floor]

      end

      summarize_result
=end

    end


  end


end
