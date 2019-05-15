gem 'parallel'
require 'json'
require 'parallel'
require 'stringio'
require_relative 'enumerable'

module MapReduce

  DATA_TYPES_FILE_PATH = "C:\\Users\\ASUS\\Documents\\GitHub\\pl-sql-compiler\\output.json"
  MAPPER_RESULT_FILE = "mapper_result.txt"
  REDUCER_RESULT_FILE = "reducer_result.txt"
  SHUFFLER_RESULT_FILE = "shuffler_result.json"

  class Mapper

    def initialize

    end

    def mapper_without_shuffling(records, aggregation_columns)

      result_file = File.open(MapReduce::MAPPER_RESULT_FILE, "w")

      aggregation_columns.each do |entry|

        result_file.puts records.map {|record| record.split(",")[entry[:index]]}.join(",")

      end

      result_file.close

      MapReduce::MAPPER_RESULT_FILE

    end

    def mapper_with_shuffling(records, grouping_columns, aggregation_columns)

      result_file = File.open(MapReduce::MAPPER_RESULT_FILE, "w")

      records.each do |record|

        attributes = record.split(",")

        result_file.puts "#{attributes.values_at(*grouping_columns).join(",")}:#{attributes.values_at(*aggregation_columns.map {|column| column[:index].to_i}).join(",")}"

      end

      result_file.close

      MapReduce::MAPPER_RESULT_FILE

    end

  end

  class Shuffler


    def initialize(input_file_name)

      @input_file_name = input_file_name

    end

    def shuffle

      result_file = File.open(MapReduce::SHUFFLER_RESULT_FILE, "w")

      result_hash = {}

      File.foreach(@input_file_name) do |line|

        key_values = line.split(":")
        key = key_values[0]
        values = key_values[1]

        result_hash[key] = [] unless result_hash[key]

        values.split(",").each_with_index do |value, index|

          result_hash[key][index] = [] unless result_hash[key][index]

          result_hash[key][index] << value.chomp

        end

      end

      result_file.puts JSON.generate(result_hash)

      result_file.close

      MapReduce::SHUFFLER_RESULT_FILE

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

    def reduce_with_shuffle


      input_hash = JSON.parse(File.read(@input_file))

      output_file = File.open(MapReduce::REDUCER_RESULT_FILE, "w")

      input_hash.each_key do |key|

        input_hash[key].each_with_index do |array_value, index|


          output_file.puts key + " : " +  case @aggregation_columns[index][:function]

                           when :SUM
                             array_value.map(&:to_i).sum.to_s

                           when :MAX
                             array_value.map(&:to_i).max.to_s

                           when :MIN
                             array_value.map(&:to_i).min.to_s

                           when :AVG
                             array_value.map(&:to_i).avg.to_s

                           when :STDEV
                             array_value.map(&:to_i).stdev.to_s

                           when :VARIANCE
                             array_value.map(&:to_i).variance.to_s

                           end

        end

      end

      output_file.close

      MapReduce::REDUCER_RESULT_FILE

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
