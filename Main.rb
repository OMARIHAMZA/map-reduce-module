require_relative 'map_reduce'
require 'json'
require 'time'


start =  Time.now

MapReduce::Mapper.new("EMPLOYEES",
                      "employees.csv",
                      "EMPLOYEE_NAME == \"Hamza\"").map

puts Time.now - start