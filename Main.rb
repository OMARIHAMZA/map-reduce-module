require_relative 'map_reduce'
require 'json'


MapReduce::Mapper.new("EMPLOYEES",
                      "employees.csv",
                      "salary > 1000").map


