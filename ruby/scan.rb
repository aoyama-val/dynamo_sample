require 'aws-sdk'
require "pp"

table_name = ARGV[0] || (exit 1)

ddb = Aws::DynamoDB::Client.new(
  region: 'ap-northeast-1',
  #http_wire_trace: true,
)

puts ENV["AWS_ACCESS_KEY_ID"]
puts ENV["SECRET_AWS_ACCESS_KEY"]

result = ddb.scan(
  table_name: table_name,
  #select: "COUNT",
  #select: "ALL_ATTRIBUTES",
)

result.items.each do |item|
  pp item
end
puts "Record count: #{result.items.count}"
