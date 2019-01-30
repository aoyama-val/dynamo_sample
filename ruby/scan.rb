require 'aws-sdk-core'
require "pp"

table_name = "development_Transport"

ddb = Aws::DynamoDB::Client.new(
  region: 'ap-northeast-1',
  #http_wire_trace: true,
)

puts ENV["AWS_ACCESS_KEY_ID"]
puts ENV["SECRET_AWS_ACCESS_KEY"]

result = ddb.scan(
  table_name: table_name,
  select: "COUNT",
  #select: "ALL_ATTRIBUTES",
)

p result
#puts "Records: " + "#{result.items.count}"
