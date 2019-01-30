require "aws-sdk-dynamodb"
require "pp"

ddb = Aws::DynamoDB::Client.new(
  region: "ap-northeast-1",
  http_wire_trace: true,
)

# https://docs.aws.amazon.com/sdk-for-ruby/v3/api/Aws/DynamoDB/Client.html#query-instance_method
res = ddb.query({
  table_name: "aoyamaFacilities",
  key_condition_expression: "corpCode = :v1",
  expression_attribute_values: {
    ":v1" => "tsuruga",
  },
  scan_index_forward: false, # trueなら昇順、falseなら降順
  return_consumed_capacity: "TOTAL",  # 消費したキャパシティを返す
})

print "\e[0;31m"
res.items.each do |item|
  pp item
end
p res.consumed_capacity
puts
puts "#{res.items.length} items"
print "\e[0m"
