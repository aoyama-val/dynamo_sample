require 'aws-sdk-core'
require "pp"

ddb = Aws::DynamoDB::Client.new(
  region: 'ap-northeast-1',
  access_key_id: ENV["AWS_ACCESS_KEY_ID"],
  secret_access_key: ENV["SECRET_AWS_ACCESS_KEY"],
  http_wire_trace: true,
)

puts ENV["AWS_ACCESS_KEY_ID"]
puts ENV["SECRET_AWS_ACCESS_KEY"]

res = ddb.query({
  table_name: "skybrain-prod-line-friends", 
  index_name: "line_bot_code-index",
  key_condition_expression: "line_bot_code = :v1", 
  expression_attribute_values: {
    ":v1" => "hinomaru_dev", 
  }, 
  #projection_expression: "SongTitle", 
})

#pp res

res.items.each do |item|
  pp item
end

puts
puts "#{res.items.length} items"
