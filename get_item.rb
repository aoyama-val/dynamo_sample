require 'aws-sdk-core'
require "pp"

ddb = Aws::DynamoDB::Client.new(
  region: 'ap-northeast-1',
  access_key_id: ENV["AWS_ACCESS_KEY_ID"],
  secret_access_key: ENV["AWS_SECRET_ACCESS_KEY"],
)

started = Time.now
res = ddb.get_item(
  table_name: 'aoyama1',
  key: {id: "hoge2"},
  consistent_read: true,
)

ended = Time.now

pp res.item

puts ended - started

