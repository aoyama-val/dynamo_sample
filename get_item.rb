require 'aws-sdk-core'
require "pp"

ddb = Aws::DynamoDB::Client.new(
  region: 'ap-northeast-1',
  access_key_id: ENV["ACCESS_KEY_ID"],
  secret_access_key: ENV["SECRET_ACCESS_KEY"],
)

res = ddb.get_item(
  table_name: 'skybrain-prod-line-friends',
  key: {id: "hoge"}
)

pp res.item

