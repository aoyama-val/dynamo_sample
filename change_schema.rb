# スキーマ変更のサンプル

require "pp"
require 'aws-sdk-core'

ddb = Aws::DynamoDB::Client.new(
  region: 'ap-northeast-1',
  access_key_id: ENV["ACCESS_KEY_ID"],
  secret_access_key: ENV["SECRET_ACCESS_KEY"],
)

# 全件取得
res = ddb.scan({
  table_name: "skybrain-prod-line-friends"
})

res.items.each do |item|
  puts item["display_name"]
  # キーを削除することにより、uuidのカラムを削除
  item.delete("uuid")
  ddb.put_item(
    table_name: 'skybrain-prod-line-friends',
    item: item
  )
end
