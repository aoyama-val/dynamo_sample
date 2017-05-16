require 'aws-sdk-core'
require 'pp'
require 'json'

ddb = Aws::DynamoDB::Client.new(
  region: 'ap-northeast-1',
  access_key_id: ENV["AWS_ACCESS_KEY_ID"],
  secret_access_key: ENV["AWS_SECRET_ACCESS_KEY"],
)

counts = {
  "aoyama": 5,
}

# インポートするアイテム
items = []
  items << {
    put_request: {
      item:{
        "id" => 2,
        "question" => JSON.pretty_generate(counts),
      }
    }
  }

# 件数    :  ループ回数
#  0      :  0
#  1 - 25 :  1
# 26 - 50 :  2
((items.size - 1) / 25 + 1).times do |i|
  ary = items[25 * i, 25]
  resp = ddb.batch_write_item({
    request_items: {
      "val00362_1" => ary
    }
  })
  sleep(0.1)
  if not resp.unprocessed_items.empty?
    puts "Error: i = #{i}: unprocessed_items is not empty: リトライしてください"
  end
  if i % 40 == 0
    puts "done: i = #{i}"
  end
end

puts "Finish"
