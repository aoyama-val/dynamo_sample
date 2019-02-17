#vim: set smartindent ts=2 sts=2 sw=2 et:

# aoyama8
# id   HASH KEY
# hoge RANGE KEY

require "aws-sdk"
require "byebug"

require_relative "./dynamo_util.rb"

ENV["http_proxy"] = "http://localhost:8081"

util = DynamoUtil.new(use_ssl: false)
#p util.get_table_capacity("aoyama1")
util.update_capacity("aoyama1", 1, 1)
exit

case ARGV[0]
when "insert"
  1000.times do |j|
    puts "j = #{j}"
    items = (0...1000).map {|i|
      {
        id: "value#{(1000 * j + i) % 10}",
        hoge: rand(),
      }
    }
    batch_write_with_retry(ddb, table_name, items)
  end
when "scan"
  total_count = 0
  last_evaluated_key = nil
  while true
    scan_params = {
      table_name: table_name,
    }
    if last_evaluated_key
      scan_params[:exclusive_start_key] = last_evaluated_key
    end

    resp = ddb.scan(scan_params)
    resp.items.each do |item|
      #puts item["id"]
    end
    total_count += resp.items.length
    puts "#{resp.items.length} items"
    puts "total = #{total_count}"
    p resp.last_evaluated_key
    if resp.last_evaluated_key.nil?
      break
    else
      last_evaluated_key = resp.last_evaluated_key
    end
  end
when "query"
  query_all(ddb, table_name, nil, "id = :id AND hoge < :v1", { ":id": "value0", ":v1": 0.0314 })
when "delete"
  resp = ddb.scan({
    table_name: table_name
  })
  resp.items.each do |item|
    puts "deleting #{item["id"]}"
    ddb.delete_item({
      key: {id: item["id"]},
      table_name: table_name
    })
  end
else
  puts "Usage: #{$0} insert|scan|delete"
end
