#vim: set smartindent ts=2 sts=2 sw=2 et:

require "aws-sdk"
require "byebug"

#ENV["http_proxy"] = "http://localhost:8081"

def batch_write_with_retry(ddb, table_name, items, max_retry=10)
  requests = items.map {|item|
    {
      put_request: {
        item: item
      }
    }
  }

  unprocessed = []
  start_time = Time.now
  i = 0
  requests.each_slice(25) do |sub_requests|
    try_count = 1
    success = false
    unprocessed = sub_requests
    while try_count <= max_retry && unprocessed.count > 0
      resp = ddb.batch_write_item(
        request_items: { table_name => unprocessed },
        return_consumed_capacity: "INDEXES",
        return_item_collection_metrics: "SIZE"
      )
      if resp.unprocessed_items.count == 0
        success = true
        break
      else
        puts "unprocessed_items.count = #{resp.unprocessed_items[table_name].count}"
        puts "retrying #{try_count}"
        unprocessed = resp.unprocessed_items[table_name]
        try_count += 1
        sleep(1.5 ** (try_count - 1))
      end
    end
    start_index = 25 * i + 1
    end_index   = 25 * (i + 1)
    if success
      puts "#{Time.now - start_time}"
      puts "batch #{i} (#{start_index} - #{end_index}) success"
      #puts "consumed_capacity = #{resp.consumed_capacity[0].capacity_units}"
    else
      puts "batch #{i} (#{start_index} - #{end_index}) failure"
      return
    end
    i += 1
  end
end

ddb = Aws::DynamoDB::Client.new(endpoint: "http://dynamodb.ap-northeast-1.amazonaws.com")

table_name = "aoyama2"

case ARGV[0]
when "insert"
  1000.times do |j|
    items = (0...1000).map {|i|
      {
        id: "value#{1000 * j + i}",
        value1: "0123456789",
        value2: "0123456789",
        value3: "0123456789",
        value4: "0123456789",
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
    p resp.last_evaluated_key
    if resp.last_evaluated_key.nil?
      break
    else
      last_evaluated_key = resp.last_evaluated_key
    end
  end
  puts "total = #{total_count}"
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
  puts "Usage: #{$0} inset|scan|delete"
end
