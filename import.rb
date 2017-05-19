require "aws-sdk"
require "byebug"

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

ddb = Aws::DynamoDB::Client.new

table_name = "aoyama4"

case ARGV[0]
when "insert"
  items = (1..1000).map {|i|
    {
      id: "value#{i}"
    }
  }
  batch_write_with_retry(ddb, table_name, items)
when "scan"
  resp = ddb.scan({
    table_name: "aoyama4",
  })
  resp.items.each do |item|
    puts item["id"]
  end
  puts "#{resp.items.length} items"
when "delete"
  resp = ddb.scan({
    table_name: "aoyama4",
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
