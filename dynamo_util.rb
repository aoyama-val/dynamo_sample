class DynamoUtil
  def initialize(*args, **kwargs)
    if kwargs[:use_ssl] == false
      kwargs[:endpoint] = "http://dynamodb.ap-northeast-1.amazonaws.com"
    end
    kwargs.delete(:use_ssl)
    @ddb = Aws::DynamoDB::Client.new(**kwargs)
    @res = Aws::DynamoDB::Resource.new(client: @ddb)
  end

  def get_table_status(table_name)
    resp = @ddb.describe_table({
      table_name: table_name
    })
    return resp.table.table_status
  end

  def get_table_capacity(table_name)
    throughput = @res.table(table_name).provisioned_throughput
    return {
      read: throughput.read_capacity_units,
      write: throughput.write_capacity_units,
    }
  end

  def update_capacity(table_name, read, write)
    capacity = get_table_capacity(table_name)
    if capacity[:read] == read && capacity[:write] == write
      puts "No change"
      return
    end

    @ddb.update_table({
      table_name: table_name,
      provisioned_throughput: {
        read_capacity_units: read, 
        write_capacity_units: write, 
      }, 
    })

    puts "Updated capacity"

    while get_table_status(table_name) != "ACTIVE"
      puts "Waiting for the table to be active..."
      sleep(1)
    end
  end

  def batch_write_with_retry(table_name, items, max_retry=10)
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
        resp = @ddb.batch_write_item(
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

  def query_all(table_name, index_name, key_condition_expression, expression_attribute_values)
    total_count = 0
    last_evaluated_key = nil
    while true
      params = {
        table_name: table_name,
        index_name: index_name,
        key_condition_expression: key_condition_expression,
        expression_attribute_values: expression_attribute_values,
      }
      if last_evaluated_key
        params[:exclusive_start_key] = last_evaluated_key
      end

      resp = @ddb.query(params)
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
  end
end
