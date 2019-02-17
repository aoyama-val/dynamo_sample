################################################################################
#   TSVファイルからDynamoDBのテーブルにインポートする
#
#   ・エラーがあった場合はリトライせず、エラーファイルに書き出す
################################################################################

require "aws-sdk"

class Tsv2DynamoDB
  def initialize(*args, **kwargs)
    @ddb = Aws::DynamoDB::Client.new(**kwargs)
  end

  def import(table_name, column_map, filename, error_filename)
    open(error_filename, "w") do |error_file|
      items = []
      linenum = 0
      IO.foreach(filename, chomp: true) do |line|
        linenum += 1
        a = line.split("\t")
        items << column_map.map.with_index {|column_name, i| [column_name, a[i]]}.to_h
        if items.length == 25
          batch_write(table_name, items, linenum, error_file)
          items = []
        end
      end
      if items.length != 0
        batch_write(table_name, items, linenum, error_file)
      end
    end
  end

private

  def batch_write(table_name, items, linenum, error_file)
    requests = items.map {|item|
      {
        put_request: {
          item: item
        }
      }
    }
    resp = @ddb.batch_write_item(
      request_items: { table_name => requests },
      return_consumed_capacity: "TOTAL",
      return_item_collection_metrics: "SIZE"
    )
    unprocessed = resp.unprocessed_items[table_name]
    puts "#{Time.now.strftime('%H:%M:%S')}   #{linenum - items.length + 1} - #{linenum}   consumed_capacity: #{resp.consumed_capacity[0].capacity_units}   #{unprocessed ? 'unprocessed: ' + unprocessed.length.to_s : ''}"
    if unprocessed
      unprocessed.each do |u|
        error_file.puts JSON.dump(u.put_request.item)
        error_file.flush
      end
    end
  rescue => ex
    p ex
    puts ex.message
    puts ex.backtrace
    error_file.puts line
    exit
  end
end
