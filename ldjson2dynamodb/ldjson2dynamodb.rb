################################################################################
#   ldjson (Line-delimited JSON, NDJSON, JSONL)ファイルから
#   DynamoDBのテーブルにインポートする
#
#   ・再試行不能なエラーがあった場合は即座に終了する
#   ・キャパシティ不足の場合はスリープしつつ無限に再試行する
#
################################################################################

require "aws-sdk"

class Ldjson2DynamoDB
  attr_accessor :capacity_error_count

  def initialize(*args, **kwargs)
    @ddb = Aws::DynamoDB::Client.new(**kwargs)
  end

  def import(table_name, filename, skip)
    @filename = filename
    @capacity_error_count = 0

    items = []
    linenum = 0
    IO.foreach(filename, chomp: true) do |line|
      linenum += 1
      if linenum <= skip
        next
      end
      items << JSON.parse(line)
      if items.length == 25
        batch_write(table_name, items, linenum)
        items = []
      end
    end
    if items.length != 0
      batch_write(table_name, items, linenum)
    end
  end

private

  def batch_write(table_name, items, linenum)
    requests = items.map {|item|
      {
        put_request: {
          item: item
        }
      }
    }
    loop do
      resp = @ddb.batch_write_item(
        request_items: { table_name => requests },
        return_consumed_capacity: "TOTAL",
        return_item_collection_metrics: "SIZE"
      )
      unprocessed = resp.unprocessed_items[table_name]
      puts "#{Time.now.strftime('%H:%M:%S')}   #{linenum - items.length + 1} - #{linenum}   consumed_capacity: #{resp.consumed_capacity[0].capacity_units}   #{unprocessed ? 'unprocessed: ' + unprocessed.length.to_s : ''}"
      if unprocessed
        puts "書き込みキャパシティが不足しています！"
        @capacity_error_count += 1
        sleep 1
      else
        return
      end
    end
  end
end

if __FILE__ == $0
  def main
    if ARGV.length < 2
      puts "Usage: ruby #{$0} TABLE_NAME FILENAME [SKIP]"
      puts "Example: ruby #{$0} my_table sample.ldjson 100"
      puts "  SKIPを指定すると、入力ファイルの最初のSKIP行目をスキップします"
      exit 1
    end

    table_name = ARGV[0]
    filename = ARGV[1]
    skip = (ARGV[2] || 0).to_i

    puts "Start"

    started = Time.now

    td = Ldjson2DynamoDB.new

    begin
      td.import(table_name, filename, skip)
    rescue Interrupt
      puts "Interrupted"
    end

    finished = Time.now
    puts "Elapsed: #{finished - started} sec"
    puts "書き込みキャパシティが不足した回数: #{td.capacity_error_count}"
  end

  main
end
