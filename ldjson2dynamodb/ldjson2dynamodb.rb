################################################################################
#   ldjson (Line-delimited JSON, NDJSON, JSONL)ファイルから
#   DynamoDBのテーブルにインポートする
#
#   ・エラーがあった場合は即座に終了する
#   ・キャパシティを上げてskipを指定して再実行すれば、途中から再開できる
#
################################################################################

require "aws-sdk"

class Tsv2DynamoDB
  def initialize(*args, **kwargs)
    @ddb = Aws::DynamoDB::Client.new(**kwargs)
  end

  def import(table_name, filename, skip)
    @filename = filename

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
    resp = @ddb.batch_write_item(
      request_items: { table_name => requests },
      return_consumed_capacity: "TOTAL",
      return_item_collection_metrics: "SIZE"
    )
    unprocessed = resp.unprocessed_items[table_name]
    puts "#{Time.now.strftime('%H:%M:%S')}   #{linenum - items.length + 1} - #{linenum}   consumed_capacity: #{resp.consumed_capacity[0].capacity_units}   #{unprocessed ? 'unprocessed: ' + unprocessed.length.to_s : ''}"
    if unprocessed
      puts "To retry:"
      puts "  ruby #{$0} #{table_name} #{@filename} #{linenum - items.length}"
      exit 1
    end
  end
end

if __FILE__ == $0
  def main
    if ARGV.length < 2
      puts "Usage: ruby #{$0} TABLE_NAME FILENAME SKIP"
      puts "Example: ruby #{$0} my_table sample.ldjson 0"
      exit 1
    end

    table_name = ARGV[0]
    filename = ARGV[1]
    skip = (ARGV[2] || 0).to_i

    puts "Start"

    started = Time.now

    td = Tsv2DynamoDB.new

    begin
      td.import(table_name, filename, skip)
    rescue Interrupt
      puts "Interrupted"
    end

    finished = Time.now
    puts "Elapsed: #{finished - started} sec"
  end

  main
end
