################################################################################
#   ldjson (Line-delimited JSON, NDJSON, JSONL)ファイルから
#   DynamoDBのテーブルにインポートする
#
#   ・エラーがあった場合はリトライせず、エラーファイルに書き出す
#   ・この方式の良いところは、キャパシティ不足でエラーが起きてもエラーファイルを
#   　食わせて再実行すれば全てのデータを漏れなくインポートできるところ。
#
################################################################################

require "aws-sdk"

class Tsv2DynamoDB
  attr_accessor :error_count

  def initialize(*args, **kwargs)
    @ddb = Aws::DynamoDB::Client.new(**kwargs)
  end

  def import(table_name, filename, error_filename)
    @error_count = 0

    open(error_filename, "w") do |error_file|
      items = []
      linenum = 0
      IO.foreach(filename, chomp: true) do |line|
        linenum += 1
        items << JSON.parse(line)
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
        @error_count += 1
      end
    end
  end
end

if __FILE__ == $0
  def main
    if ARGV.length != 2
      puts "Usage: ruby #{$0} TABLE_NAME FILENAME"
      puts "Example: ruby #{$0} my_table sample.ldjson"
      exit 1
    end

    table_name = ARGV[0]
    filename = ARGV[1]
    error_filename = "error_#{Time.now.strftime('%Y%m%d_%H%M%S')}.ldjson"

    puts "Start"
    puts "error_filename: #{error_filename}"

    started = Time.now

    td = Tsv2DynamoDB.new

    begin
      td.import(table_name, filename, error_filename)
    rescue Interrupt
      puts "Interrupted"
    end

    finished = Time.now
    puts "Elapsed: #{finished - started} sec"
    puts "error_filename: #{error_filename}"
  end

  main
end
