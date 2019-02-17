require_relative "./lib/tsv2dynamodb.rb"

def main
  if ARGV.length != 3
    puts "Usage: ruby #{$0} TABLE_NAME COLUMN_MAP FILENAME"
    puts "Example: ruby #{$0} my_table id,name,value hoge.tsv"
    exit 1
  end

  table_name = ARGV[0]
  column_map = ARGV[1]
  filename = ARGV[2]
  error_filename = "#{Time.now.strftime('%Y%m%d_%H%M%S')}.ldjson"

  puts "Start"
  puts "error_filename: #{error_filename}"

  started = Time.now

  td = Tsv2DynamoDB.new

  begin
    td.import(table_name, column_map.split(","), filename, error_filename)
  rescue Interrupt
    puts "Interrupted"
  end

  finished = Time.now
  puts "Elapsed: #{finished - started} sec"
  puts "error_filename: #{error_filename}"
end

main
