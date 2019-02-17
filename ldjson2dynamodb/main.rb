require_relative "./lib/ldjson2dynamodb.rb"

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
