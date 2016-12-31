#!/usr/bin/env ruby
BASE_PATH = File.expand_path(File.join(File.dirname(__FILE__), ".."))
$LOAD_PATH.unshift(File.join(BASE_PATH, 'lib'))

require 'inetdata'
require 'optparse'

options = {}
OptionParser.new do |opts|
  opts.banner = "Usage: normalize [options]"

  opts.on("-l", "--list-sources", "List available sources") do |opt|
    options[:list_sources] = true
  end
  opts.on("-s", "--sources [sources]", "Comma-separated list of sources to normalize") do |opt|
    options[:selected_sources] = opt.split(/,\s+/).uniq.map{|x| x.downcase}
  end
end.parse!

config = InetData::Config.new
logger = InetData::Logger.new(config, 'normalize')

allowed_sources = (InetData::Source.constants - [:Base]).map{|c| InetData::Source.const_get(c) }
sources = []

allowed_sources.each do |sname|
  s = sname.new(config)
  if ! s.available?
    logger.log("Warning: Source #{s.name} is disabled due to configuration")
    next
  end

  if s.manual? && (options[:selected_sources].nil? || ! options[:selected_sources].include?(s.name))
    logger.log("Warning: Source #{s.name} must be specified manually")
  end

  sources << s

end

if options[:list_sources]
  $stderr.puts "Available Sources: "
  sources.each do |s|
    $stderr.puts " * #{s.name}"
  end
  exit(1)
end

if options[:selected_sources]
  sources = sources.select do |s|
    options[:selected_sources].include?(s.name)
  end
end

logger.log("Normalize initiated with sources: #{sources.map{|s| s.name}.join(", ")}")

sources.each do |s|
  begin
    s.normalize
  rescue ::Exception
    logger.log("Error: Source #{s.name} threw an exception: #{$!.class} #{$!} #{$!.backtrace}")
  end
end

logger.log("Normalize completed with sources: #{sources.map{|s| s.name}.join(", ")}")
