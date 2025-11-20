# gem install sparql
# http://www.rubydoc.info/github/ruby-rdf/sparql/frames

require 'sparql/client'
require 'json'
require 'uri'

endpoint = "https://query.wikidata.org/sparql"
sparql = <<'SPARQL'.chop
SELECT ?org ?artsdataID ?url WHERE {
  ?org wdt:P31/wdt:P279* wd:Q7168296 .
  ?org wdt:P7627 ?artsdataID .
  ?org wdt:P856 ?url .
}
SPARQL

client = SPARQL::Client.new(
  endpoint,
  method: :get,
  headers: { 'User-Agent' => 'artsdata-crawler/3.3' }
)

rows = client.query(sparql)

def artifact_from_url(url)
  host = URI.parse(url).host rescue nil
  return nil unless host
  host = host.sub(/^www\./, '')
  host.gsub('.', '-')
end

data = rows.map do |row|
  url = row[:url].to_s

  {
    "org" => row[:org].to_s,
    "artsdataID" => row[:artsdataID].to_s,
    "url" => url,
    "artifact" => artifact_from_url(url)
  }
end

batch_size = 50
batches = data.each_slice(batch_size).to_a

batches.each_with_index do |batch, i|
  file_index = i + 1
  file_name = "wikidata_orgs_#{file_index}.json"
  File.write(file_name, JSON.pretty_generate(batch))
  puts "Saved #{batch.size} rows to #{file_name}"
end

puts "Total records: #{data.size}"
puts "Total batch files created: #{batches.size}"
