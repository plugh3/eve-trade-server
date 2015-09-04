#require 'curb'

require 'stringio'
require 'typhoeus'
require 'mysql2'
require 'json'

#require 'net/http'
#require "open-uri"
#require "openssl"


$timers = Hash.new(0)
$counters = Hash.new(0)

SYS_AMARR = 	30002187
SYS_JITA = 		30000142
SYS_DODIXIE = 30002659

STN_AMARR =   60008494
STN_JITA =    60003760
STN_DODIXIE = 60011866

def sys2sn(x)
	lookup = {
	  SYS_AMARR => "Amarr", 
	  SYS_JITA => "Jita", 
	  SYS_DODIXIE => "Dodixie",
	}
	lookup[x.to_i]
end

### TODO: use DB lookups?
def stn_fn2id(x) 
	lookup = {
	  "Amarr VIII (Oris) - Emperor Family Academy" => STN_AMARR,
	  "Jita IV - Moon 4 - Caldari Navy Assembly Plant" => STN_JITA,
	  "Dodixie IX - Moon 20 - Federation Navy Assembly Plant" => STN_DODIXIE,
    }
	lookup[x] 
end

def stn_sn2id(x) 
	lookup = {
	  "Amarr" 	=> STN_AMARR,
	  "Jita" 	=> STN_JITA,
	  "Dodixie" => STN_DODIXIE,
	}
	lookup[x] 
end

def stn_id2sn(x) 
	lookup = {
	  STN_AMARR => "Amarr",
	  STN_JITA => "Jita",
	  STN_DODIXIE => "Dodixie",
	}
	lookup[x.to_i] 
end

def stn_is_hub(x) 
	lookup = {
	  "Amarr VIII (Oris) - Emperor Family Academy" => true,
	  "Jita IV - Moon 4 - Caldari Navy Assembly Plant" => true,
	  "Dodixie IX - Moon 20 - Federation Navy Assembly Plant" => true,
	}
	lookup[x] 
end

def url2route(url)
	sys_from = Regexp.new(/&fromt=([0-9]+)&/).match(url)[1]
	sys_to = Regexp.new(/&to=([0-9]+)&/).match(url)[1]
	sys2sn(sys_from) + "-to-" + sys2sn(sys_to)
end

def sz (h)
  size = 0
  h.each do |x|
    size += (JSON.to_json(x)).size
  end
  ("%.3f" % (size/1_000_000.0)) + "MB"
end

def assert(test, err)
  raise err unless test
end
def assert_eq(a, b, err)
  raise "#{err}: #{a}, #{b}" unless a = b
end
def ref2s(x)
  "\#<#{x.class}:" + ("0x%x" % x.object_id) + ">"
end



### mysql shared options
$mysql_opts = Hash.new
$mysql_opts[:host]     = "69.181.214.54"
#$mysql_opts[:host]     = "127.0.0.1"
$mysql_opts[:port]     = "3306"
$mysql_opts[:username] = "dev"
$mysql_opts[:password] = "BNxJYjXbYXQHAvFM"


### load_db_sdd() - import DB rows to object hash using provided field mappings
###   hash is dup-indexed by id and name
### args
###   db_name: name of DB in mysql
###   db_table: table in mysql
###   dst_type: class name of object to be instantiated and stored in "dst_store"
###   dst_store: datastore hash
###   fieldmap: dst_key=>src_key mappings, for each field to be imported
def load_db_sdd(db_name, db_table, dst_type, dst_store, field_map) 
  if dst_store.size == 0 then
    ### load SDD db
    opts = $mysql_opts.dup
    opts[:database] = db_name
    db = Mysql2::Client.new(opts)
    sql = "SELECT * FROM #{db_table}"
    results = db.query(sql)
    results.each do |src_row|
      id = src_row[field_map[:id]]
      name = src_row[field_map[:name]]

      dst_fields = Hash.new
      field_map.each { |k_dst, k_src| dst_fields[k_dst] = src_row[k_src] }
      klass = Object.const_get(dst_type)
      x = klass.new(dst_fields)  ### used for Item, Station, Region, etc.

      dst_store[id] = x
      dst_store[name] = x
    end

    puts "#{dst_type} DB sdd.#{db_table} (#{sz dst_store})"
  end
end



### Items class - wrapper class for item info 
### from (a) EVE static data dump DB (id, name, volume) and (b) CREST (href)
### public methods:
###   Items[id] => Item
### Item fields:
###   id
###   name
###   volume
###   href
###   data_sdd
###   data_crest
class Item
  attr_accessor   :id, :name, :volume, :href

  def initialize(fields={})
    fields.each { |k,v| instance_variable_set("@#{k}", v) }
  end
end
class Items
  @@_items = {}   ### duplicate indexed by id and name
  
  def self.[](id)
    load_db
    @@_items[id]
  end
  
  def self.add(item)
    load_db
    @@_items[item.id] = item
  end
  
  SDD_DB = "evesdd_galatea"
  SDD_TABLE = "invtypes"
  def self.load_db
    load_db_sdd(SDD_DB, SDD_TABLE, "Item", @@_items, {id:"typeID", name:"typeName", volume:"volume"})
  end
end
### test
id = 28272
vol = 10.0
name = "'Augmented' Hammerhead"
assert_eq(Items[id].name, name, "item DB: name info failed")
assert_eq(Items[id].volume, vol, "item DB: volume info failed")
assert_eq(Items[name].id, id, "item DB: name lookup failed")


### Stations class - wrapper class for station info 
###   pulls data from (a) EVE static data dump DB (id, name, region_id) and (b) CREST (href, region_href)
### public methods:
###   Stations[id] => Station object
### Station fields:
###   id
###   name
###   href
###   region_id
###   region_href
class Station
  attr_accessor  :id, :name, :href, :region_id, :region_href, :region_buy_href, :region_sell_href

  def initialize(fields={})
    fields.each { |k,v| instance_variable_set("@#{k}", v) }
  end
end
class Stations
  SDD_DB    = "evesdd_galatea"
  SDD_TABLE = "stastations"

  @@_stations = Hash.new  ### main datastore, indexed by id and name
  
  def self.[](id)
    load_db
    @@_stations[id]
  end
  
  def self.each
    load_db
    @@_stations.each { |k,v| yield(k,v) }
  end
  
  def self.load_db
    load_db_sdd(SDD_DB, SDD_TABLE, "Station", @@_stations, \
      {id:"stationID", name:"stationName", region_id:"regionID"})
  end
end
### test 
id = 60000004
name = "Muvolailen X - Moon 3 - CBD Corporation Storage"
region = 10000033
assert_eq(Stations[id].name, name, "station DB: name info failed")
assert_eq(Stations[id].region_id, region, "station DB: region info failed")
assert_eq(Stations[name].id, id, "station DB: name lookup failed")



### Regions: wrapper classes for region info
### usage
###   Regions[id].name
###   Regions[name].id
class Region
  attr_accessor   :id, :name, :href, :buy_href, :sell_href

  def initialize(fields={})
    fields.each { |k,v| instance_variable_set("@#{k}", v) }
  end
end
class Regions
  SDD_DB    = "evesdd_galatea"
  SDD_TABLE = "mapregions"

  @@_regions = Hash.new  ### main datastore, indexed by id and name
  
  def self.[](id)
    load_db
    @@_regions[id]
  end
  
  def self.each
    load_db
    @@_regions.each { |k,v| yield(k,v) }
  end
  
  def self.load_db
    load_db_sdd(SDD_DB, SDD_TABLE, "Region", @@_regions, {id:"regionID", name:"regionName"})
  end
end
### test 
id = 10000002
name = "The Forge"
assert_eq(Regions[id].name, name, "region DB: name info failed")
assert_eq(Regions[name].id, id, "region DB: name lookup failed")



### http-get alternate implementations
=begin
			# Version 3: open-uri (1.7-2.4 MB/s single, no multi)
			uri = URI.parse(url)
			http = Net::HTTP.new(uri.host, uri.port)
			http.use_ssl = true
			http.verify_mode = OpenSSL::SSL::VERIFY_NONE
			response = http.get(uri.request_uri)
			body = response.body
=end		

=begin
			# Version 4: curb (0.15 MB/s single, multi ?)
			c = Curl::Easy.new(url)
			c.ssl_verify_peer = false
			c.perform
			body = c.body_str
=end

# Version 5: typhoeus (1.2 MB/s single, 3.0 MB/s 200x multi)

### NOTE: use subclassing to add fields to existing structures
class Request2 < Typhoeus::Request
  attr_accessor :timer
end

class Source
	@@hydra = Typhoeus::Hydra.hydra

  def self.get(url, opts = {})
    urls = [url]
    responses = mget(urls, opts)
    responses[0]
  end
  
	def self.mget(urls, opts = {}, blocks = nil)
		requests = Array.new
    start = 0 # set below, declared here so we can use it in on_complete block
    stop = 0
    urls.each do |url|
      puts "Typhoeus.GET -- #{url}"
			# HTTP-GET Version 5: typhoeus (1.2 MB/s single, 3.0 MB/s 200x multi)
			#request = Typhoeus::Request.new(url, {ssl_verifypeer: false})
      opts[:ssl_verifypeer] ||= false

			request = Request2.new(url, opts)
			request.on_complete do |response|
			  if response.success?
          # hell yeah
          puts "completed    -- #{url}"
          # if callback is defined, call it
          blocks[url].call(response) if blocks
        elsif response.timed_out?
          # aw hell no
          puts("time out")
			  elsif response.code == 0
          # Could not get an http response, something's wrong.
          puts(response.return_message)
			  else
          # Received a non-successful http response.
          puts("=> HTTP request failed: " + response.code.to_s)
			  end
			end

			@@hydra.queue(request)
      $counters[:get] += 1
			requests << request
    end

    @@hydra.run 
 
    requests.map {|x| x.response}
  end
end


### Evecentral - wrapper class for pulling tradefinder results from eve-central.com
### public interface:
###   get_profitables() => array of "from_stn_id:to_stn_id:item_id" strings
class Evecentral < Source

  @url_base = "https://eve-central.com/home/tradefind_display.html"

  ### URLs for eve-central.com
  def self.url_evecentral(from, to)
    maxDelay = 48
    minProfit = 1000
    maxSpace = 8967
    maxRecords = 99999

    params = \
      "?set=1" + \
      "&fromt=#{from}" + \
      "&to=#{to}" + \
      "&qtype=Systems" + \
      "&age=#{maxDelay}" + \
      "&minprofit=#{minProfit}" + \
      "&size=#{maxSpace}" + \
      "&limit=#{maxRecords}" + \
      "&sort=sprofit" + \
      "&prefer_sec=0"

    url = @url_base + params
  end
  
  def self.urls_evecentral
    urls = []
    hubs = [SYS_AMARR, SYS_JITA, SYS_DODIXIE]
    hubs2 = hubs
    hubs.each do |from|
      hubs2.each do |to|
        if from==to then next end
        url = url_evecentral(from, to)
        urls << url
        #puts "#{url}\n\n"
      end
    end
    urls
  end

  ### regexp patterns for parsing eve-central.com pages
  REGEXP_PREAMBLE = \
'\<p\>Found \<i\>([0-9]+)\<\/i\> possible routes
.\<\/p\>
\<p\>Page [0-9]+\.
(\<a href=\".*\"\>Next page\<\/a\>
)?
\<hr \/\>
\<table border=0 width=90\%\>\n'

  ### begin string literal
  REGEXP_PROFITABLE_ORIG = \
'
\<tr\>
  \<td\>\<b\>From:\<\/b\> (?<askLocation>[^<]+?)\<\/td\>
  \<td\>\<b\>To:\<\/b\> (?<bidLocation>[^<]+?) \<\/td\>
  \<td\>\<b\>Jumps:\<\/b\> [0-9]+?\<\/td\>
\<\/tr\>
\<tr\>
  \<td\>\<b\>Type:\<\/b\> \<a href=\"(?<itemURLsuffix>quicklook.html\?typeid=(?<itemID>[0-9]+?))\"\>(?<itemName>[^<]+?)\<\/a\>\<\/td\>
  \<td\>\<b\>Selling:\<\/b\> (?<askPrice>[,.0-9]+?) ISK\<\/td\>
  \<td\>\<b\>Buying:\<\/b\> (?<bidPrice>[,.0-9]+?) ISK\<\/td\>
\<\/tr\>

\<tr\>
  \<td\>\<b\>Per-unit profit:\<\/b\> [,.0-9]+? ISK\<\/td\>
  \<td\>\<b\>Units tradeable:\<\/b\> [,0-9]+? \((?<askVolume>[,0-9]+?) -\&gt; (?<bidVolume>[,0-9]+?)\)\<\/td\>
  \<td\>\&nbsp;\<\/td\>
\<\/tr\>
\<tr\>
  \<td\>\<b\>\<i\>Potential profit\<\/i\>\<\/b\>: [,.0-9]+? ISK \<\/td\>
  \<td\>\<b\>\<i\>Profit per trip:\<\/i\>\<\/b\>: [,.0-9]+? ISK\<\/td\>
  \<td\>\<b\>\<i\>Profit per jump\<\/i\>\<\/b\>: [,.0-9]+?\<\/td\>
\<\/tr\>
\<tr\>\<td\>\&nbsp;\<\/td\>\<\/tr\>

'
### end string literal

  ### begin string literal
  REGEXP_PROFITABLE = \
'
\<tr\>
  \<td\>\<b\>From:\<\/b\> (?<askLocation>[^<]+)\<\/td\>
  \<td\>\<b\>To:\<\/b\> (?<bidLocation>[^<]+) \<\/td\>
  \<td\>\<b\>Jumps:\<\/b\> [0-9]+\<\/td\>
\<\/tr\>
\<tr\>
  \<td\>\<b\>Type:\<\/b\> \<a href=\"(?<itemURLsuffix>quicklook.html\?typeid=(?<itemID>[0-9]+))\"\>(?<itemName>[^<]+)\<\/a\>\<\/td\>
  \<td\>\<b\>Selling:\<\/b\> (?<askPrice>[,.0-9]+) ISK\<\/td\>
  \<td\>\<b\>Buying:\<\/b\> (?<bidPrice>[,.0-9]+) ISK\<\/td\>
\<\/tr\>

\<tr\>
  \<td\>\<b\>Per-unit profit:\<\/b\> [,.0-9]+ ISK\<\/td\>
  \<td\>\<b\>Units tradeable:\<\/b\> [,0-9]+ \((?<askVolume>[,0-9]+) -\&gt; (?<bidVolume>[,0-9]+)\)\<\/td\>
  \<td\>\&nbsp;\<\/td\>
\<\/tr\>
\<tr\>
  \<td\>\<b\>\<i\>Potential profit\<\/i\>\<\/b\>: [,.0-9]+ ISK \<\/td\>
  \<td\>\<b\>\<i\>Profit per trip:\<\/i\>\<\/b\>: [,.0-9]+ ISK\<\/td\>
  \<td\>\<b\>\<i\>Profit per jump\<\/i\>\<\/b\>: [,.0-9]+\<\/td\>
\<\/tr\>
\<tr\>\<td\>\&nbsp;\<\/td\>\<\/tr\>

'
### end string literal

  
  ### alternate implementation: chop into 24-line blocks for pattern match (4x slower)
  def self.get_chunk(io)
    chunk = String.new
    24.times do chunk << io.gets end
    chunk
  end
  
  def self.parse(html)
    puts "  parsing " + ("%0.2f" % (html.size / 1_000_000.0)) + "MB"
    
    ### parse preamble
    re1 = Regexp.new(REGEXP_PREAMBLE)
    m = re1.match(html)

    ### parse body (multiple blocks)
    trades = {}
    re2 = Regexp.new(REGEXP_PROFITABLE, Regexp::MULTILINE)
    #re2 = Regexp.new(REGEXP_PROFITABLE)
    block = m.post_match                 # primary implementation
    #io = StringIO.new(m.post_match)     # alternate implementation
    #block = get_chunk(io)               # alternate implementation
    start_parse = Time.new
    n = $counters[:orders]
    ### TODO: embed m.post_match in condition
    while (m = re2.match(block)) do
      $counters[:orders] += 1
      from = stn_fn2id(m[:askLocation])
      to = stn_fn2id(m[:bidLocation])
      item = m[:itemID]

      ### skip if non-hub station 
      if !(from && to) then 
        block = m.post_match            # primary implementation
        #block = get_chunk(io)          # alternate implementation
        next 
      end

      trade_id = [from, to, ("%05i" % item)].join(":")
      trades[trade_id] = true
      #puts trade_id
      block = m.post_match              # primary implementation
      #block = get_chunk(io)            # alternate implementation
    end 

    puts ("%5i" % ($counters[:orders] - n)) + " routes parsed"
    $timers[:parse] += Time.new - start_parse
    
    trades.keys
  end
  
  ### get_profitables(): get + parse list of profitable trades from evecentral
  ### return value: array of trade IDs (from_stn_id:to_stn_id:item_id)
  def self.get_profitables
    ### get html from eve-central.com
    responses = mget(urls_evecentral)
    $timers[:get] += (responses.max_by {|x| x.total_time}).total_time

    ### parse html
    trades_all = []
    responses.each do |r|
      html = r.body
      $counters[:size] += html.size

      puts url2route(r.effective_url)
      trades = parse(html)
      trades_all += trades
      puts ""
    end
    trades_all.sort 
  end
end ### class Evecentral

class Crest < Source
  @@root_url = 'https://crest-tq.eveonline.com/'
  @@client_id_encoded = 'ZTVhMTIyODAwYTEzNGRhMmFkNGIwZTAxNjY0YjYyN2I6WVE1aUN4a0FMM0tqQkNrOWRqQ0RLS0psc205SUptWmxxUlFIeWNTYg=='
  @@client_rtoken = '2-X4wdpBzGMTkpy8bdk0jg-gi6YfwVWyp_G9PbJtAME1'  

  @@crest_opts = Hash.new
  @@crest_opts[:followlocation]   = true
  @@crest_opts[:ssl_verifypeer]   = true
  @@crest_opts[:ssl_cipher_list]  = "TLSv1"
  @@crest_opts[:cainfo]           = __dir__ + "/cacert.pem"
  
  ### TODO: cache
  @@root_hrefs = nil

  
  ### get access token
  def self.refresh_atoken
    auth_url = "https://login-tq.eveonline.com/oauth/token/"
    #auth_url = "https://login.eveonline.com/oauth/token"

    ### HTTP options
    headers = {}
    headers["Authorization"] = "Basic #{@@client_id_encoded}"
    headers["Accept"] = "application/vnd.ccp.eve.Api-v3+json"
    params = {}
    params["grant_type"] = "refresh_token"
    params["refresh_token"] = @@client_rtoken
    opts = @@crest_opts.dup
    opts[:headers] = headers
    opts[:params] = params
    opts[:method] = :post
    #opts[:verbose] = true   ### debug

    now = Time.now
    r = get(auth_url, opts)
    json = JSON.parse(r.body)
    @@access_token = json["access_token"]
    @@access_expire = now + json["expires_in"]
    puts "access_token exp #{@@access_expire - Time.now}s = #{@@access_token}"
  end
  
  def self.get_atoken
    if (!defined?(@@access_token)) || (Time.now > @@access_expire) then refresh_atoken end
    @@access_token
  end

  
  ### TODO: check cache before sending GET
  
  ### get_href(): retrieves results from single- or multi-page responses
  ### return value: Array of "item" results (json hashes)
  def self.get_href(href)
    ### HTTP options
    opts = @@crest_opts.dup
    headers = {}
    headers["Authorization"] = "Bearer #{get_atoken}"
    headers["Accept"] = "application/vnd.ccp.eve.Api-v3+json"
    opts[:headers] = headers 
    opts[:method] = :get

    r = get(href, opts)
    json = JSON.parse(r.body)
    items = json["items"]  ### refactor with yield()

    ### technically we're supposed to iterate through each "next" link one at a time (for marketTypes takes 50s)
    ### instead we do a parallel get of pages 2-N (2x faster for for marketTypes)
    ### href format example "https://crest-tq.eveonline.com/market/types/?page=2"
    if (json["pageCount"] and json["pageCount"] > 1) then
      ### hack URI for pages 2-n
      n_pages = json["pageCount"]
      href_p2 = json["next"]["href"]
      m = href_p2.match("page=2")
      hrefs = (2..n_pages).map {|x| m.pre_match + "page=" + x.to_s + m.post_match}

      responses = mget(hrefs, opts) 
      responses.each do |r| 
        json = JSON.parse(r.body)
        items.concat(json["items"])
      end
    end

    items
  end
  ### alternate implementation: follow "next" links one at a time (CREST-compliant)
  #while (json["next"]) do
  #  href_next = json["next"]["href"]
  #  r = get(href_next, opts)
  #  json = JSON.parse(r.body)
  #  items.concat(json["items"])
  #end

  def self.get_root
    ### HTTP options
    opts = @@crest_opts.dup
    headers = {}
    headers["Authorization"] = "Bearer #{get_atoken}"
    headers["Accept"] = "application/vnd.ccp.eve.Api-v3+json"
    opts[:headers] = headers 
    opts[:method] = :get

    href = @@root_url
    r = get(href, opts)
    
    JSON.parse(r.body)    
  end
  
  
  ### item info from "/marketTypes/"
  def self.get_items
    raise ">>> get_items()"
    @@root_hrefs ||= get_root
    get_href(@@root_hrefs["marketTypes"]["href"])
  end

  ### region info from "/regions/"
  def self.get_regions
    raise ">>> get_regions()"
    @@root_hrefs ||= get_root
    get_href(@@root_hrefs["regions"]["href"])
  end
  
  ### arg "href_name": toplevel symbolic path ("/regions/", "/marketTypes/", etc.)
  ### NOTE: not the same as the corresponding path in the href itself
  def self.href_each(href_name)
    @@root_hrefs ||= get_root
    items = get_href(@@root_hrefs[href_name]["href"])
    items.each { |i| yield(i) }
  end
  
  def self.hrefs_each(hrefs, blocks)
    ### HTTP options
    opts = @@crest_opts.dup
    headers = {}
    headers["Authorization"] = "Bearer #{get_atoken}"
    headers["Accept"] = "application/vnd.ccp.eve.Api-v3+json"
    opts[:headers] = headers 
    opts[:method] = :get

    mget(hrefs, opts, blocks)
  end

end

### region hrefs
Crest.href_each("regions") do |x| 
  Regions[x["name"]].href = x["href"] 
end
### region market hrefs
hrefs = Array.new
blocks = Hash.new
Regions.each do |k, r|
  hrefs << r.href
  blocks[r.href] = lambda do |response| 
    json = JSON.parse(response.body)
    r.buy_href = json["marketBuyOrders"]["href"]
    r.sell_href = json["marketSellOrders"]["href"]
  end
end
hrefs.uniq!
Crest.hrefs_each(hrefs, blocks)

### TODO: href_each and hrefs_each have different usage
### href_each applies to each *item* returned by a single href
### hrefs_each applies to each *response* returned by multiple hrefs

#regions = Crest.get_regions
#jj regions[0]
#regions.each do |r|
#  Regions[r["name"]].href = r["href"]
#end

### station.region_href 
Stations.each do |k, s| 
  region = Regions[s.region_id]
  s.region_href = region.href
  s.region_buy_href = region.buy_href
  s.region_sell_href = region.sell_href  
end

### item.href
=begin this hits the crest server pretty hard, comment out until ready to test item data
Crest.href_each("marketTypes") do |x|
  id = x["type"]["id"]
  if (! Items[id]) then 
    ### late-added SKINs
    Items.add(Item.new({id:id, name:x["type"]["name"], volume:1.0}))
    #puts ">>> Item not in SDD: #{id} \"#{x[:type.to_s][:name.to_s]}\""
  end
  assert(Items[id], "Crest item #{id} \"" + x["type"]["name"] + "\" not in SDD")
  assert_eq(Items[id].name, x["type"]["name"], "item DB mismatch name")
  Items[id].href = x["type"]["href"]
end
=end


### main loop

while 1 
  trades = Evecentral.get_profitables
  trades.each do |t|
      from, to, item = t.split(":")
      ### get orders
      ### add to route candidates
  end

  ### calc candidates
  ### write route

  ### get trades
  ### get market orders from crest
        # need region IDs
        # need 
  ### calc trades
  ### save trades + orders to db
  
  
  size = $counters[:size]; $counters[:size] = 0;
	puts "overall size: " + sprintf("%3.1f", size/1_000_000.0) + "MB"
	puts "overall rate: " + sprintf("%.2f", ((size/1_000_000.0) / $timers[:get])) + "MB/s"
	puts "overall time: " + $timers[:get].to_s + "s"; $timers[:get] = 0;
	puts "orders: #{$counters[:orders]}" ; $counters[:orders] = 0;
	puts "trades: #{trades.length}"
	puts "parse time:   " + ("%.1f" % ($timers[:parse] * 1_000.0)) + "ms"; $timers[:parse] = 0
	puts "GETs: #{$counters[:get]}" ; $counters[:get] = 0;
	puts "\n------------\n"
	
	sleep 60
end



### abandoned junk
# Timer: start stop reset get

