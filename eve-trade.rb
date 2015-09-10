require 'typhoeus'
require 'mysql2'
require 'json'
require 'base64'
require 'stringio'
require 'pp'



### global variables
$timers = Hash.new(0)
$counters = Hash.new(0)



### utility functions
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



### Mymysql class - wrapper for mysql default options
class Mymysql < Mysql2::Client
  ### mysql option defaults
  @@mysql_opts = {}
  @@mysql_opts[:host]     = "69.181.214.54"
  #@@mysql_opts[:host]     = "127.0.0.1"
  @@mysql_opts[:port]     = "3306"
  @@mysql_opts[:username] = "dev"
  @@mysql_opts[:password] = "BNxJYjXbYXQHAvFM"

  def initialize(db_name)
    opts = {}
    opts[:database] = db_name
    @@mysql_opts.each do |k, default| 
      opts[k] ||= default
    end
    super(opts)
  end
end



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
    db = Mymysql.new(db_name)
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

    puts "Loading #{dst_type.downcase} DB \t sdd." + ("%-11s" % db_table) + "  (#{sz dst_store})"
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
###   pulls data from: 
###   (a) EVE static data - id, name, region_id
###   (b) CREST - href
### public methods:
###   Stations[id] => Station object
### Station fields:
###   id
###   name
###   href
###   region_id
class Station
  attr_accessor  :id, :name, :href, :region_id, :solarsystem_id

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
      {id:"stationID", name:"stationName", region_id:"regionID", solarsystem_id:"solarSystemID"})
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

  @@_regions = Hash.new  ### main datastore, duplicately indexed by id and name
  
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


class Order
  attr_accessor \
    :id, \
    :item_id, :buy, :price, \
    :vol_rem, :vol_orig, :vol_min, \
    :station_id, :range, :region_id, \
    :issued, :duration, :sampled

  def initialize(fields={})
    fields.each { |k,v| instance_variable_set("@#{k}", v) }
  end
end

### $markets[region][item] => Market
###   buy_orders = Orders[]
###   sell_orders = Orders[]
###   buy_sampled = Time
###   sell_sampled = Time
### NOTE: need to be able to delete all orders for a given region-item (at new data)
class Market
  attr_accessor :buy_orders, :sell_orders, :buy_sampled, :sell_sampled
  def initialize()
    @buy_orders = []
    @sell_orders = []
    @buy_sampled = Time.new(0)
    @sell_sampled = Time.new(0)
  end
end
## auto-populates $markets[x] and $markets[x][y]
$markets = Hash.new {|h1,region| h1[region] = Hash.new {|h2,item| h2[item] = Market.new} } 




### Cache class - key, value (Response), expiration time
### get(key)
### set(key, val, time)
### NOTE: _cache[k] = [v, expire]
class Cache
  @@_cache = Hash.new([nil, nil])
  @@_hits = 0
  
  def self.set(k, v, exp)
    @@_cache[k] = [v, exp]
  end

  def self.get(k, now=Time.now)
    v, exp = @@_cache[k]
    if v and now < exp
      #puts ">>> cache hit #{Crest.pretty k}" unless k.match(Crest.re_market)
      @@_hits += 1
      v
    else
      nil
    end
  end

  def self.save(fname)
    f = File.new(fname, "w")
    @@_cache.each do |k, pair|
      v, exp = pair
      flat = {"key" => k, "val" => v, "exp" => exp.to_i}
      f.puts JSON.generate(flat)
    end
    f.close
    ### debug
    auth_url = "https://login-tq.eveonline.com/oauth/token/" 
    v, exp = @@_cache[auth_url]
    puts "cache line <#{auth_url}>: <#{v}> exp #{exp.to_s}"
 end
  
  def self.restore(fname)
    return if not File.exist? fname
    now = Time.now
    f = File.new(fname, "r")
    while (line = f.gets)
      line.chomp!
      x = JSON.parse(line)
      k = x["key"]
      v = x["val"]
      exp = Time.at(x["exp"]).getutc
      @@_cache[k] = [v, exp] unless exp < now
    end
    f.close
  end
  
  def self.hits
    @@_hits
  end
  def self.size
    size = 0;
    @@_cache.each do |k, x| size += k.size + x[0].size + x[1].inspect.size end
    size
  end
  def self.length
    @@_cache.length
  end
  def self.biggest
    k, biggest = @@_cache.max_by { |x| x[0].size }
    v, exp = biggest
    k.size + v.size + exp.inspect.size
  end

  ### cacheable() - checks if Response is cacheable
  ### return value - returns TTL, or nil if not cacheable
  ### never caches eve-central.com
  def self.cacheable(response)
    ttl = nil
    url = response.effective_url
    hdr = response.headers 
    body = JSON.parse(response.body)

    ### debug auth_url
    #puts ">>> cacheable #{url}"
    auth_url = "https://login-tq.eveonline.com/oauth/token/" 
    if url == auth_url then puts ">>> cacheable" + response.body end

    if hdr["Cache-Control"] and hdr["Cache-Control"].match("max-age=([0-9]+)") then ttl = $1.to_i end
    if body["expires_in"] then 
      puts ">>> found oauth response!"
      ttl = body["expires_in"] 
    end

    ### override for special cases
    if ttl and url.match(Evecentral.url_base) then ttl = nil end  ### 0s for Evecentral.get_profitables()
    #if ttl and url.match(Crest.re_market) then ttl = 60 end       ### 60s for market orders

    ttl
  end

end



### FakeResponse - mimic Typhoeus::Response objects for cache hits
### TODO: refactor mget() to return Response.body instead of Response
class FakeResponse
  attr_accessor :body, :effective_url, :total_time

  def initialize(url, body)
    @effective_url = url
    @body = body
    @total_time = 0
  end
end

class Source
	@@hydra = Typhoeus::Hydra.hydra
  
  def self.get(url, opts = {})
    urls = [url]
    responses = mget(urls, opts)
    responses[0]
  end
  
  ### mget() - parallel GETs with optional per-URL callback, returns array of Response.body's
  ### arguments:
  ###   urls[]    - array of URLs to get
  ###   opts{}    - (optional) hash of HTTP options, applied to all GETs
  ###   blocks{}  - (optional) hash of response callbacks indexed by URL, *each* callback is optional
	def self.mget(urls, opts = {}, blocks = nil)
    success = []
    now = Time.now.getutc
    puts "mget(#{urls.length}) -- #{Crest.pretty urls[0]}"
    while (not urls.empty?)
      requests = []
      first = true # debug
      urls.each do |url|
        #puts "GET -- #{url}"  ### debug
        opts[:ssl_verifypeer] ||= false
        #if url.match(Crest.re_market) then opts[:headers]["Accept"] = "application/vnd.ccp.eve.MarketOrderCollection-v1+json; charset=utf-8" end
        if url.match(Crest.re_market) then opts[:headers].delete("Accept") end

        body = Cache.get(url, now)
        if body
          response2 = FakeResponse.new(url, body)
          if blocks and blocks[url] then blocks[url].call(response2) end
          success << response2
          next
        end
        
        request = Typhoeus::Request.new(url, opts)
        request.on_complete do |response|
          if response.success?
            # hell yeah
            # invoke callback (if defined)
            blocks[url].call(response) if blocks && blocks[url]
            success << response
            ### cache
            if (ttl = Cache.cacheable(response)) then Cache.set(url, response.body, now + ttl) end
          elsif response.timed_out?
            # aw hell no
            puts("time out")
          elsif response.code == 0
            # Could not get an http response, something's wrong.
            puts(response.return_message)
          else
            # Received a non-successful http response.
            puts " => HTTP request failed: #{response.code} #{Crest.pretty url}" unless url.match(Crest.re_market)
            urls << url if response.code == 502   ### put 502s back on queue
          end
        end

        @@hydra.queue(request)
        $counters[:get] += 1
      end
      urls = [] ### reset queue; callbacks will repopulate
      
      @@hydra.run
      sleep 1
    end ### while

    success
  end
end


### Evecentral - wrapper class for pulling tradefinder results from eve-central.com
### public interface:
###   get_profitables() => returns array of joined strings "<from_stn_id>:<to_stn_id>:<item_id>"
class Evecentral < Source
  URL_BASE = "https://eve-central.com/home/tradefind_display.html"

  STN_AMARR   = 60008494
  STN_JITA    = 60003760
  STN_DODIXIE = 60011866

  SYS_AMARR = 	30002187
  SYS_JITA = 		30000142
  SYS_DODIXIE = 30002659

  def self.stn_is_hub(x) 
    lookup = {
      STN_AMARR   => true,
      STN_JITA    => true,
      STN_DODIXIE => true,
    }
    lookup[x] 
  end

  def self.pretty(url)
    sys2sn = {
      SYS_AMARR => "Amarr", 
      SYS_JITA => "Jita", 
      SYS_DODIXIE => "Dodixie",
    }
    sys_from = url.match("&fromt=([0-9]+)&")[1].to_i
    sys_to   = url.match("&to=([0-9]+)&")[1].to_i
    sys2sn[sys_from] + "-to-" + sys2sn[sys_to]
  end

  ### URLs for eve-central.com
  def self.url_base
    URL_BASE
  end
  def self.url_evecentral(from, to)
    maxDelay    = 48
    minProfit   = 1000
    maxSpace    = 8967
    maxRecords  = 99999

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

    url = URL_BASE + params
  end
  
  def self.urls_all_routes(hubs)
    urls = []
    hubs.each do |from|
      hubs.each do |to| 
        urls << url_evecentral(from, to) unless from == to 
      end
    end
    urls
  end

  REGEXP_PREAMBLE = \
'\<p\>Found \<i\>([0-9]+)\<\/i\> possible routes
.\<\/p\>
\<p\>Page [0-9]+\.
(\<a href=\".*\"\>Next page\<\/a\>
)?
\<hr \/\>
\<table border=0 width=90\%\>\n'

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

' ### end REGEXP_PROFITABLE

  def self.parse(html)
    #puts "  parsing " + ("%0.2f" % (html.size / 1_000_000.0)) + "MB"
    
    ### parse preamble
    re_preamble = Regexp.new(REGEXP_PREAMBLE)
    m = re_preamble.match(html)
    nominal = m[1]
    #puts ("%5i" % nominal) + " nominal routes"

    ### parse body (multiple blocks)
    trades = {}
    re_block = Regexp.new(REGEXP_PROFITABLE, Regexp::MULTILINE)
    start_parse = Time.new
    n = $counters[:profitables]
    while (m = re_block.match(m.post_match)) do
      $counters[:profitables] += 1
      from = Stations[m[:askLocation]].id
      to = Stations[m[:bidLocation]].id
      next if !stn_is_hub(from) || !stn_is_hub(to)  ### hub stations only

      item = m[:itemID]
      trade_id = [from, to, ("%05i" % item)].join(":")
      trades[trade_id] = true  # filter out dups
    end 
    $timers[:parse] += Time.new - start_parse
    #puts ("%5i" % ($counters[:profitables] - n)) + " routes parsed"
    
    trades.keys
  end
  
  ### get_profitables(): get + parse list of profitable trades from evecentral
  ### return value: array of trade IDs (from_stn_id:to_stn_id:item_id)
  def self.get_profitables
    puts "Evecentral.get_profitables()"
    ### get html from eve-central.com
    hubs = [SYS_AMARR, SYS_JITA, SYS_DODIXIE]
    #opts = {}; opts[:verbose] = true   ### debug
    #responses = mget(urls_all_routes(hubs), opts)
    responses = mget(urls_all_routes(hubs))
    $timers[:get] += (responses.max_by {|x| x.total_time}).total_time

    ### parse html
    trades_all = []
    responses.each do |r|
      html = r.body
      $counters[:size] += html.size

      puts pretty(r.effective_url) + " parsing"
      trades = parse(html)
      trades_all.concat(trades)
    end
    
    trades_all.uniq!
    trades_all.sort!
  end
end ### class Evecentral


### Crest - wrapper class for pulling market data from CCP authenticated CREST
class Crest < Source
  URI_ROOT = 'https://crest-tq.eveonline.com/'
  URI_PATH_ITEMTYPES  = 'marketTypes'   ### "/marketTypes/"
  URI_PATH_REGIONS    = 'regions'       ### "/regions/"
  RE_MARKET = "https://crest-tq\\.eveonline\\.com/market/[0-9]{8}/orders/(buy|sell)/\\?type=https://crest-tq\\.eveonline\\.com/types/[0-9]+/"

  @@client_id = 'e5a122800a134da2ad4b0e01664b627b'  # app ID (secret key passed by command line)
  @@client_rtoken = '2-X4wdpBzGMTkpy8bdk0jg-gi6YfwVWyp_G9PbJtAME1' ### app refresh token

  @@crest_http_opts = Hash.new
  @@crest_http_opts[:followlocation]   = true
  @@crest_http_opts[:ssl_verifypeer]   = true
  @@crest_http_opts[:ssl_cipher_list]  = "TLSv1"
  @@crest_http_opts[:cainfo]           = "#{__dir__}/cacert.pem"
  
  ### TODO: cache
  @@root_hrefs = nil
  
  def self.re_market
    RE_MARKET
  end
  
  def self.client_secret
    ARGV.each { |arg| if arg.match("--client-secret=(.*)") then return $1 end }
    raise "client secret key not provided"
  end
  
  def self.client_id_encoded
    Base64.strict_encode64(@@client_id + ":" + client_secret)
  end
  
  ### get access token
  def self.refresh_atoken
    auth_url = "https://login-tq.eveonline.com/oauth/token/"
    #auth_url = "https://login.eveonline.com/oauth/token"

    ### HTTP options
    headers = {}
    headers["Authorization"] = "Basic #{client_id_encoded}"
    headers["Accept"] = "application/vnd.ccp.eve.Api-v3+json"
    params = {}
    params["grant_type"] = "refresh_token"
    params["refresh_token"] = @@client_rtoken
    opts = @@crest_http_opts.dup
    opts[:headers] = headers
    opts[:params] = params
    opts[:method] = :post
    #opts[:verbose] = true   ### debug

    now = Time.now
    r = get(auth_url, opts)
    json = JSON.parse(r.body)
    @@access_token = json["access_token"]
    @@access_expire = now + json["expires_in"]
    #Cache.set(auth_url, r.body, @@access_token) ### shouldn't need to do this manually
    puts "access_token exp #{@@access_expire - Time.now}s = #{@@access_token}"
  end
  
  def self.get_atoken
    if (!defined?(@@access_token)) || (Time.now > @@access_expire) then refresh_atoken end
    @@access_token
  end

  
  ### TODO: check cache before sending GET
  
  ### get_href() - returns href response as array of Responses
  ###   handles multi-page responses invisibly
  def self.get_href(href)
    #puts "get_href -- #{href}"

    ### HTTP options
    opts = @@crest_http_opts.dup
    headers = {}
    headers["Authorization"] = "Bearer #{get_atoken}"
    #headers["Accept"] = "application/vnd.ccp.eve.Api-v3+json"
    opts[:headers] = headers 
    opts[:method] = :get

    p1_response = get(href, opts)
    responses = [p1_response]

    ### check for multi-page response
    ### technically we're supposed to iterate through each "next" link one at a time (for marketTypes takes 50s)
    ### instead we do a parallel get of pages 2-N (2x faster for for marketTypes)
    p1_json = JSON.parse(p1_response.body)
    if (p1_json["pageCount"] and p1_json["pageCount"] > 1) then
      ### parallel multi-get: hack URIs for pages 2-n
      ### href format example "https://crest-tq.eveonline.com/market/types/?page=2"
      n_pages = p1_json["pageCount"]
      p2_href = p1_json["next"]["href"]
      m = p2_href.match("page=2")
      hrefs = (2..n_pages).map {|x| m.pre_match + "page=" + x.to_s + m.post_match}
      responses_2_to_n = mget(hrefs, opts) 
      responses.concat(responses_2_to_n)
    end

    responses
  end
  ### alternate implementation: for multi-page responses, follow "next" links one at a time (CREST-compliant)
  #json = p1_json
  #while (json["next"]) do
  #  href_next = json["next"]["href"]
  #  r = get(href_next, opts)
  #  responses << r
  #  json = JSON.parse(r.body)
  #end

  ### get_href_as_items() - returns href response as array of json "items"
  def self.get_href_as_items(href)
    responses = get_href(href)
    items = Array.new
    responses.each do |r| 
      json = JSON.parse(r.body)
      items.concat(json["items"])
    end
    items
  end
  
  
  ### get_hrefs_each(): get multiple hrefs and apply corresponding block to each Response
  ### args
  ###   hrefs - array of hrefs to fetch
  ###   blocks - array of blocks indexed by href
  ### returns: array of Responses
  def self.get_hrefs_each(hrefs, blocks)
    #puts "get_hrefs_each(#{hrefs.length}) -- #{pretty hrefs[0]}"
    #hrefs.each do |h| print "  #{pretty(h)}" end ; puts ""
    
    ### HTTP options
    opts = @@crest_http_opts.dup
    headers = {}
    headers["Authorization"] = "Bearer #{get_atoken}"
    #headers["Accept"] = "application/vnd.ccp.eve.Api-v3+json"
    opts[:headers] = headers 
    opts[:method] = :get

    mget(hrefs, opts, blocks)
  end


  

  def self._get_root
    r = get_href(URI_ROOT)[0]
    JSON.parse(r.body)    
  end
  def self._href_item_types
    @@root_hrefs ||= _get_root
    @@root_hrefs[URI_PATH_ITEMTYPES]["href"]
  end
  def self._href_regions
    @@root_hrefs ||= _get_root
    @@root_hrefs[URI_PATH_REGIONS]["href"]
  end

  
  RE_CREST_MARKET2 = "https://crest-tq\\.eveonline\\.com/market/([0-9]{8})/orders/(buy|sell)/\\?type=https://crest-tq\\.eveonline\\.com/types/([0-9]+)/"
  def self.pretty(href)
    x = href
    if x.match(RE_CREST_MARKET2) then x = "#{$1}.#{$3}.#{$2}" end
    if x.match "eve-central.com" then x = Evecentral.pretty x end
    x = x.gsub(URI_ROOT, "/")
    x
  end
  
 
  
  ### load_regions() - populate Regions[].href from Crest
  def self._load_region_hrefs
    items = get_href_as_items(_href_regions)
    items.each { |i| Regions[i["name"]].href = i["href"] }
  end
  ### load_markets() - populate Regions[].buy_href, .sell_href (assumes Region.href is loaded)
  ### NOTE: ~100 GETs 
  def self._load_market_hrefs
    hrefs = []
    blocks = {}
    Regions.each do |k, region|
      hrefs << region.href
      blocks[region.href] = lambda do |response| 
        json = JSON.parse(response.body)
        region.buy_href = json["marketBuyOrders"]["href"]
        region.sell_href = json["marketSellOrders"]["href"]
      end
    end
    hrefs.uniq!
    get_hrefs_each(hrefs, blocks)
  end
  ### load_items() - populate Items[].href
  ### NOTE: Crest might have "new" items not seen in static data dump
  def self._load_itemtype_hrefs
    items = get_href_as_items(_href_item_types)
    items.each do |i|
      id = i["type"]["id"]
      if not Items[id] then Items.add(Item.new({id:id, name:i["type"]["name"], volume: 1.0})) end
      Items[id].href = i["type"]["href"]
      assert_eq(Items[id].name, i["type"]["name"], "item DB mismatch name")  ### test
    end
  end
  ### load_static_data() - public function, load hrefs for Regions and Items (prereq to fetching market orders)
  def self.load_static_data
    _load_region_hrefs    # Regions[].href
    _load_market_hrefs    # Regions[].buy_href, .sell_href
    _load_itemtype_hrefs  # Items[].href
  end

  
  ### converts Crest market order "item" to Order object
  def self.item2order(i, sampled = Time.new(0))
    Order.new({
      :id         => i["id"],
      :item_id    => i["type"]["id"],
      :buy        => i["buy"],
      :price      => i["price"].to_f,
      :vol_rem    => i["volume"],
      :vol_orig   => i["volumeEntered"],
      :vol_min    => i["minVolume"],
      :station_id => i["location"]["id"],
      :range      => i["range"],
      :region_id  => Stations[i["location"]["id"]].region_id,
      :issued     => DateTime.strptime(i["issued"], '%Y-%m-%dT%H:%M:%S').to_time.getutc,
      :duration   => i["duration"],
      :sampled    => sampled,
    })
  end

  ### get_market_orders() - returns href and callback for market order fetch
  ### branched for two flavors (buy and sell)
  def self._get_market_orders(region, item, now, buy)
    r = Regions[region]
    i = Items[item]
    m = $markets[region][item]
    sampled = buy ? m.buy_sampled : m.sell_sampled
    if now > sampled 
      href = (buy ? r.buy_href : r.sell_href) + "?type=" + i.href
      proc = Proc.new do |response|
        items = (JSON.parse(response.body))["items"]
        orders = items.map { |i| item2order(i, sampled) }
        m.instance_variable_set(buy ? :@buy_orders : :@sell_orders, orders)
        m.instance_variable_set(buy ? :@buy_sampled : :@sell_sampled, now)
        #puts ">>> crest #{buy ? 'buy ' : 'sell'} #{'%-11s' % r.name}::#{i.name}  #{now}"
      end
      [href, proc]
    else
      [nil, nil]
    end
  end
  def self._get_market_buy_orders(from, item, now)
    _get_market_orders(from, item, now, true)
  end
  def self._get_market_sell_orders(to, item, now)
    _get_market_orders(to, item, now, false)
  end

  def self.get_trades(trades)
    puts "Crest.get_trades()"
    hrefs = []
    procs = {}
    now = Time.now.getutc
    trades.each do |t|
      from_stn, to_stn, item = t.split(":")
      from = Stations[from_stn.to_i].region_id
      to = Stations[to_stn.to_i].region_id
      item = item.to_i
      
      ### get buy orders
      href, proc = _get_market_buy_orders(from, item, now)
      hrefs << href if href
      procs[href] = proc if href
      ### get sell orders
      href, proc = _get_market_sell_orders(to, item, now)
      hrefs << href if href
      procs[href] = proc if href 
    end
    responses = Crest.get_hrefs_each(hrefs, procs) ### multi-get
  end
  
end ### class Crest


class Marketlogs < Source
  ### known filename transformations {filename => in_game_name}
  @@fname2name = {
    'GDN-9 Nightstalker Combat Goggles' => 'GDN-9 "Nightstalker" Combat Goggles',
    'Odin Synthetic Eye (left_gray)' => 'Odin Synthetic Eye (left/gray)',
    'SPZ-3 Torch Laser Sight Combat Ocular Enhancer (right_black)' => 'SPZ-3 "Torch" Laser Sight Combat Ocular Enhancer (right/black)',
    'Public Portrait_ How To' => 'Public Portrait: How To',
  }
  @@name2fname = {}
  @@fname2name.each { |k, v| @@name2fname[v] = k }

  EXPORT_FNAME_REGEXP = /^C:\\Users\\csserra\\Documents\\EVE\\logs\\Marketlogs\\(?<region>[^-]+)-(?<item>.*?)-(?<yr>[0-9]{4})\.(?<mo>[0-9][0-9])\.(?<dy>[0-9][0-9]) (?<hh>[0-9][0-9])(?<mm>[0-9][0-9])(?<ss>[0-9][0-9])\.txt$/
  EXPORT_TRUMP_TIME = 5*60  ### live export data trumps CREST data for 5 mins 
  def self.file_attrs(fname)
    m = fname.match(EXPORT_FNAME_REGEXP)
    assert(m, "marketlog filename #{fname}")
    region_id = Regions[m[:region]].id
    item_id = Items[ @@fname2name[m[:item]] || m[:item] ].id
    sample_time = Time.utc(m[:yr], m[:mo], m[:dy], m[:hh], m[:mm], m[:ss]) + EXPORT_TRUMP_TIME
    [region_id, item_id, sample_time]
  end

  ### line2order() - converts marketlog row into Order object
  def self.line2order(line, sample_time=Time.new(0))
    price, vol_rem, item_id, range, order_id, vol_orig, vol_min, \
      buy, issued, duration, station_id, region_id, solarsystem_id, jumps \
      = line.split(',')
    Order.new({
      :id         => order_id.to_i, 
      :item_id    => item_id.to_i, 
      :buy        => (buy == "True"), 
      :price      => price.to_f,
      :vol_rem    => vol_rem.to_i, 
      :vol_orig   => vol_orig.to_i, 
      :vol_min    => vol_min.to_i,
      :station_id => station_id.to_i, 
      :region_id  => region_id.to_i,
      :range      => range, 
      :issued     => DateTime.strptime(issued, '%Y-%m-%d %H:%M:%S.%L').to_time.getutc,
      :duration   => duration.to_i, 
      :sampled    => sample_time,
    })
  end
  
  def self.file2orders(fname)
    region_id, item_id, sample_time = file_attrs(fname)
    file = File.new(fname, "r")
    file.gets   ### header line
    #header = "price,volRemaining,typeID,range,orderID,volEntered,minVolume,bid,issueDate,duration,stationID,regionID,solarSystemID,jumps,"
    orders = []
    while line = file.gets
      orders << line2order(line, sample_time)
    end
    file.close
    orders
  end
  ### test
  #assert_eq(item_id,   order.item_id,   "Marketlogs.file2orders() item mismatch")
  #assert_eq(region_id, order.region_id, "Marketlogs.file2orders() region mismatch")
  #assert_eq(region_id, Stations[order.station_id].region_id, "Marketlogs.file2orders() station-region mismatch")      

  EXPORT_DIR = "C:\\Users\\csserra\\Documents\\EVE\\logs\\Marketlogs\\"
  EXPORT_AGE = 3*24*60*60
  
  ### purge_old() - delete older versions of marketlog files
  def self.purge_old
    dir = Dir.new(EXPORT_DIR)
    latest_fnames = {}
    latest_times = {}
    dir.each do |fname_short|
      next if fname_short == "." or fname_short == ".."
      me_fname = EXPORT_DIR + fname_short
      region_id, item_id, me_sampled = Marketlogs.file_attrs(me_fname)
      mkt_id = [region_id, item_id].join(":")
      
      if latest_fnames[mkt_id]
        if me_sampled < latest_times[mkt_id]
          File.delete(me_fname)
          next
        elsif me_sampled > latest_times[mkt_id]
          File.delete(latest_fnames[mkt_id])
        end
      end
      latest_fnames[mkt_id] = me_fname
      latest_times[mkt_id] = me_sampled
    end
  end
  
  ### refresh() - check all marketlog files, import if more recent
  def self.refresh
    puts "Marketlogs.refresh()"
    ### call this every 2 sec
    purge_old
    update = false
    dir = Dir.new(EXPORT_DIR)
    dir.each do |fname_short|
      next if fname_short == "." or fname_short == ".."   ### skip system dirs

      fname = EXPORT_DIR + fname_short
      region_id, item_id, f_sampled = Marketlogs.file_attrs(fname)
      
      mkt = $markets[region_id][item_id]
      buy_sampled = mkt.buy_sampled
      sell_sampled = mkt.sell_sampled
      if (f_sampled > buy_sampled or f_sampled > sell_sampled) then
        orders = Marketlogs.file2orders(fname)
        if f_sampled > buy_sampled then
          puts ">>> file buy  #{'%11s' % Regions[region_id].name}::#{Items[item_id].name}"
          puts "    file=#{f_sampled.to_s}"
          puts "    mkts=#{$markets[region_id][item_id].buy_sampled}"
          mkt.buy_orders  = orders.select { |x| x.buy }
          mkt.buy_sampled = f_sampled
          update = true
        else
          puts "!!! file buy -- skip import"
          puts "    file=#{f_sampled.to_s}"
          puts "    mkts=#{$markets[region_id][item_id].buy_sampled}"
        end
        if f_sampled > sell_sampled then
          puts ">>> file sell #{'%11s' % Regions[region_id].name}::#{Items[item_id].name}"
          puts "    file=#{f_sampled.to_s}"
          puts "    mkts=#{$markets[region_id][item_id].sell_sampled}"
          mkt.sell_orders = orders.select { |x| !x.buy }
          mkt.sell_sampled = f_sampled
          update = true
        else
          puts "!!! file sell -- skip import"
          puts "    file=#{f_sampled.to_s}"
          puts "    mkts=#{$markets[region_id][item_id].sell_sampled}"
        end
      end
    end
    update
  end

end ### class Marketlogs




### main loop
while 1
  f_cache = "#{__dir__}/cache.txt"
  Cache.restore(f_cache)
  
  Crest.load_static_data
  #Crest._load_region_hrefs # test
  Crest._load_itemtype_hrefs # debug to show cache is working
 
  ### get trades
  ### requests << Evecentral.queue_profitables
  ### requests << Crest.queue_static_data
  ### mget

  #trades = Evecentral.get_profitables
  #Crest.get_trades(trades)
  #Marketlogs.refresh

  ### calc trades
  ### save to db
  Cache.save(f_cache)

  
  size = $counters[:size]; $counters[:size] = 0;
  puts "cache size:   #{'%.1f' % (Cache.size/1_000_000.0)}MB, #{Cache.length} entries, #{'%.1f' % (Cache.biggest/1_000.0)}KB max"
	puts "overall size: " + sprintf("%3.1f", size/1_000_000.0) + "MB"
	puts "overall rate: " + sprintf("%.2f", ((size/1_000_000.0) / $timers[:get])) + "MB/s"
	puts "overall time: " + $timers[:get].to_s + "s"; $timers[:get] = 0;
	puts "trades: #{$counters[:profitables]}" ; $counters[:profitables] = 0;
	puts "parse time:   " + ("%.1f" % ($timers[:parse] * 1_000.0)) + "ms"; $timers[:parse] = 0
  gets = $counters[:get]
  hits = Cache.hits
	puts "GETs: #{gets}"
	puts "cache hits: #{hits} (#{'%.1f' % (hits.to_f * 100.0 / (hits + gets))}%)"
  $counters[:get] = 0;
	STDERR.puts "\n------------\n"
	exit
	sleep 60
end ### while 1
