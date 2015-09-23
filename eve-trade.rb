require 'typhoeus'
require 'typhoeus'
require 'mysql2'
require 'json'
require 'base64'
require 'stringio'
require 'pp'
require 'net/http'


### global variables
$timers = Hash.new(0)
$counters = Hash.new(0)



### utility functions
def sz (h)
  size = 0
  h.each do |x|
    size += (JSON.generate(x)).size
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
def comma(x, pre="")
  s = '%.2f' % x
  n = s.size - 3
  min = (x < 0.0)? 4 : 3  ### account for leading "-"
  while n > min
    s.insert(n-3, ',')
    n -= 3
  end
  #'%17s' % (pre + s)
  pre + s
end
def comma_i(x, pre="")
  s = '%i' % x
  n = s.size
  min = (x < 0) ? 4 : 3  ### account for leading "-"
  while n > min
    s.insert(n-3, ',')
    n -= 3
  end
  pre + s
end



### Mymysql class - wrapper for mysql default options
class Mymysql < Mysql2::Client
  ### mysql option defaults
  @@mysql_opts = {}
  #@@mysql_opts[:host]     = "127.0.0.1"
  #@@mysql_opts[:host]     = "plugh.asuscomm.com"
  @@mysql_opts[:host]     = "69.181.214.54"
  @@mysql_opts[:port]     = 3306
  @@mysql_opts[:username] = "dev"
  @@mysql_opts[:password] = "BNxJYjXbYXQHAvFM"

  def initialize(db_name)
    opts = @@mysql_opts.dup
    opts[:database] = db_name
    super(opts)
  end
end

### SqlQueue class -- aggregate SQL INSERTs
### public methods
###   new_insert  -- set db, table, and field names
###   <<          -- add line to query (auto-submits if max exceeded)
###   flush()     -- submit query
class SqlQueue
  SQL_MAX_ADJUST = 2
  #SQL_MAX = 1_048_576 - SQL_MAX_ADJUST
  SQL_HDR = "\n  "
  SQL_EOL = ", "
  SQL_EOF = ";"

  def initialize(db, sql_base)
    @db = db
    @sql_base = sql_base
    @sql = @sql_base.dup
    ### query max packet size
    r = @db.query("SHOW variables LIKE 'max_allowed_packet';")
    r.each do |row| @sql_max = (row["Value"].to_i - SQL_MAX_ADJUST) if row["Variable_name"] == 'max_allowed_packet' end
  end

  def self.new_insert(db, table, field_names)
    sql_base = "INSERT INTO `#{table}` \n  #{field_names} \nVALUES "
    SqlQueue.new(db, sql_base)
  end
  
  def <<(vals)
    if (@sql.size + SQL_HDR.size + vals.size + SQL_EOF.size) > @sql_max then flush end
    @sql << SQL_HDR + vals + SQL_EOL
  end
  
  def flush
    @sql.chomp!(SQL_EOL)
    @sql << SQL_EOF
    #puts "Sql.flush #{@sql.size} #{'%+i' % (@sql.size - @sql_max)}"
    @db.query(@sql)
    @sql = @sql_base.dup  # reset
  end
end



### import_sdd() - import SDD DB rows into datastore according to field mappings
###   hash is duplicate indexed by id and name
### args
###   SDD_DB_NAME:  source DB
###   db_table:     source DB table
###   dst_type:     Class to be instantiated and stored in "dst_store" ["Item", "Station", "Region", etc.]
###   dst_store:    datastore hash
###   fieldmap:     src=>dst field mappings, for each field to be imported
SDD_DB_NAME = "evesdd_galatea"
def import_sdd(db_table, fieldmap, klass, dst_store) 
  # skip if already loaded
  if dst_store.size == 0 then
    print "loading DB \"#{SDD_DB_NAME}.#{db_table}\"..."
    start = Time.now

    ### query db
    db_name = SDD_DB_NAME
    db = Mymysql.new(db_name)
    db_fields = fieldmap.keys.join(", ")
    sql = "SELECT #{db_fields} FROM #{db_table}"
    results = db.query(sql)

    ### convert row
    results.each do |src_row|
      ### define fields and instantiate object
      dst_fields = {}
      fieldmap.each { |k_src, k_dst| dst_fields[k_dst] = src_row[k_src] }
      #klass = Object.const_get(dst_type)  ### lookup constructor by class name
      x = klass.new(dst_fields)
      ### save to datastore
      dst_store[x.id] = x
      dst_store[x.name] = x if x.name ### duplicate index by name
    end
    puts "done (#{sz dst_store}, #{Time.now - start} sec)"
  end
end



### InitByHash -- initialize() via hash of name-value pairs
module InitByHash
  def initialize(fields={})
    fields.each { |k,v| instance_variable_set("@#{k}", v) }
  end
end


### Datum -- base wrapper class for Eve game data
### subclassed for each data type (Item, Region, Station)
### each instance is a single data element
### for DB compatibility, each instance has a unique ID and can be accessed by using the class as a hash
###   Item[id]
###   Region[name]  (sometimes duplicate indexed)
###   Station.each
class Datum
  include InitByHash
  
  def self.[](id)
    @data[id]
  end
  
  def self.[]=(id, rval)
    @data[id] = rval
  end
  
  def self.each
    @data.values.uniq.each { |v| yield(v.id, v) }  ### filter out duplicate indexes
  end

  def self.delete(id)
    @data.delete(id)
  end
  
  def self.export_sql_table(db, db_table)
    #db_name, db_table = db_id.split(".")
    #db = Mymysql.new(db_name)
    ### wipe table
    db.query("DELETE FROM `#{db_table}`;")
    ### insert new rows
    q = SqlQueue.new_insert(db, db_table, self.export_sql_hdr)
    self.each do |id, x| q << x.export_sql end
    q.flush
    puts "INSERT #{db_table}"
  end
end


### Item class - wrapper class for Eve item data 
### fields
###   id
###   name
###   volume
###   href
class Item < Datum
  attr_accessor :id, :name, :volume, :href

  @data = {}        ### Item[] datastore
  @sdd_db_table     = "invtypes"
  @sdd_db_fieldmap  = {"typeID"=>:id, "typeName"=>:name, "volume"=>:volume}
  import_sdd(@sdd_db_table, @sdd_db_fieldmap, self, @data)
end


### Station class - wrapper class for Eve station data 
### fields
###   id
###   name
###   href
###   region_id
###   system_id (solar system)
class Station < Datum
  attr_accessor :id, :name, :sname, :href, :region_id, :system_id
  def initialize(fields={})
    super(fields)
    if @name then @sname = @name[0, @name.index(' ')] end  ### shortname
  end

  @data = {}        ### Station[] datastore
  @sdd_db_table     = "stastations"
  @sdd_db_fieldmap  = {"stationID"=>:id, "stationName"=>:name, "regionID"=>:region_id, "solarSystemID"=>:system_id}
  import_sdd(@sdd_db_table, @sdd_db_fieldmap, self, @data)
end


### Region: wrapper class for Eve region data
### fields
###   id
###   name
###   href          Crest URI for region
###   buy_href      Crest URI base for market buy orders
###   sell_href     Crest URI base for market sell orders
class Region < Datum
  attr_accessor :id, :name, :href, :buy_href, :sell_href
  
  @data = {}        ### Region[] datastore
  @sdd_db_table     = "mapregions"
  @sdd_db_fieldmap  = {"regionID"=>:id, "regionName"=>:name}  
  import_sdd(@sdd_db_table, @sdd_db_fieldmap, self, @data)
end
### Region[]: add index by Station id
Station.each {|stn_id, stn| Region[stn_id] = Region[stn.region_id]}



### Item test cases
id = 28272
vol = 10.0
name = "'Augmented' Hammerhead"
assert_eq(Item[id].name, name, "item DB: name info failed")
assert_eq(Item[id].volume, vol, "item DB: volume info failed")
assert_eq(Item[name].id, id, "item DB: name lookup failed")
### Station test cases
id = 60000004
name = "Muvolailen X - Moon 3 - CBD Corporation Storage"
region = 10000033
assert_eq(Station[id].name, name, "station DB: name info failed")
assert_eq(Station[id].region_id, region, "station DB: region info failed")
assert_eq(Station[name].id, id, "station DB: name lookup failed")
### Region test cases
id = 10000002
name = "The Forge"
assert_eq(Region[id].name, name, "region DB: name info failed")
assert_eq(Region[name].id, id, "region DB: name lookup failed")




### Order class - EVE market order
### NOTE: data source could be Crest or Marketlogs
class Order < Datum
  attr_accessor \
    :id, :db_id, \
    :item_id, :buy, :price, \
    :vol_rem, :vol_orig, :vol_min, \
    :station_id, :range, :region_id, \
    :issued, :duration, :sampled, \
    :ignore

  @data = {}    ### Order[] datastore
  
  def self.import_crest(i, sampled = Time.new(0))
    o = Order.new({
      :id         => i["id"],
      :item_id    => i["type"]["id"],
      :buy        => i["buy"],
      :price      => i["price"].to_f,
      :vol_rem    => i["volume"],
      :vol_orig   => i["volumeEntered"],
      :vol_min    => i["minVolume"],
      :station_id => i["location"]["id"],
      :range      => i["range"],
      :region_id  => Station[i["location"]["id"]].region_id,
      :issued     => DateTime.strptime(i["issued"], '%Y-%m-%dT%H:%M:%S').to_time.getutc,
      :duration   => i["duration"],
      :sampled    => sampled,
    })
    Order[o.id] = o
    o
  end

  def to_s
    (buy ? 'bid' : 'ask') + " #{'%17s' % (comma(price, '$'))} #{'%7s' % (comma_i(vol_rem, 'x'))}" + (vol_min > 1 ? " (_#{vol_min})" : "")
  end

  def self.export_sql_hdr
    "(order_id, station_id, region_id, item_id, buy, price, price_str, vol, vol_str, ignored)"
  end
  def export_sql
    "(#{id}, #{station_id}, #{region_id}, #{item_id}, #{buy}, #{price}, '#{comma(price, '$')}', #{vol_rem}, '#{comma_i vol_rem}', #{ignore ? 'true' : 'false'})"
  end
end



### TODO: store $markets data in Region[r].buy_orders[item]

### $markets[region][item] => Market
###   buy_orders = Orders[]
###   sell_orders = Orders[]
###   buy_sampled = Time
###   sell_sampled = Time
### NOTE: need to be able to delete all orders for a particular region-item pair (when fresher data)
class Market
  attr_accessor :region, :item, :buy_orders, :sell_orders, :buy_sampled, :sell_sampled
  def initialize(region, item)
    @region = region
    @item = item
    @buy_orders = []
    @sell_orders = []
    @buy_sampled = Time.new(0)
    @sell_sampled = Time.new(0)
  end
end
## autovivify $markets[r] and $markets[r][i]
$markets = Hash.new {|h1,region| h1[region] = Hash.new {|h2,item| h2[item] = Market.new(region, item)} } 



### Cache class -- stores 3-tuples (key x value x expiration time)
### methods
###   get(key)
###   set(key, val, time)
###   ttl() -- defines cache policy based on key type
### usage
###   _cache[k] = [v, expire]
###   v = Response.body
class Cache
  CACHE_FILE = "#{__dir__}/cache.txt"

  @@_cache = Hash.new([nil, nil])
  @@_hits = 0

  def self.set(k, v, exp)
    @@_cache[k] = [v, exp]
  end

  def self.get(k, now=Time.now)
    v, exp = @@_cache[k]
    if v and now < exp
      #puts "    cache hit #{Crest.pretty k}" if k.match("^#{Crest.re_root}$")
      #puts "    cache hit #{Crest.pretty k}" if k.match(Crest.re_auth)
      #puts "    cache hit #{Crest.pretty k}" if k.match(Crest.re_market)
      #puts "    cache hit #{Crest.pretty k}" if k.match("/market/types/")
      #puts "    cache hit #{Crest.pretty k}" if k.match("/market/types/.page=12")
      #puts "    cache hit #{Crest.pretty k}" if k.match("/regions/$")
      #puts "    cache hit #{Crest.pretty k}" if k.match("/regions/10000001")
      puts "    cache hit #{Crest.pretty k}" if k.match(Evecentral.url_base)
      @@_hits += 1
      ### access token response is cached with relative "expires_in" value, so adjust to actual TTL
      if k.match(Crest.re_auth) then
        jhash = JSON.parse(v)
        jhash["expires_in"] = exp - now
        v = JSON.generate(jhash)
        @@_cache[k] = [v, exp]
      elsif k.match(Evecentral.url_base)
        @@_cache.delete(k)  ### eve-central.com expires after 1 cache get (for prefetch)
      end
      v
    else
      nil
    end
  end

  def self.save
    fname = CACHE_FILE
    now = Time.now
    @@_cache.delete_if { |k, (v, exp)| now > exp }
    f = File.new(fname, "w")
    @@_cache.each do |k, pair|
      next if k.match(Evecentral.url_base)  ### do not save eve-central.com (~20MB) 
      v, exp = pair
      if now > exp then @@_cache.delete(k); next end
      flat = {"key" => k, "val" => v, "exp" => exp.to_i}
      f.puts JSON.generate(flat)
    end
    f.close
    puts "cache saved #{Time.now-now}s"
 end
  
  def self.restore
    fname = CACHE_FILE
    return if not File.exist? fname
    print "cache loading..."
    start = Time.now
    f = File.new(fname, "r")
    while (line = f.gets)
      line.chomp!
      x = JSON.parse(line)
      k = x["key"]
      v = x["val"]
      exp = Time.at(x["exp"]).getutc
      @@_cache[k] = [v, exp] unless exp < start
    end
    f.close
    puts "done #{Time.now-start}s"
  end
  
  def self.hits
    @@_hits
  end
  def self.hits=(rval)
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

  ### ttl() - returns TTL, or nil if not cacheable 
  ### not cached: eve-central.com, google.com
  def self.ttl(response)
    ret = nil
    url = response.effective_url
    hdr = response.headers 

    ### override for special cases
    if url.match("google")
      ret = nil
    elsif url.match(Crest.re_market) 
      ret = nil
    elsif url.match("/market/types/") or url.match("/regions/") or url.match("^#{Crest.re_root}$")
      ret = 60*60
    elsif url.match(Evecentral.url_base) 
      ret = 60  ### just enough for prefetch to carryover to actual fetch
    ### server-defined TTLs
    elsif url.match(Crest.re_auth) 
      ret = (JSON.parse(response.body))["expires_in"] 
    elsif hdr["Cache-Control"] and hdr["Cache-Control"].match("max-age=([0-9]+)")
      ret = $1.to_i
    end

    ret
  end

end



### FakeResponse - mimic Typhoeus::Response objects for cache hits
### problem: mget returns Response, cache returns Response.body
### TODO: refactor mget() to return Response.body instead of Response, so we don't have to do this
class FakeResponse
  attr_accessor :body, :effective_url, :total_time

  def initialize(url, body)
    @effective_url = url
    @body = body
    @total_time = 0
  end
end


### Source - wrapper class for Typhoeus mget() and get()
### subclass for each HTTP data source
class Source
	@@hydra = Typhoeus::Hydra.new
  
  def self.get(url, opt = {})
    urls = [url]
    opts = {url => opt}
    responses = mget(urls, opts)
    responses[0]
  end
  
  ### mget() - parallel GETs with optional per-URL callback, returns array of Response.body's
  ### arguments:
  ###   urls[]    - array of URLs to get
  ###   opts{}    - (optional) hash of HTTP options, applied to all GETs
  ###   blocks{}  - (optional) hash of response callbacks indexed by URL, *each* callback is optional
	def self.mget(urls, opts = {}, blocks = {})
    success = []
    now = Time.now.getutc
    puts "mget(#{urls.length}) -- #{Crest.pretty urls[0]}"
    start = nil
    output = nil
    while (not urls.empty?)
      requests = []
      first = true # debug
      urls.each do |url|
        #puts "GET -- #{url}"  ### debug
        opt = (opts[url])? opts[url] : Hash.new
        opt[:ssl_verifypeer]  ||= false
        #opt[:ssl_verifypeer]  ||= true
        #opt[:followlocation]  ||= true
        #opt[:ssl_cipher_list] ||= "TLSv1"
        #opt[:cainfo]          ||= "#{__dir__}/cacert.pem"
        #opt[:verbose] = true
        #if url.match(Crest.re_market) then opt[:headers]["Accept"] = "application/vnd.ccp.eve.MarketOrderCollection-v1+json; charset=utf-8" end
        if url.match(Crest.re_market) then opt[:headers].delete("Accept") end
        
        body = Cache.get(url, now)
        if body
          response2 = FakeResponse.new(url, body)
          blocks[url].call(response2) if blocks[url]
          success << response2
          next
        end
        
        request = Typhoeus::Request.new(url, opt)
        request.on_complete do |response|
          output = ">>> #{pretty url} at #{Time.now - start}" unless url.match("/regions/[0-9]{8}") #or url.match(Crest.re_market)
          if response.success?
            # invoke callback
            blocks[url].call(response) if blocks[url]
            success << response
            ### cache
            if (ttl = Cache.ttl(response)) then Cache.set(url, response.body, now + ttl) end
          elsif response.timed_out?
            puts("timeout")
            urls << url
            exit
          elsif response.code == 0
            puts(response.return_message)
          else
            puts " => HTTP request failed: #{response.code} #{Crest.pretty url}" unless url.match(Crest.re_market)
            if response.code == 502 then urls << url end    ### requeue 502s
          end
        end

        @@hydra.queue(request)
        $counters[:get] += 1
      end
      urls = [] ### reset queue; callbacks will repopulate
      
      start = Time.now
      @@hydra.run
      if output then puts output end
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

  def self.import_html(html)
    
    ### parse preamble
    re_preamble = Regexp.new(REGEXP_PREAMBLE)
    m = re_preamble.match(html)
    nominal = m[1]

    ### parse body (multiple blocks)
    trades = {}
    re_block = Regexp.new(REGEXP_PROFITABLE, Regexp::MULTILINE)
    start_parse = Time.new
    n = $counters[:profitables]
    while (m = re_block.match(m.post_match)) do
      $counters[:profitables] += 1
      from = Station[m[:askLocation]].id
      to = Station[m[:bidLocation]].id
      item = m[:itemID].to_i
      next if !stn_is_hub(from) || !stn_is_hub(to)  ### hub stations only
      trade = [from, to, item]
      trade_id = trade.join(":")
      trades[trade_id] = trade  ### filter out dups
    end 
    $timers[:parse] += Time.new - start_parse
    #puts ("%5i" % ($counters[:profitables] - n)) + " routes parsed"
    #puts "  parsing " + ("%0.2f" % (html.size / 1_000_000.0)) + "MB"

    trades.values
  end
  
  ### get_profitables(): get + parse list of profitable trades from evecentral
  ### return value: array of trade IDs (from_stn_id:to_stn_id:item_id)
  def self.get_profitables
    #puts "Evecentral.get_profitables()"

    ### get html from eve-central.com
    hubs = [SYS_AMARR, SYS_JITA, SYS_DODIXIE]
    responses = mget(urls_all_routes(hubs))
    $timers[:get] += (responses.max_by {|x| x.total_time}).total_time

    ### parse html
    ret = []
    responses.each do |r|
      puts pretty(r.effective_url) + " parsing"
      $counters[:size] += r.body.size
      html = r.body
      trades = import_html(html)
      ret.concat(trades)
    end
    ret
  end
end ### class Evecentral




### Crest - wrapper class for pulling market data from CCP authenticated CREST
### public interface
###   import_orders
class Crest < Source
  URI_ROOT = 'https://crest-tq.eveonline.com/'
  URI_PATH_ITEMTYPES  = 'marketTypes'   ### "/marketTypes/"
  URI_PATH_REGIONS    = 'regions'       ### "/regions/"
  URL_AUTH = "https://login-tq.eveonline.com/oauth/token/"
  #auth_url = "https://login.eveonline.com/oauth/token"
  RE_MARKET = "https://crest-tq\\.eveonline\\.com/market/[0-9]{8}/orders/(buy|sell)/\\?type=https://crest-tq\\.eveonline\\.com/types/[0-9]+/"

  @@client_id = 'e5a122800a134da2ad4b0e01664b627b'  ### app ID (secret key is passed by command line)
  @@client_rtoken = '2-X4wdpBzGMTkpy8bdk0jg-gi6YfwVWyp_G9PbJtAME1' ### app refresh token

  @@crest_http_opts = Hash.new
  @@crest_http_opts[:followlocation]   = true
  @@crest_http_opts[:ssl_verifypeer]   = true
  @@crest_http_opts[:ssl_cipher_list]  = "TLSv1"
  @@crest_http_opts[:cainfo]           = "#{__dir__}/cacert.pem"
  
  ### TODO: cache
  @@root_hrefs = nil
  @@_static_loaded = false
  
  def self.re_market
    RE_MARKET
  end
  def self.re_auth
    URL_AUTH
  end
  def self.re_root
    URI_ROOT
  end
  
  def self._client_secret
    ARGV.each { |arg| if arg.match("--client-secret=(.*)") then return $1 end }
    raise "client secret key not provided"
  end
  
  def self._client_id_encoded
    Base64.strict_encode64(@@client_id + ":" + _client_secret)
  end
  
  ### get access token
  def self.refresh_atoken_opts
    opts = @@crest_http_opts.dup

    headers = {}
    headers["Authorization"] = "Basic #{_client_id_encoded}"
    headers["Accept"] = "application/vnd.ccp.eve.Api-v3+json"
    params = {}
    params["grant_type"] = "refresh_token"
    params["refresh_token"] = @@client_rtoken
    opts[:headers] = headers
    opts[:params] = params
    opts[:method] = :post
    #opts[:verbose] = true   ### debug
    
    opts
  end
  def self.set_atoken(response)
    json = JSON.parse(response.body)
    @@access_token = json["access_token"]

    now = Time.now
    @@access_expire = now + json["expires_in"]
    #puts "access_token exp #{@@access_expire - Time.now}s = #{@@access_token}"
  end
  def self._refresh_atoken
    opts = refresh_atoken_opts
    r = get(URL_AUTH, opts)
    set_atoken(r)
  end  
  def self.get_atoken
    if (!defined?(@@access_token)) || (Time.now > @@access_expire) then _refresh_atoken end
    @@access_token
  end
  def self.singleton
    self
  end

  def self.http_opts
    @@crest_http_opts
  end
  
  ### get_href() - returns href response as array of Responses
  ###   handles multi-page responses invisibly
  def self.get_href(href)
    ### HTTP options
    opt = @@crest_http_opts.dup
    headers = {}
    headers["Authorization"] = "Bearer #{get_atoken}"
    #headers["Accept"] = "application/vnd.ccp.eve.Api-v3+json"
    opt[:headers] = headers 
    opt[:method] = :get
    opts = Hash.new(opt)
    
    p1_response = get(href, opt)
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
    opt = @@crest_http_opts.dup
    headers = {}
    headers["Authorization"] = "Bearer #{get_atoken}"
    #headers["Accept"] = "application/vnd.ccp.eve.Api-v3+json"
    opt[:headers] = headers 
    opt[:method] = :get
    opts = Hash.new(opt)
    
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
    if x.match(URL_AUTH) then x = "/oauth/token/" end
    x = x.gsub(URI_ROOT, "/")
    x
  end
  
 
  
  ### load_regions() - populate Region[].href from Crest
  def self._load_region_hrefs
    items = get_href_as_items(_href_regions)
    items.each { |i| Region[i["name"]].href = i["href"] }
  end
  ### load_markets() - populate Region[].buy_href, .sell_href (assumes Region.href is loaded)
  ### NOTE: ~100 GETs 
  def self._load_market_hrefs
    hrefs = []
    blocks = {}
    Region.each do |k, region|
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
  ### load_items() - populate Item[].href
  ### NOTE: Crest might have "new" items not seen in static data dump
  def self._load_itemtype_hrefs
    items = get_href_as_items(_href_item_types)
    items.each do |i|
      id = i["type"]["id"]
      if not Item[id] then Item[id] = Item.new({id:id, name:i["type"]["name"], volume: 1.0}) end
      Item[id].href = i["type"]["href"]
      assert_eq(Item[id].name, i["type"]["name"], "item DB mismatch name")  ### test
    end
  end
  ### _load_static_data() - fetch hrefs for Region and Item (prereq to fetching market orders)
  def self._load_static_data
    _load_region_hrefs    # Region[].href
    _load_market_hrefs    # Region[].buy_href, .sell_href
    _load_itemtype_hrefs  # Item[].href
    @@_static_loaded = true
  end

  
  ### converts Crest market order "item" to Order object
  def self.import_crest_order(i, sampled = Time.new(0))
    o = Order.new({
      :id         => i["id"],
      :item_id    => i["type"]["id"],
      :buy        => i["buy"],
      :price      => i["price"].to_f,
      :vol_rem    => i["volume"],
      :vol_orig   => i["volumeEntered"],
      :vol_min    => i["minVolume"],
      :station_id => i["location"]["id"],
      :range      => i["range"],
      :region_id  => Station[i["location"]["id"]].region_id,
      :issued     => DateTime.strptime(i["issued"], '%Y-%m-%dT%H:%M:%S').to_time.getutc,
      :duration   => i["duration"],
      :sampled    => sampled,
    })
    Order[o.id] = o
    o
  end

  ### get_market_orders() - returns href and callback for market order fetch
  ### branched for two flavors (buy and sell)
  def self._get_market_orders(region, item, now, buy)
    _load_static_data unless @@_static_loaded
    r = Region[region]
    i = Item[item]
    m = $markets[region][item]
    sampled = buy ? m.buy_sampled : m.sell_sampled
    if now > sampled 
      href = (buy ? r.buy_href : r.sell_href) + "?type=" + i.href
      proc = Proc.new do |response|
        items = (JSON.parse(response.body))["items"]
        $counters[:size] += response.body.size
        orders = items.map { |i| import_crest_order(i, sampled) }
        m.instance_variable_set(buy ? '@buy_orders' : '@sell_orders', orders)
        m.instance_variable_set(buy ? '@buy_sampled' : '@sell_sampled', now)
        #puts ">>> crest #{buy ? 'buy ' : 'sell'} #{'%-11s' % r.name}::#{i.name}  #{now}"
      end
      [href, proc]
    else
      [nil, nil]
    end
  end
  def self._get_market_buy_orders(to, item, now)
    _get_market_orders(to, item, now, true)
  end
  def self._get_market_sell_orders(from, item, now)
    _get_market_orders(from, item, now, false)
  end

  ### import_orders() - fetch market orders; populate $markets[region][item].buy_orders
  def self.import_orders(trades)
    #puts "Crest.import_orders()"
    hrefs = []
    procs = {}
    now = Time.now.getutc
    trades.each do |t|
      from_stn, to_stn, item = t
      from = Station[from_stn.to_i].region_id
      to = Station[to_stn.to_i].region_id
      item = item.to_i
      
      ### get sell orders
      href, proc = _get_market_sell_orders(from, item, now)
      hrefs << href if href
      procs[href] = proc if href
      ### get buy orders
      href, proc = _get_market_buy_orders(to, item, now)
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
    region_id = Region[m[:region]].id
    item_id = Item[ @@fname2name[m[:item]] || m[:item] ].id
    sample_time = Time.utc(m[:yr], m[:mo], m[:dy], m[:hh], m[:mm], m[:ss]) + EXPORT_TRUMP_TIME
    [region_id, item_id, sample_time]
  end

  ### import_line() - converts marketlog row into Order object
  def self.import_line(line, sample_time=Time.new(0))
    price, vol_rem, item_id, range, order_id, vol_orig, vol_min, \
      buy, issued, duration, station_id, region_id, system_id, jumps \
      = line.split(',')
    o = Order.new({
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
    Order[o.id] = o
    o
  end
  
  def self.import_file(fname)
    region_id, item_id, sample_time = file_attrs(fname)
    file = File.new(fname, "r")
    file.gets   ### header line
    #header = "price,volRemaining,typeID,range,orderID,volEntered,minVolume,bid,issueDate,duration,stationID,regionID,solarSystemID,jumps,"
    orders = []
    while line = file.gets
      orders << import_line(line, sample_time)
    end
    file.close
    orders
  end
  ### test
  #assert_eq(item_id,   order.item_id,   "Marketlogs.import_file() item mismatch")
  #assert_eq(region_id, order.region_id, "Marketlogs.import_file() region mismatch")
  #assert_eq(region_id, Station[order.station_id].region_id, "Marketlogs.import_file() station-region mismatch")      

  EXPORT_DIR = "C:\\Users\\csserra\\Documents\\EVE\\logs\\Marketlogs\\"
  EXPORT_TTL = 3*24*60*60 # delete after 3 days
  
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

      if Time.now - me_sampled > EXPORT_TTL     # past expiration so delete
          puts "!!! deleting marketlog expired #{me_fname}"
          File.delete(me_fname)
          next
      end
      
      if latest_fnames[mkt_id]
        if me_sampled < latest_times[mkt_id]      # less recent so delete
          puts "!!! deleting marketlog overtaken #{me_fname}"
          File.delete(me_fname)
          next
        elsif me_sampled > latest_times[mkt_id]   # more recent so delete other
          puts "!!! deleting marketlog overtaken #{latest_fnames[mkt_id]}"
          File.delete(latest_fnames[mkt_id])
        end
      end
      # current file is now best
      latest_fnames[mkt_id] = me_fname
      latest_times[mkt_id] = me_sampled
    end
  end
  
  ### import_orders() - check all marketlog files, import if more recent
  def self.import_orders
    #puts "Marketlogs.import_orders()"
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
        orders = Marketlogs.import_file(fname)
        if f_sampled > buy_sampled then
          puts ">>> file buy  #{'%11s' % Region[region_id].name}::#{Item[item_id].name}"
          mkt.buy_orders  = orders.select { |x| x.buy }
          mkt.buy_sampled = f_sampled
          update = true
        else
          puts "!!! file buy -- skip import, out of date"
        end
        if f_sampled > sell_sampled then
          puts ">>> file sell #{'%11s' % Region[region_id].name}::#{Item[item_id].name}"
          mkt.sell_orders = orders.select { |x| !x.buy }
          mkt.sell_sampled = f_sampled
          update = true
        else
          puts "!!! file sell -- skip import, out of date"
        end
      end
    end
    update
  end

end ### class Marketlogs






###
### prefetch
###

def _prefetch_mget(mget_q)
  urls = []
  opts = {}
  callbacks = {}
  mget_q.each do |x|
    url, opt, callback = x
    urls << url
    opts[url] = opt if opt
    callbacks[url] = callback if callback
  end
  Crest.mget(urls, opts, callbacks)
end

def prefetch
  prefetch_start = Time.now
  
  ###
  ### Phase 1
  ###
  mget_q = []   ### aggregate mget requests
  ### Phase 1: Crest access token
  url = "https://login-tq.eveonline.com/oauth/token/"
  opt = Crest.refresh_atoken_opts
  callback = Crest.method(:set_atoken)  ### callback is singleton method
  #puts callback
  mget_q << [url, opt, callback]
  ### Phase 1: Crest root
  mget_q << ['https://crest-tq.eveonline.com/', nil, nil]
  #mget_q << ['http://www.google.com', nil, nil]
  #mget_q << ['https://eve-central.com/home/tradefind_display.html?set=1&fromt=30002187&to=30000142&qtype=Systems&age=48&minprofit=1000&size=8967&limit=99999&sort=sprofit&prefer_sec=0', nil, nil]
  ### Phase 1: mget
  _prefetch_mget(mget_q)

  ###
  ### Phase 2
  ###
  mget_q = []
  ### Phase 2: eve-central profitables
  urls1 = [
  'https://eve-central.com/home/tradefind_display.html?set=1&fromt=30002187&to=30000142&qtype=Systems&age=48&minprofit=1000&size=8967&limit=99999&sort=sprofit&prefer_sec=0',
  'https://eve-central.com/home/tradefind_display.html?set=1&fromt=30002187&to=30002659&qtype=Systems&age=48&minprofit=1000&size=8967&limit=99999&sort=sprofit&prefer_sec=0',
  'https://eve-central.com/home/tradefind_display.html?set=1&fromt=30000142&to=30002187&qtype=Systems&age=48&minprofit=1000&size=8967&limit=99999&sort=sprofit&prefer_sec=0',
  'https://eve-central.com/home/tradefind_display.html?set=1&fromt=30000142&to=30002659&qtype=Systems&age=48&minprofit=1000&size=8967&limit=99999&sort=sprofit&prefer_sec=0',
  'https://eve-central.com/home/tradefind_display.html?set=1&fromt=30002659&to=30002187&qtype=Systems&age=48&minprofit=1000&size=8967&limit=99999&sort=sprofit&prefer_sec=0',
  'https://eve-central.com/home/tradefind_display.html?set=1&fromt=30002659&to=30000142&qtype=Systems&age=48&minprofit=1000&size=8967&limit=99999&sort=sprofit&prefer_sec=0',
  ]
  urls1.each do |url| mget_q << [url, nil, nil] end
  ### Phase 2: Crest root, regions, itemtypes
  urls2 = [
    'https://crest-tq.eveonline.com/',
    'https://crest-tq.eveonline.com/market/types/',
    'https://crest-tq.eveonline.com/market/types/?page=2',
    'https://crest-tq.eveonline.com/market/types/?page=3',
    'https://crest-tq.eveonline.com/market/types/?page=4',
    'https://crest-tq.eveonline.com/market/types/?page=5',
    'https://crest-tq.eveonline.com/market/types/?page=6',
    'https://crest-tq.eveonline.com/market/types/?page=7',
    'https://crest-tq.eveonline.com/market/types/?page=8',
    'https://crest-tq.eveonline.com/market/types/?page=9',
    'https://crest-tq.eveonline.com/market/types/?page=10',
    'https://crest-tq.eveonline.com/market/types/?page=11',
    'https://crest-tq.eveonline.com/market/types/?page=12',
    'https://crest-tq.eveonline.com/market/types/?page=13',
    'https://crest-tq.eveonline.com/regions/',
    'https://crest-tq.eveonline.com/regions/11000001/', 
    'https://crest-tq.eveonline.com/regions/11000002/', 
    'https://crest-tq.eveonline.com/regions/11000003/', 
    'https://crest-tq.eveonline.com/regions/10000019/', 
    'https://crest-tq.eveonline.com/regions/10000054/', 
    'https://crest-tq.eveonline.com/regions/11000004/', 
    'https://crest-tq.eveonline.com/regions/11000005/', 
    'https://crest-tq.eveonline.com/regions/11000006/', 
    'https://crest-tq.eveonline.com/regions/11000007/', 
    'https://crest-tq.eveonline.com/regions/11000008/', 
    'https://crest-tq.eveonline.com/regions/10000069/', 
    'https://crest-tq.eveonline.com/regions/10000055/', 
    'https://crest-tq.eveonline.com/regions/11000009/', 
    'https://crest-tq.eveonline.com/regions/11000010/', 
    'https://crest-tq.eveonline.com/regions/11000011/', 
    'https://crest-tq.eveonline.com/regions/11000012/', 
    'https://crest-tq.eveonline.com/regions/11000013/', 
    'https://crest-tq.eveonline.com/regions/11000014/', 
    'https://crest-tq.eveonline.com/regions/11000015/', 
    'https://crest-tq.eveonline.com/regions/10000007/', 
    'https://crest-tq.eveonline.com/regions/10000014/', 
    'https://crest-tq.eveonline.com/regions/10000051/', 
    'https://crest-tq.eveonline.com/regions/10000053/', 
    'https://crest-tq.eveonline.com/regions/10000012/', 
    'https://crest-tq.eveonline.com/regions/11000016/', 
    'https://crest-tq.eveonline.com/regions/11000017/', 
    'https://crest-tq.eveonline.com/regions/11000018/', 
    'https://crest-tq.eveonline.com/regions/11000019/', 
    'https://crest-tq.eveonline.com/regions/11000020/', 
    'https://crest-tq.eveonline.com/regions/11000021/', 
    'https://crest-tq.eveonline.com/regions/11000022/', 
    'https://crest-tq.eveonline.com/regions/11000023/', 
    'https://crest-tq.eveonline.com/regions/10000035/', 
    'https://crest-tq.eveonline.com/regions/10000060/', 
    'https://crest-tq.eveonline.com/regions/10000001/', 
    'https://crest-tq.eveonline.com/regions/10000005/', 
    'https://crest-tq.eveonline.com/regions/10000036/', 
    'https://crest-tq.eveonline.com/regions/10000043/', 
    'https://crest-tq.eveonline.com/regions/11000024/', 
    'https://crest-tq.eveonline.com/regions/11000025/', 
    'https://crest-tq.eveonline.com/regions/11000026/', 
    'https://crest-tq.eveonline.com/regions/11000027/', 
    'https://crest-tq.eveonline.com/regions/11000028/', 
    'https://crest-tq.eveonline.com/regions/11000029/', 
    'https://crest-tq.eveonline.com/regions/10000039/', 
    'https://crest-tq.eveonline.com/regions/10000064/', 
    'https://crest-tq.eveonline.com/regions/10000027/', 
    'https://crest-tq.eveonline.com/regions/10000037/', 
    'https://crest-tq.eveonline.com/regions/11000030/', 
    'https://crest-tq.eveonline.com/regions/10000046/', 
    'https://crest-tq.eveonline.com/regions/10000056/', 
    'https://crest-tq.eveonline.com/regions/10000058/', 
    'https://crest-tq.eveonline.com/regions/11000031/', 
    'https://crest-tq.eveonline.com/regions/10000029/', 
    'https://crest-tq.eveonline.com/regions/10000067/', 
    'https://crest-tq.eveonline.com/regions/10000011/', 
    'https://crest-tq.eveonline.com/regions/11000032/', 
    'https://crest-tq.eveonline.com/regions/10000030/', 
    'https://crest-tq.eveonline.com/regions/10000025/', 
    'https://crest-tq.eveonline.com/regions/10000031/', 
    'https://crest-tq.eveonline.com/regions/10000009/', 
    'https://crest-tq.eveonline.com/regions/10000017/', 
    'https://crest-tq.eveonline.com/regions/11000033/', 
    'https://crest-tq.eveonline.com/regions/10000052/', 
    'https://crest-tq.eveonline.com/regions/10000049/', 
    'https://crest-tq.eveonline.com/regions/10000065/', 
    'https://crest-tq.eveonline.com/regions/10000016/', 
    'https://crest-tq.eveonline.com/regions/10000013/', 
    'https://crest-tq.eveonline.com/regions/10000042/', 
    'https://crest-tq.eveonline.com/regions/10000028/', 
    'https://crest-tq.eveonline.com/regions/10000040/', 
    'https://crest-tq.eveonline.com/regions/10000062/', 
    'https://crest-tq.eveonline.com/regions/10000021/', 
    'https://crest-tq.eveonline.com/regions/10000057/', 
    'https://crest-tq.eveonline.com/regions/10000059/', 
    'https://crest-tq.eveonline.com/regions/10000063/', 
    'https://crest-tq.eveonline.com/regions/10000066/', 
    'https://crest-tq.eveonline.com/regions/10000048/', 
    'https://crest-tq.eveonline.com/regions/10000047/', 
    'https://crest-tq.eveonline.com/regions/10000023/', 
    'https://crest-tq.eveonline.com/regions/10000050/', 
    'https://crest-tq.eveonline.com/regions/10000008/', 
    'https://crest-tq.eveonline.com/regions/10000032/', 
    'https://crest-tq.eveonline.com/regions/10000044/', 
    'https://crest-tq.eveonline.com/regions/10000022/', 
    'https://crest-tq.eveonline.com/regions/10000041/', 
    'https://crest-tq.eveonline.com/regions/10000020/', 
    'https://crest-tq.eveonline.com/regions/10000045/', 
    'https://crest-tq.eveonline.com/regions/10000061/', 
    'https://crest-tq.eveonline.com/regions/10000038/', 
    'https://crest-tq.eveonline.com/regions/10000033/', 
    'https://crest-tq.eveonline.com/regions/10000002/', 
    'https://crest-tq.eveonline.com/regions/10000034/', 
    'https://crest-tq.eveonline.com/regions/10000018/', 
    'https://crest-tq.eveonline.com/regions/10000010/', 
    'https://crest-tq.eveonline.com/regions/10000004/', 
    'https://crest-tq.eveonline.com/regions/10000003/', 
    'https://crest-tq.eveonline.com/regions/10000015/', 
    'https://crest-tq.eveonline.com/regions/10000068/', 
    'https://crest-tq.eveonline.com/regions/10000006/', 
  ]
  opt = Crest.http_opts.dup
  headers = {}
  headers["Authorization"] = "Bearer #{Crest.get_atoken}"
  #headers["Accept"] = "application/vnd.ccp.eve.Api-v3+json"
  opt[:headers] = headers 
  opt[:method] = :get
  urls2.each do |url| mget_q << [url, opt, nil] end
  ### Phase 2: mget
  _prefetch_mget(mget_q)

  puts "prefetch #{Time.now - prefetch_start}s"
end





class Trade < Datum
  attr_accessor :id, :db_id
  attr_accessor :from_stn, :to_stn, :item, :bids, :asks
  attr_accessor :qty, :profit, :cost, :age, :size, :ppv, :roi

  @@uid = 1
  
  @data = {}
  K_SUM = 'totals'
  def self.each
    @data.values.uniq.each { |v| yield(v.id, v) unless v.item == K_SUM }  ### filter out duplicate indexes
  end  

  def initialize(fields={})
    ### primary
    @qty = 0
    @profit = 0.0
    @cost = 0.0
    @age = Time.new(0)
    ### secondary (derived)
    @size = 0.0
    @ppv = 0.0
    @roi = 0.0
    ### db
    @id = @@uid; @@uid+=1
    super(fields)
  end

  def +(other)
    self2 = self.dup
    self2.profit += other.profit
    self2.cost += other.cost
    self2.age = [self.age, other.age].max
    self2.size += other.size
    self2.ppv = self2.profit / self2.size
    self2.roi = self2.profit / self2.cost
    self2
  end

  def self.export_sql_hdr
    "(trade_id, from_stn, to_stn, item, qty, profit, cost, size, ppv, roi)"
  end
  def export_sql
    itemname_esc = Mysql2::Client.escape(Item[item].name)
    "(#{id}, '#{Station[from_stn].sname}', '#{Station[to_stn].sname}', '#{itemname_esc}', #{qty}, #{profit}, #{cost}, #{size}, #{ppv}, #{roi})"
  end

  def suspicious
    known_scams = {
      "Cormack's Modified Armor Thermic Hardener" => true,
      "Draclira's Modified EM Plating" => true,
      "Gotan's Modified EM Plating" => true,
      "Tobias' Modified EM Ward Amplifier" => true,
      "Ahremen's Modified Explosive Plating" => true,
      "Setele's Modified Explosive Plating" => true,
      "Raysere's Modified Mega Beam Laser" => true,
    }

    from = Station[from_stn].region_id
    to = Station[to_stn].region_id
    rt = "#{from}:#{to}"
    asks = $markets[from][item].sell_orders
    bids = $markets[to][item].buy_orders
    
    if Item[item].name.match("^(Improved|Standard|Strong) .* Booster$")
      ### contraband
      true
    elsif profit > 1_000_000_000 and known_scams[Item[item].name] then
      ### profit > 1B (scam)
      true
    elsif (bids[0].price - asks[0].price) > 29_000_000 and asks[0].price > 190_000_000
      ### 1x profit > 29M, cost > 190M
      true
    else
      ### minimum qty > 1
      bids.each {|order| if order.vol_min > 1 then return true end}
      false
    end
  end

end   ### Trade class


### find profitable trades
### in: $markets, candidates
### out: trades
def calc_trades(candidates)
  ### sort orders by price
  $markets.each_key do |reg|
    $markets[reg].each_key do |i|
      mkt = $markets[reg][i]
      next if mkt.buy_orders.length == 0 and mkt.sell_orders.length == 0
      mkt.sell_orders.sort! {|o1, o2| o1.price <=> o2.price }
      mkt.buy_orders.sort!  {|o1, o2| o2.price <=> o1.price }
    end
  end
  
  trades = {} ### trades{route_id}{item}  => item
              ### trades{route_id}{Trade::K_SUM} => route totals
  candidates.each do |tuple|
    from_stn, to_stn, iid = tuple
    from = Region[from_stn].id
    to = Region[to_stn].id
    rt = "#{from_stn}:#{to_stn}"
    trades[rt]               ||= {}
    trades[rt][Trade::K_SUM] ||= Trade.new({from_stn:from_stn, to_stn:to_stn, item:Trade::K_SUM})
    trades[rt][iid]          ||= Trade.new({from_stn:from_stn, to_stn:to_stn, item:iid})
    Trade[trades[rt][iid].id] = trades[rt][iid]  ### save to datastore
    trades[rt][iid].asks = $markets[from][iid].sell_orders
    trades[rt][iid].bids = $markets[to][iid].buy_orders
    trades[rt][iid].age = [$markets[from][iid].sell_sampled, $markets[to][iid].buy_sampled].max  ### most recent
  end

  net_tax = 0.9925  ### Accounting V
  min_profit = 1
  min_ppv = 1_000
  min_total_profit = 3_000_000

  ### match bids/asks
  ### iterate through all trades[route][item]
  puts "------------\n"
  trades.each_key do |rt| 
    from_stn, to_stn = rt.split(':')
    from_stn = from_stn.to_i
    to_stn = to_stn.to_i
    trades[rt].each do |iid, t|
      next if iid == Trade::K_SUM
      asks = t.asks
      bids = t.bids   

      i_ask = 0
      i_bid = 0
      ask_vol = 0
      bid_vol = 0
      while i_ask < asks.size and i_bid < bids.size
        ask = asks[i_ask]
        bid = bids[i_bid]
        if ask_vol == 0 then ask_vol = ask.vol_rem end
        if bid_vol == 0 then bid_vol = bid.vol_rem end
        
        profit = (net_tax * bid.price) - ask.price
        if profit < min_profit then break end
        
        qty = (ask_vol < bid_vol) ? ask_vol : bid_vol
        ask_vol -= qty
        bid_vol -= qty
        if ask_vol == 0 then i_ask += 1 end
        if bid_vol == 0 then i_bid += 1 end

        t.profit += profit
        t.qty += qty
        t.cost += qty * ask.price
      end  ### match bids/asks
      if (i_ask == 0 and i_bid == 0) then Trade.delete(t.id); trades[rt].delete(iid); next end ### no matches

      ### derived calcs
      t.size = t.qty * Item[iid].volume
      t.ppv = t.profit / t.size
      t.roi = t.profit / t.cost

      ### skip? check profit thresholds, known scams, etc.
      if t.profit < min_total_profit  then Trade.delete(t.id); trades[rt].delete(iid); next end
      if t.ppv < min_ppv              then Trade.delete(t.id); trades[rt].delete(iid); next end
      if t.suspicious                 then Trade.delete(t.id); trades[rt].delete(iid); next end
      
      ### flag unprofitable orders
      ### loop exits on (a) first unprofitable match or (b) ran out of bids or asks
      ### adjust so i_bid/i_ask point to first unprofitables (or N)
      if i_ask < asks.size and ask_vol < asks[i_ask].vol_rem then i_ask+=1 end   
      if i_bid < bids.size and bid_vol < bids[i_bid].vol_rem then i_bid+=1 end   
      hi_ask = asks[i_ask-1].price
      lo_bid = bids[i_bid-1].price
      ### ignore remaining (unprofitable)
      (i_ask...asks.size).each {|x| asks[x].ignore = true}
      (i_bid...bids.size).each {|x| bids[x].ignore = true}

      ### add to route totals
      trades[rt][Trade::K_SUM] += t
    end  ### each trade[rt][item]

    ### print routes
    puts "#{Station[from_stn].sname} -> #{Station[to_stn].sname}"
    totals = trades[rt][Trade::K_SUM]
    puts "=> $#{comma(totals.profit / 1_000_000.0)}M total, #{comma_i totals.size.to_i} m3"
    
    ### sort most profitable first
    sorted = trades[rt].values.sort do |a,b| b.profit <=> a.profit end 
    sorted.each do |t| 
      next if t.item == Trade::K_SUM 
      puts "  $#{'%.1f' % (t.profit / 1_000_000.0)}M, #{comma_i t.qty}x #{Item[t.item].name}, $#{'%.1f' % (t.ppv / 1_000.0)}K/m3"
      if t.profit > 100_000_000
        t.asks.each {|x| print "    #{x}"; puts x.ignore ? "" : " *"}
        puts "    ---"
        t.bids.each {|x| print "    #{x}"; puts x.ignore ? "" : " *"}
      end
    end
    end  ### trades[route]

    trades
end

class WheatDB
  def initialize(db_name)
    @db_name = db_name
    @db = Mymysql.new(db_name)
  end

  ### TODO: refactor $markets
  def export_orders(markets)
    Order.export_sql_table(@db, "orders")
  end
  
  def export_trades
    Trade.export_sql_table(@db, "trades")
  end
  
  def export_orders_trades
    start = Time.now
    ### wipe table
    @db.query("DELETE FROM `orders_trades`;")
    #puts "DELETE orders_trades #{Time.now - start}s"
    ### insert new rows
    q = SqlQueue.new_insert(@db, "orders_trades", "(order_id, trade_id)")
    Trade.each do |tid, t|
      t.asks.each do |o| q << "(#{o.id}, #{t.id})" end
      t.bids.each do |o| q << "(#{o.id}, #{t.id})" end
    end
    q.flush
    puts "INSERT orders_trades #{Time.now - start}s"
  end
end


Cache.restore
### main loop
while 1
  $timers[:main] = Time.now
  prefetch

  candidates = Evecentral.get_profitables # returns 3-tuple [from_stn, to_stn, item] 
  Crest.import_orders(candidates)         # populates $markets
  Marketlogs.import_orders                # populates $markets
  Cache.save

  trades = calc_trades(candidates)        # confirm trades against fresh market data

  ### export to DB: orders, trades, orders_trades
	STDERR.puts "------------\n"
  db2 = WheatDB.new("wheat_development")
  db2.export_orders($markets)
  db2.export_trades
  db2.export_orders_trades

  ### runtime stats
	STDERR.puts "------------\n"
  puts "full loop: #{Time.now - $timers[:main]}s"
  size = $counters[:size]; $counters[:size] = 0;
  #puts "cache size:   #{'%.1f' % (Cache.size/1_000_000.0)}MB, #{Cache.length} entries, #{'%.1f' % (Cache.biggest/1_000.0)}KB max"
	puts "overall downloaded: " + sprintf("%3.1f", size/1_000_000.0) + "MB"
	#puts "overall rate: " + sprintf("%.2f", ((size/1_000_000.0) / $timers[:get])) + "MB/s"
	#puts "overall time: " + $timers[:get].to_s + "s"; $timers[:get] = 0;
	puts "trades: #{$counters[:profitables]}" ; $counters[:profitables] = 0;
	puts "parse time:   " + ("%.1f" % ($timers[:parse] * 1_000.0)) + "ms"; $timers[:parse] = 0
  gets = $counters[:get]
	puts "GETs: #{gets}"
	puts "cache hits: #{Cache.hits} (#{'%.1f' % (Cache.hits.to_f * 100.0 / (Cache.hits + gets))}%)"; Cache.hits = 0
  $counters[:get] = 0;
	STDERR.puts "------------\n"

  ### repeat main loop every 60 sec
  repeat_rem = [60 - (Time.now - $timers[:main]), 0].max
	sleep repeat_rem
end ### while 1
