### install
require 'typhoeus'
require 'mysql2'
require 'clipboard'
require 'json'
### sys
require 'base64'
require 'stringio'
require 'pp'
require 'rexml/document'

### global variables
$timers = Hash.new(0)
$counters = Hash.new(0)
$debug_urls = []


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
  min = 3
  min += 1 if s[0] == '-' ### account for leading "-"
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


### Cache class -- (key x value x expiration time)
### defined
###   get(key)
###   set(key, val, time) -- vals are Typhoeus::Response.body
###   ttl() -- defines cache policy based on key (URL) type
class Cache
  CACHE_FILE = "#{__dir__}/cache.txt"
  @@cache = Hash.new([nil, nil])
  @@hits = 0

  ### ttl() -- defines cache policy based on URL type
  ### returns TTL, or nil if uncacheable
  ### not cached: eve-central.com, google.com
  def self.ttl(response)
    ret = nil
    url = response.effective_url
    hdr = response.headers 

    ### override for special cases
    if url.match("google")
      ret = nil
    elsif url.match("https://crest-tq\\.eveonline\\.com/market/([0-9]{8})/orders/(buy|sell)/\\?type=https://crest-tq\\.eveonline\\.com/types/([0-9]+)/") 
      ret = 20
    elsif url.match("/market/types/") or url.match("/regions/") or url.match("^https://crest-tq.eveonline.com/$")
      ret = 24*60*60    # 24 hrs
    elsif url.match("ConquerableStationList")
      #ret = 7*24*60*60  # 1 wk
      ret = 60*60  # 1 hr
    elsif url.match("eve-central.com") 
      ret = 60  ### only need prefetch to carryover to actual fetch; see Cache.get() 
    ### server-defined TTLs
    elsif url.match("https://login-tq.eveonline.com/oauth/token/") 
      ret = (JSON.parse(response.body))["expires_in"] 
    elsif hdr["Cache-Control"] and hdr["Cache-Control"].match("max-age=([0-9]+)")
      ret = $1.to_i
    end

    ret
  end

  SYS_AMARR       = 30002187
  SYS_JITA        = 30000142
  SYS_DODIXIE     = 30002659
  REG_PROVIDENCE  = 10000047
  REG_AMARR       = 10000043
  REG_JITA        = 10000002
  REG_DODIXIE     = 10000032

  def self.pretty(url)
    sys2sn = {
      SYS_AMARR       => "Amarr", 
      SYS_JITA        => "Jita", 
      SYS_DODIXIE     => "Dodixie",
      REG_PROVIDENCE  => "Providence",
      REG_AMARR       => "Amarr", 
      REG_JITA        => "Jita", 
      REG_DODIXIE     => "Dodixie",
    }
    sys_from = url.match("&fromt=([0-9]+)&")[1].to_i
    sys_to   = url.match("&to=([0-9]+)&")[1].to_i
    sys2sn[sys_from] + "-to-" + sys2sn[sys_to]
  end

  def self.set(k, v, exp)
    @@cache[k] = [v, exp]
  end

  def self.to_m(t)
    h = (t/3600).to_i
    t -= h*3600
    m = (t/60).to_i
    t -= m*60
    s = (t).to_i
    "#{'%02i'%h}:#{'%02i'%m}:#{'%02i'%s}"
  end
  
  def self.get(k, now=Time.now)
    v, exp = @@cache[k]
    if v and now < exp
      ### debug
      puts "    cache hit #{to_m(exp-now)} #{Crest.pretty k}" if k.match("^https://crest-tq.eveonline.com/?$")
      puts "    cache hit #{to_m(exp-now)} #{Crest.pretty k}" if k.match("https://login-tq.eveonline.com/oauth/token/")
      puts "    cache hit #{to_m(exp-now)} #{Crest.pretty k}" if k.match("https://crest-tq\\.eveonline\\.com/market/([0-9]{8})/orders/(buy|sell)/\\?type=https://crest-tq\\.eveonline\\.com/types/([0-9]+)/")
      puts "    cache hit #{to_m(exp-now)} #{Crest.pretty k}" if k.match("/market/types/$")
      puts "    cache hit #{to_m(exp-now)} #{Crest.pretty k}" if k.match("/market/types/.page=12")
      puts "    cache hit #{to_m(exp-now)} #{Crest.pretty k}" if k.match("/regions/$")
      puts "    cache hit #{to_m(exp-now)} #{Crest.pretty k}" if k.match("/regions/10000001")
      puts "    cache hit #{to_m(exp-now)} #{Crest.pretty k}" if k.match("eve-central.com")
      puts "    cache hit #{to_m(exp-now)} ConquerableStationList" if k.match("ConquerableStationList")

      ### special handling
      if k.match("https://login-tq.eveonline.com/oauth/token/") then
        ### access token contains a relative "expires_in", so we adjust it to actual TTL
        jhash = JSON.parse(v)
        jhash["expires_in"] = exp - now
        v = JSON.generate(jhash)
        @@cache[k] = [v, exp]
      elsif k.match("eve-central.com")
        ### eve-central.com cache expires after 1 get (prefetch only; no carryover between loops)
        @@cache.delete(k)
      end

      @@hits += 1      
      v
    else
      nil
    end
  end

  def self.save
    fname = CACHE_FILE
    now = Time.now
    @@cache.delete_if { |k, (v, exp)| now > exp }
    f = File.new(fname, "w")
    @@cache.each do |k, (v, exp)|
      next if k.match("eve-central.com")  ### do not save eve-central.com (~40MB) 
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
      @@cache[k] = [v, exp] unless exp < start
    end
    f.close
    puts "done #{Time.now-start}s"
  end
  
  def self.hits
    @@hits
  end
  def self.hits=(rval)
    @@hits
  end
  def self.size
    size = 0
    @@cache.each do |k, x| size += k.size + x[0].size + x[1].inspect.size end
    size
  end
  def self.length
    @@cache.length
  end
  def self.biggest
    k, biggest = @@cache.max_by { |x| x[0].size }
    v, exp = biggest
    k.size + v.size + exp.inspect.size
  end

end
Cache.restore


### CachedResponse - mimic Typhoeus::Response objects for cache hits
### problem: mget returns Response, cache returns Response.body
### TODO: refactor mget() to return Response.body instead of Response, so we don't have to do this
class CachedResponse
  attr_accessor :body, :effective_url, :total_time

  def initialize(url, body)
    @effective_url = url
    @body = body
    @total_time = 0
  end
end


### HTTPSource - wrapper class for HTTP get() and mget()
### subclass for each HTTP data source
class HTTPSource
	@@hydra = Typhoeus::Hydra.new

  URI_ROOT = 'https://crest-tq.eveonline.com/'
  URL_AUTH = "https://login-tq.eveonline.com/oauth/token/"
  RE_CREST_MARKET2 = "https://crest-tq\\.eveonline\\.com/market/([0-9]{8})/orders/(buy|sell)/\\?type=https://crest-tq\\.eveonline\\.com/types/([0-9]+)/"
  def self.pretty(href)
    x = href
    if x.match(RE_CREST_MARKET2) then x = "#{$1}.#{$3}.#{$2}" end
    if x.match "eve-central.com" then x = Evecentral.pretty x end
    if x.match(URL_AUTH) then x = "/oauth/token/" end
    x = x.gsub(URI_ROOT, "/")
    x
  end

  
  ### get() - returns Response.body
  def self.get(url, opt = {})
    urls = [url]
    opts = {url => opt}
    responses = mget(urls, opts)
    responses[0]
  end

  ### mget() - parallel GETs with optional per-URL callback, returns array of Typhoeus::Response
  ### arguments:
  ###   urls[]    - array of URLs to get
  ###   opts{}    - (optional) HTTP options, applied to all GETs
  ###   blocks{}  - (optional) response callbacks indexed by URL, *each* callback is optional
	def self.mget(urls, opts = {}, blocks = {})
    success = []
    now = Time.now.getutc
    puts "mget(#{urls.length}) -- #{pretty urls[0]}"
    start = nil
    output = nil
    while (not urls.empty?)
      requests = []
      first = true # debug
      urls.each do |url|
        #puts "GET -- #{url}"  ### debug
        opt = (opts[url])? opts[url] : Hash.new
        opt[:ssl_verifypeer]  ||= false
        #opt[:verbose] = true
        #if url.match(Crest.re_market) then opt[:headers]["Accept"] = "application/vnd.ccp.eve.MarketOrderCollection-v1+json; charset=utf-8" end
        if url.match(RE_CREST_MARKET2) then opt[:headers].delete("Accept") end
        
        body = Cache.get(url, now)
        if body
          response2 = CachedResponse.new(url, body)
          blocks[url].call(response2) if blocks[url]
          success << response2
          next
        end
        
        request = Typhoeus::Request.new(url, opt)
        request.on_complete do |response|
          output = ">>> #{pretty url} at #{Time.now - start}" unless url.match("/regions/[0-9]{8}") #or url.match(RE_CREST_MARKET2)  
          #puts ">>> #{pretty url} at #{Time.now - start}" unless url.match(RE_CREST_MARKET2) #or url.match("/regions/[0-9]{8}")
          if response.success?
            # invoke callback
            blocks[url].call(response) if blocks[url]
            success << response
            ### cache
            if (ttl = Cache.ttl(response)) then Cache.set(url, response.body, now + ttl) end
          elsif response.timed_out?
            puts " => HTTP request timeout: #{pretty url}"
            urls << url   ### requeue on timeout
          elsif response.code == 0
            puts(response.return_message)
          else
            puts " => HTTP request failed: #{response.code} #{pretty url}" unless url.match(Crest.re_market)
            if response.code == 502 then urls << url end    ### requeue on 502 (Crest server bandwidth exceeded)
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
  end  ### mget()

  ### mget_ary() -- mget() with arguments as array of [url, opt, block] tuples
  def self.mget_ary(args)
    urls = []
    opts = {}
    callbacks = {}
    args.each do |x|
      url, opt, callback = x
      urls << url
      opts[url] = opt if opt
      callbacks[url] = callback if callback
    end
    mget(urls, opts, callbacks)
  end
end



### Mymysql class - wrapper for mysql default options
class Mymysql < Mysql2::Client
  ### mysql option defaults
  @@mysql_opts = {}
  @@mysql_opts[:host]     = "127.0.0.1"
  #@@mysql_opts[:host]     = "plugh.asuscomm.com"
  #@@mysql_opts[:host]     = "69.181.214.54"
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
###   new_insert  -- new INSERT query
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
    @done = {}
    ### query max packet size
    r = @db.query("SHOW variables LIKE 'max_allowed_packet';")
    r.each {|row| if row["Variable_name"]=='max_allowed_packet' then @sql_max = row["Value"].to_i - SQL_MAX_ADJUST; break end}
  end

  def self.new_insert(db, table, field_names)
    sql_base = "INSERT INTO `#{table}` \n  #{field_names} \nVALUES "
    SqlQueue.new(db, sql_base)
  end
  
  def <<(vals)
    if (@sql.size + SQL_HDR.size + vals.size + SQL_EOF.size) > @sql_max then flush end
    @sql << SQL_HDR + vals + SQL_EOL unless @done[vals]
    @done[vals] = true
  end
  
  def flush
    @sql.chomp!(SQL_EOL)
    @sql << SQL_EOF
    #puts "Sql.flush #{@sql.size} #{'%+i' % (@sql.size - @sql_max)}"
    @db.query(@sql)
    @sql = @sql_base.dup  # reset
  end
end


### EveDataCollection -- base wrapper class for Eve game data
### objects instantiated by hash of (instance variable) name-value pairs
### subclassed for each data type (Item, Region, Station, etc)
### an instance is a single data element
### class is a hash of all instances indexed by ID and name, eg...
###   Item[id]
###   Region[name]
###   Station.each
class EveDataCollection
  @@hits = 0
  @data = {}
  def initialize(fields={})
    fields.each { |k,v| instance_variable_set("@#{k}", v) }
    self.class[@id] = self
    self.class[@name] = self if @name
  end
  
  def self.[]=(id, rval)
    @@hits += 1
    @data[id] = rval
  end
  
  def self.[](id)
    @@hits += 1
    @data[id]
  end
  
  def self.each
    @data.values.uniq.each { |v| yield(v.id, v) }  ### filter out duplicate indices
  end

  def self.delete(id)
    @data.delete(id)
  end
  
  def self.hits
    @@hits
  end

  ### adds <EveData>_id references to other EveData types (Region, Station, etc.)
  ### usage: "ref_accessor :region_id" adds methods for region_id(r) and region()
  def self.ref_accessor(*id_syms)
    id_syms.each do |id_sym|
      ### id accessor
      attr_accessor id_sym                              ### :region_id

      ### object accessor (dynamically defined)
      id_name = id_sym.to_s                             ### "region_id"
      obj_name = id_name.chomp("_id")                   ### "region"
      obj_klass = Object.const_get(obj_name.capitalize) ### "Region"
      class_eval("def #{obj_name}; @#{obj_name} ||= #{obj_klass.to_s}[@#{id_name}]; @#{obj_name} end")
      ### ex: "def Station.region { Region[@region_id] }"
      #define_method(obj_name) do obj_klass[instance_variable_get("@#{id_name}")] end ### safer than below?
      ### alternative implementation
      #class_eval("def #{obj_name};#{ref_klass.to_s}[@#{id_name}];end")              ### this also works
      #puts "ref_accessor(#{self.to_s}) :#{id_name}, :#{obj_name}()"
    end
  end
end


### ImportSql module -- import DB table into EveDataCollection class datastore
### defined
###   import_sql()   -- "fieldmap" arg is src (DB field) => dst (instance variable) name mappings, for each field imported (class method)
###   import_sdd()   -- import from Eve Static Data Dump
module ImportSql
  ### begin idiom for module class methods
  def self.included(base)
    base.extend(ClassMethods)
  end
  module ClassMethods
  ### end idiom
  
    def import_sql(db_name, db_table, fieldmap)
      print "loading DB \"#{DB_SDD}.#{db_table}\"..."
      _start = Time.now
      ### query DB
      db = Mymysql.new(db_name)
      db_fields = fieldmap.keys.join(", ")
      sql = "SELECT #{db_fields} FROM #{db_table}"
      results = db.query(sql)
      results.each do |src_row|
        ### map fields and instantiate objects
        ivars = {}
        fieldmap.each { |k_src, k_dst| ivars[k_dst] = src_row[k_src] }
        x = self.new(ivars) # instantiate by hash
      end
      puts "done (#{sz @data}, #{Time.now - _start} sec)"
    end

    DB_SDD = "evesdd_galatea"
    def import_sdd(db_table, fieldmap) 
      import_sql(DB_SDD, db_table, fieldmap)
    end
  end
end


### ExportSql interface -- export EveDataCollection class datastore to DB table
### defined
###   export_sql_table() -- exports class datastore to DB table (class method)
### requires
###   export_sql_hdr()   -- list of fieldnames for INSERT query (class method)
###   export_sql()       -- convert instance to list of values for INSERT query
module ExportSql
  def export_sql
    raise NoMethodError # virtual instance method
  end

  ### begin idiom for module class methods
  def self.included(base)
    base.extend(ClassMethods)
  end
  module ClassMethods
  ### end idiom
  
    def export_sql_hdr
      raise NoMethodError # virtual class method
    end

    def export_sql_table(db, db_table, filter = nil)
      t0 = Time.now
      ### wipe table
      db.query("TRUNCATE `#{db_table}`;")  ### much faster than "DELETE"
      t_del = Time.now - t0
      ### insert new rows
      q = SqlQueue.new_insert(db, db_table, self.export_sql_hdr)
      n = 0
      self.each { |id, x| if !filter or filter.call(x) then q << x.export_sql ; n+=1 end }
      t_str = Time.now - t0 - t_del
      q.flush
      t_all = Time.now - t0
      #puts "INSERT #{db_table}"
      puts "INSERT #{db_table} x#{n} -- #{'%.3f'%t_all}s (del #{'%.3f'%t_del}s, str #{'%.3f'%t_str}s, ins #{'%.3f'%(t_all-t_del)}s)"
    end
  end
end


### Item class - wrapper class for Eve item data 
### fields
###   id
###   name
###   volume
###   href
class Item < EveDataCollection
  attr_accessor :id, :name, :volume, :href

  @data = {}     ### Item[] datastore

  include ImportSql
  @sdd_table     = "invtypes"
  @sdd_fieldmap  = {"typeID"=>:id, "typeName"=>:name, "volume"=>:volume}
  import_sdd(@sdd_table, @sdd_fieldmap)
end


### Region: wrapper class for Eve region data (REGION > System > Station)
### fields
###   id
###   name
###   href          Crest URI for region
###   buy_href      Crest URI base for market buy orders
###   sell_href     Crest URI base for market sell orders
class Region < EveDataCollection
  attr_accessor :id, :name, :href, :buy_href, :sell_href
  
  @data = {}     ### Region[] datastore

  include ImportSql
  @sdd_table     = "mapregions"
  @sdd_fieldmap  = {"regionID"=>:id, "regionName"=>:name}  
  import_sdd(@sdd_table, @sdd_fieldmap)
end


### System class - wrapper class for Eve solar system data (Region > SYSTEM > Station)
### fields
###   id
###   name
###   region_id
class System < EveDataCollection
  attr_accessor :id, :name
  ref_accessor  :region_id

  @data = {}     ### System[] datastore

  include ImportSql
  @sdd_table     = "mapsolarsystems"
  @sdd_fieldmap  = {"solarSystemID"=>:id, "solarSystemName"=>:name, "regionID"=>:region_id}
  import_sdd(@sdd_table, @sdd_fieldmap)
end


### Station class - wrapper class for Eve station data (Region > System > STATION)
### fields
###   id
###   name
###   href
###   region_id
###   system_id (solar system)
class Station < EveDataCollection
  attr_accessor :id, :name, :sname, :href
  ref_accessor  :region_id, :system_id

  @data = {}     ### Station[] datastore

  STN_AMARR       = 60008494
  STN_JITA        = 60003760
  STN_DODIXIE     = 60011866
  @@nicknames = {
    STN_AMARR   => "*Amarr*",
    STN_JITA    => "*Jita*",
    STN_DODIXIE => "*Dodixie*",
  }
  def initialize(fields={})
    super(fields)
    @sname = @@nicknames[@id] ? @@nicknames[@id] : @name  ### shortname
  end

  ### two import sources: (i) Static Data Dump (SQL) and (ii) Conquerable Station List (XML)
  include ImportSql
    @sdd_table     = "stastations"
    @sdd_fieldmap  = {"stationID"=>:id, "stationName"=>:name, "regionID"=>:region_id, "solarSystemID"=>:system_id}
  import_sdd(@sdd_table, @sdd_fieldmap)
  def self.import_csl
    url = "https://api.eveonline.com/eve/ConquerableStationList.xml.aspx"
    raw = (HTTPSource.get(url)).body
    xml = REXML::Document.new(raw)
    xml.elements.each("eveapi/result/rowset/row") do |e| 
      stn_id = e.attributes["stationID"].to_i
      stn_name = e.attributes["stationName"]
      system_id = e.attributes["solarSystemID"].to_i
      region_id = System[system_id].region_id
      next if Station[stn_id]  # we already know about this station
      s = Station.new({id:stn_id, name:stn_name, system_id:system_id, region_id:region_id})
    end
  end
  import_csl
  
  def Station.hub?(stn_id)
    lookup = {
      STN_AMARR   => true,
      STN_JITA    => true,
      STN_DODIXIE => true,
    }
    lookup[stn_id] 
  end
  def hub?
    Station.hub?(@id)
  end
end




### Item test cases
id = 28272
vol = 10.0
name = "'Augmented' Hammerhead"
assert_eq(Item[id].name, name,  "item DB: id lookup failed")
assert_eq(Item[id].volume, vol, "item DB: volume info failed")
assert_eq(Item[name].id, id,    "item DB: name lookup failed")
### Station test cases
id = 60000004
name = "Muvolailen X - Moon 3 - CBD Corporation Storage"
region = 10000033
assert_eq(Station[id].name, name,         "station DB: id lookup failed")
assert_eq(Station[id].region_id, region,  "station DB: region info failed")
assert_eq(Station[name].id, id,           "station DB: name lookup failed")
### System test cases
id = 30000142
name = "Jita"
region = 10000002
assert_eq(System[id].name, name,        "system DB: id lookup failed")
assert_eq(System[name].id,   id,        "system DB: name lookup failed")
assert_eq(System[id].region.id, region, "system DB: region lookup failed")

### Region test cases
id = 10000002
name = "The Forge"
assert_eq(Region[id].name, name, "region DB: name info failed")
assert_eq(Region[name].id, id, "region DB: name lookup failed")



### Order class - EVE market order (from Crest or Marketlogs)
class Order < EveDataCollection
  ref_accessor  :item_id, :station_id, :region_id
  attr_accessor :id, :buy, :price, \
    :vol_rem, :vol_orig, :vol_min, \
    :range, :duration, :issued, :sampled, :ignore
  #def item()    Item[@item_id]       end
  #def station() Station[@station_id] end
  #def region()  Region[@region_id]   end
  
  @data = {}    ### Order[] datastore
 
  ###
  ### import/export conversions
  ###
  
  ### import_crest() - converts Crest market order "item" into Order object
  def self.import_crest_item(citem, sampled = Time.new(0))
    Order.new({
      :id         => citem["id"],
      :item_id    => citem["type"]["id"],
      :buy        => citem["buy"],
      :price      => citem["price"].to_f,
      :vol_rem    => citem["volume"],
      :vol_orig   => citem["volumeEntered"],
      :vol_min    => citem["minVolume"],
      :station_id => citem["location"]["id"],
      :range      => citem["range"],
      :region_id  => Station[citem["location"]["id"]].region_id,
      :issued     => DateTime.strptime(citem["issued"], '%Y-%m-%dT%H:%M:%S').to_time.getutc,
      :duration   => citem["duration"],
      :sampled    => sampled,
    })
  end

  ### import_marketlog_line() - converts marketlog line into Order object
  def self.import_marketlog_line(line, sample_time=Time.new(0))
    price, vol_rem, item_id, range, order_id, vol_orig, vol_min, \
      buy, issued, duration, station_id, region_id, system_id, jumps \
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

  def to_s
    (buy ? 'bid' : 'ask') + " #{'%17s' % (comma(price, '$'))} #{'%7s' % (comma_i(vol_rem, 'x'))}" + (vol_min > 1 ? " (_#{vol_min})" : "")
  end

  include ExportSql
  def self.export_sql_hdr
    "(order_id, station_id, region_id, item_id, buy, price, price_str, vol, vol_str, ignored)"
  end
  def export_sql
    "(#{id}, #{station_id}, #{region_id}, #{item_id}, #{buy}, #{price}, '#{comma(price, '$')}', #{vol_rem}, '#{comma_i vol_rem}', #{ignore ? 'true' : 'false'})"
  end
end



### TODO: store Market[] data in Region[r].buy_orders[item]

### Market[region][item] => Market
###   buy_orders = Orders[]
###   sell_orders = Orders[]
###   buy_sampled = Time
###   sell_sampled = Time
### NOTE: need to be able to delete all orders for a particular region-item pair (when fresher data)
class Market
  attr_accessor :region_id, :item_id, :buy_orders, :sell_orders, :buy_sampled, :sell_sampled
  def region() Region[@region_id] end
  def item()   Item[@item_id]     end

  ### autovivify Market[region][item]
  @data = Hash.new {|h1,r| h1[r] = Hash.new {|h2,i| h2[i] = Market.new(r.id, i.id)} } 
  
  def initialize(region_id, item_id)
    @region_id = region_id
    @item_id = item_id
    @buy_orders = []
    @sell_orders = []
    @buy_sampled = Time.new(0)
    @sell_sampled = Time.new(0)
  end  

  def self.[](region)
    @data[region]
  end
  
  ### each_reg() -- each region
  ###   blk(reg, Market[reg][])
  def self.each_reg
    @data.each { |r, mkt_by_item| yield(r, mkt_by_item) }
  end  
  ### each() -- each market (region-item) 
  ###   blk(reg, item, Market)
  def self.each
    @data.each_key do |r| 
      @data[r].each_key do |i|
        yield(r, i, @data[r][i]) 
      end
    end
  end  
 
end




### Evecentral - wrapper class for pulling tradefinder results from eve-central.com
### public interface:
###   get_profitables() => returns array of joined strings "<from_stn_id>:<to_stn_id>:<item_id>"
class Evecentral < HTTPSource
  URL_BASE = "https://eve-central.com/home/tradefind_display.html"
  MAX_DELAY       = 48
  MIN_PROFIT      = 100000
  MAX_SPACE       = 8967
  MAX_RESULTS     = 99999
  
  SYS_AMARR       = 30002187
  SYS_JITA        = 30000142
  SYS_DODIXIE     = 30002659
  REG_PROVIDENCE  = 10000047
  REG_AMARR       = 10000043
  REG_JITA        = 10000002
  REG_DODIXIE     = 10000032
  
  def self.pretty(url)
    if url.match(URL_BASE) then
      sys2sn = {
        SYS_AMARR       => "Amarr", 
        SYS_JITA        => "Jita", 
        SYS_DODIXIE     => "Dodixie",
        REG_PROVIDENCE  => "Providence",
        REG_AMARR       => "Amarr",
        REG_JITA        => "Jita",
        REG_DODIXIE     => "Dodixie",
      }
      sys_from = url.match("&fromt=([0-9]+)&")[1].to_i
      sys_to   = url.match("&to=([0-9]+)&")[1].to_i
      sys2sn[sys_from] + "-to-" + sys2sn[sys_to]
    else
      super(url)
    end
  end

  ### URLs for eve-central.com
  def self.url_base
    URL_BASE
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
  \<td\>\<b\>From:\<\/b\> (?<from_stn_name>[^<]+)\<\/td\>
  \<td\>\<b\>To:\<\/b\> (?<to_stn_name>[^<]+) \<\/td\>
  \<td\>\<b\>Jumps:\<\/b\> [0-9]+\<\/td\>
\<\/tr\>
\<tr\>
  \<td\>\<b\>Type:\<\/b\> \<a href=\"(?<item_url_suffix>quicklook.html\?typeid=(?<item_id>[0-9]+))\"\>(?<item_name>[^<]+)\<\/a\>\<\/td\>
  \<td\>\<b\>Selling:\<\/b\> (?<ask_price>[,.0-9]+) ISK\<\/td\>
  \<td\>\<b\>Buying:\<\/b\> (?<bid_price>[,.0-9]+) ISK\<\/td\>
\<\/tr\>

\<tr\>
  \<td\>\<b\>Per-unit profit:\<\/b\> [,.0-9]+ ISK\<\/td\>
  \<td\>\<b\>Units tradeable:\<\/b\> [,0-9]+ \((?<ask_vol>[,0-9]+) -\&gt; (?<bid_vol>[,0-9]+)\)\<\/td\>
  \<td\>\&nbsp;\<\/td\>
\<\/tr\>
\<tr\>
  \<td\>\<b\>\<i\>Potential profit\<\/i\>\<\/b\>: [,.0-9]+ ISK \<\/td\>
  \<td\>\<b\>\<i\>Profit per trip:\<\/i\>\<\/b\>: [,.0-9]+ ISK\<\/td\>
  \<td\>\<b\>\<i\>Profit per jump\<\/i\>\<\/b\>: [,.0-9]+\<\/td\>
\<\/tr\>
\<tr\>\<td\>\&nbsp;\<\/td\>\<\/tr\>

' ### end REGEXP_PROFITABLE
### match variables
###   from_stn_name, ask_price, ask_vol
###   to_stn_name, bid_price, bid_vol
###   item_id, item_name, item_url_suffix

  def self.import_html(html)
    
    ### parse preamble
    re_preamble = Regexp.new(REGEXP_PREAMBLE)
    m = re_preamble.match(html)
    nominal = m[1]  ### number of profitables according to eve-central

    ### parse body (multiple blocks)
    trades = {}
    re_block = Regexp.new(REGEXP_PROFITABLE, Regexp::MULTILINE)
    start_parse = Time.new
    n = $counters[:profitables]
    while (m = re_block.match(m.post_match)) do
      $counters[:profitables] += 1
      assert(Station[m[:from_stn_name]], "Evecentral.import_html(): station not found #{m[:from_stn_name]}")
      assert(Station[m[:to_stn_name]],   "Evecentral.import_html(): station not found #{m[:to_stn_name]}")
      from_stn = Station[m[:from_stn_name]]
      to_stn   = Station[m[:to_stn_name]]
      item  = Item[m[:item_id].to_i]
      #next if !from.hub? || !to.hub?  ### hub stations only
      trade = [from_stn, to_stn, item]
      trades[trade] = true  ### filter out dups
    end 
    $timers[:parse] += Time.new - start_parse
    #puts ("%5i" % ($counters[:profitables] - n)) + " routes parsed"
    #puts "  parsing " + ("%0.2f" % (html.size / 1_000_000.0)) + "MB"

    trades.keys
  end

  def self.url_gen(from, to, qtype)
    params = \
      "?set=1" + \
      "&fromt=#{from}" + \
      "&to=#{to}" + \
      "&qtype=#{qtype}" + \
      "&age=#{MAX_DELAY}" + \
      "&minprofit=#{MIN_PROFIT}" + \
      "&size=#{MAX_SPACE}" + \
      "&limit=#{MAX_RESULTS}" + \
      "&sort=sprofit" + \
      "&prefer_sec=0"
    url = URL_BASE + params
  end
  def self.url_by_sys(from_sys, to_sys)
    url_gen(from_sys, to_sys, "Systems")
  end
    def self.url_by_reg(from_r, to_r)
    url_gen(from_r, to_r, "Regions")
  end
  
  ### all route permutations
  def self.urls_all_routes_by_sys(x)
    urls = []
    x.each do |from|
      x.each do |to| 
        urls << url_by_sys(from, to) unless from == to 
      end
    end
    urls
  end
  def self.urls_all_routes_by_reg(x)
    urls = []
    x.each do |from|
      x.each do |to| 
        urls << url_by_reg(from, to) unless from == to 
      end
    end
    urls
  end
  
  ### get_profitables(): get + parse list of profitable trades from evecentral
  ### return value: array of trade IDs (from_stn_id:to_stn_id:item_id)
  def self.get_profitables
    ### get all route permutations between regions [Amarr, Jita, Dodixie, Providence]
    #hubs = [SYS_AMARR, SYS_JITA, SYS_DODIXIE]
    #urls = urls_all_routes_by_sys(hubs)
    r = [System[SYS_AMARR].region_id, System[SYS_JITA].region_id, System[SYS_DODIXIE].region_id, Evecentral::REG_PROVIDENCE]
    urls = urls_all_routes_by_reg(r)

    ### get html from eve-central.com
    responses = mget(urls)
    $timers[:get] += (responses.max_by {|x| x.total_time}).total_time

    ### parse html
    ret = []
    responses.each do |r|
      puts pretty(r.effective_url) + " parsing #{'%.2f'%(r.body.size/1_000_000.0)}M"
      $counters[:size] += r.body.size
      html = r.body
      profitables = import_html(html)
      ret.concat(profitables)
    end
    ret
  end
end ### class Evecentral




### Crest - wrapper class for pulling market data from CCP's authenticated CREST API
### public interface
###   import_orders()
class Crest < HTTPSource
  URI_ROOT = 'https://crest-tq.eveonline.com/'
  URI_PATH_ITEMTYPES  = 'marketTypes'   ### "/marketTypes/"
  URI_PATH_REGIONS    = 'regions'       ### "/regions/"
  URL_AUTH = "https://login-tq.eveonline.com/oauth/token/"
  RE_MARKET = "https://crest-tq\\.eveonline\\.com/market/[0-9]{8}/orders/(buy|sell)/\\?type=https://crest-tq\\.eveonline\\.com/types/[0-9]+/"
 
  @@client_id = 'e5a122800a134da2ad4b0e01664b627b'  ### app's ID (secret key is passed by command line)
  @@client_rtoken = '2-X4wdpBzGMTkpy8bdk0jg-gi6YfwVWyp_G9PbJtAME1'  ### app's refresh token

  @@crest_http_opts = Hash.new
  @@crest_http_opts[:followlocation]   = true
  @@crest_http_opts[:ssl_verifypeer]   = true
  @@crest_http_opts[:ssl_cipher_list]  = "TLSv1"
  @@crest_http_opts[:cainfo]           = "#{__dir__}/cacert.pem"
  
  @@root_hrefs = nil
  @@_static_loaded = false
  
  def Crest.re_market
    RE_MARKET
  end
  def Crest.re_auth
    URL_AUTH
  end
  def Crest.re_root
    URI_ROOT
  end
  
  def Crest._client_secret
    ARGV.each { |arg| if arg.match("--client-secret=(.*)") then return $1 end }
    raise "client secret key not provided"
  end
  
  def Crest._client_id_encoded
    Base64.strict_encode64(@@client_id + ":" + _client_secret)
  end
  
  ### get access token
  def Crest.refresh_atoken_opts
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
  def Crest.set_atoken(response)
    json = JSON.parse(response.body)
    @@access_token = json["access_token"]

    now = Time.now
    @@access_expire = now + json["expires_in"]
    #puts "access_token exp #{@@access_expire - Time.now}s = #{@@access_token}"
  end
  def Crest._refresh_atoken
    opts = refresh_atoken_opts
    r = get(URL_AUTH, opts)
    set_atoken(r)
  end  
  def Crest.get_atoken
    if (!defined?(@@access_token)) || (Time.now > @@access_expire) then _refresh_atoken end
    @@access_token
  end

  def Crest.http_opts
    @@crest_http_opts
  end
  
  ### get_href() - returns href response as array of Responses
  ###   handles multi-page responses invisibly
  def Crest.get_href(href)
    ### HTTP options
    opt = @@crest_http_opts.dup
    headers = {}
    headers["Authorization"] = "Bearer #{get_atoken}"
    #headers["Accept"] = "application/vnd.ccp.eve.Api-v3+json"
    opt[:headers] = headers 
    opt[:method] = :get
    opts = Hash.new(opt)
    
    p1_response = get(href, opt)
    ret = [p1_response]

    ### check for multi-page response
    ### technically we're supposed to iterate through each "next" link one at a time (for marketTypes takes 50s)
    ### instead we do a parallel get of pages 2-N (much faster)
    p1_json = JSON.parse(p1_response.body)
    if (p1_json["pageCount"] and p1_json["pageCount"] > 1) then
      ### parallel multi-get: hack URIs for pages 2-n
      ### href format example "https://crest-tq.eveonline.com/market/types/?page=2"
      n_pages = p1_json["pageCount"]
      p2_href = p1_json["next"]["href"]
      m = p2_href.match("page=2")
      hrefs = (2..n_pages).map {|x| m.pre_match + "page=" + x.to_s + m.post_match}
      responses_2_to_n = mget(hrefs, opts) 
      ret.concat(responses_2_to_n)
    end

    ret
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
  def Crest.get_href_as_items(href)
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
  def Crest.get_hrefs_each(hrefs, blocks)
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

  

  def Crest._get_root
    r = get_href(URI_ROOT)[0]
    JSON.parse(r.body)    
  end
  def Crest._href_item_types
    @@root_hrefs ||= _get_root
    @@root_hrefs[URI_PATH_ITEMTYPES]["href"]
  end
  def Crest._href_regions
    @@root_hrefs ||= _get_root
    @@root_hrefs[URI_PATH_REGIONS]["href"]
  end


  RE_CREST_MARKET2 = "https://crest-tq\\.eveonline\\.com/market/([0-9]{8})/orders/(buy|sell)/\\?type=https://crest-tq\\.eveonline\\.com/types/([0-9]+)/"
  def Crest.pretty(href)
    x = href
    if x.match(RE_CREST_MARKET2) then x = "#{$1}.#{$3}.#{$2}" end
    if x.match "eve-central.com" then x = Evecentral.pretty x end
    if x.match(URL_AUTH) then x = "/oauth/token/" end
    x = x.gsub(URI_ROOT, "/")
    x
  end
  
 
  ### load_regions() - populate Region[].href from Crest
  def Crest._load_region_hrefs
    items = get_href_as_items(_href_regions)
    items.each { |i| Region[i["name"]].href = i["href"] }
  end
  ### load_markets() - fetch values for Region[].buy_href and .sell_href 
  ### prereq - assumes Region[].href is already defined
  ### NOTE: ~100 GETs 
  def Crest._load_market_hrefs
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
  ### NOTE: Crest might have "new" items not seen in import
  def Crest._load_itemtype_hrefs
    items = get_href_as_items(_href_item_types)
    items.each do |i|
      id = i["type"]["id"]
      Item[id] ||= Item.new({id:id, name:i["type"]["name"], volume: 1.0})
      Item[id].href = i["type"]["href"]
      assert_eq(Item[id].name, i["type"]["name"], "item DB name mismatch")  ### test
    end
  end
  ### _load_static_data() - fetch hrefs for Region and Item (prereq to fetching market orders)
  def Crest._load_static_data
    _load_region_hrefs    # Region[].href
    _load_market_hrefs    # Region[].buy_href, .sell_href
    _load_itemtype_hrefs  # Item[].href
    @@_static_loaded = true
  end
  
  ### get_market_orders() - returns href and callback for market order fetch
  ### branched for two flavors (buy and sell)
  def Crest._get_market_orders(r, i, now, buy)
    _load_static_data unless @@_static_loaded
    m = Market[r][i]
    sampled = buy ? m.buy_sampled : m.sell_sampled
    if now > sampled 
      href = (buy ? r.buy_href : r.sell_href) + "?type=" + i.href
      proc = Proc.new do |response|
        items = (JSON.parse(response.body))["items"]
        $counters[:size] += response.body.size
        orders = items.map { |x| Order.import_crest_item(x, sampled) }
        m.instance_variable_set(buy ? '@buy_orders' : '@sell_orders', orders)
        m.instance_variable_set(buy ? '@buy_sampled' : '@sell_sampled', now)
        #puts ">>> crest #{buy ? 'buy ' : 'sell'} #{'%-11s' % r.name}::#{i.name}  #{now}"
      end
      [href, proc]
    else
      [nil, nil]
    end
  end
  def self._get_market_sell_orders(from, item, now)
    _get_market_orders(from, item, now, false)
  end
  def self._get_market_buy_orders(to, item, now)
    _get_market_orders(to, item, now, true)
  end

  ### import_orders() - fetch market orders; populate Market[][].buy_orders, .sell_orders
  def Crest.import_orders(candidates)
    #puts "Crest.import_orders()"
    hrefs = []
    procs = {}
    now = Time.now.getutc
    candidates.each do |t|
      from_stn, to_stn, item = t
      from_r = from_stn.region
      to_r   =   to_stn.region
      
      ### get sell orders
      href, proc = _get_market_sell_orders(from_r, item, now)
      hrefs << href if href
      procs[href] = proc if href
      ### get buy orders
      href, proc = _get_market_buy_orders(to_r, item, now)
      hrefs << href unless href==nil
      procs[href] = proc unless href==nil
    end
    hrefs.uniq!
    responses = Crest.get_hrefs_each(hrefs, procs) ### multi-get
  end
  
end ### class Crest


class Marketlogs < HTTPSource
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
  def Marketlogs.file_attrs(fname)
    m = fname.match(EXPORT_FNAME_REGEXP)
    assert(m, "marketlog filename #{fname}")
    region_id = Region[m[:region]].id
    item_name = @@fname2name[m[:item]] || m[:item]  ### some item filenames are modified
    item_id = Item[item_name].id
    sample_time = Time.utc(m[:yr], m[:mo], m[:dy], m[:hh], m[:mm], m[:ss]) + EXPORT_TRUMP_TIME
    
    [region_id, item_id, sample_time]
  end

  def Marketlogs.import_file(fname)
    region_id, item_id, sample_time = file_attrs(fname)
    file = File.new(fname, "r")
    file.gets   ### header line
    #header = "price,volRemaining,typeID,range,orderID,volEntered,minVolume,bid,issueDate,duration,stationID,regionID,solarSystemID,jumps,"
    orders = []
    while line = file.gets
      orders << Order.import_marketlog_line(line, sample_time)
    end
    file.close
    orders
  end
  ### test
  #assert_eq(item_id,   order.item_id,   "Marketlogs.import_file() item mismatch")
  #assert_eq(region_id, order.region_id, "Marketlogs.import_file() region mismatch")
  #assert_eq(region_id, Station[order.station_id].region_id, "Marketlogs.import_file() station-region mismatch")      

  EXPORT_DIR = "C:\\Users\\csserra\\Documents\\EVE\\logs\\Marketlogs\\"
  MARKETLOG_DIR_OSX = "/Users/csserra/Library/Application Support/EVE Online/p_drive/User/My Documents/EVE/logs/Marketlogs"
  EXPORT_TTL = 3*24*60*60 # delete after 3 days
  
  ### purge_old() - delete older versions of marketlog files
  def Marketlogs.purge_old
    dir = Dir.new(EXPORT_DIR)
    latest_fnames = {}
    latest_times = {}
    dir.each do |fname_short|
      next if fname_short == "." or fname_short == ".."
      me_fname = EXPORT_DIR + fname_short
      region_id, item_id, me_sampled = Marketlogs.file_attrs(me_fname)
      mkt_id = [region_id, item_id]

      if Time.now - me_sampled > EXPORT_TTL     # past expiration so delete me
          puts "!!! deleting marketlog expired #{me_fname}"
          File.delete(me_fname)
          next
      end
      if latest_fnames[mkt_id]
        if me_sampled < latest_times[mkt_id]      # less recent so delete me
          puts "!!! deleting marketlog overtaken #{me_fname}"
          File.delete(me_fname)
          next  ### current file deleted
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
  ### populates Market[][]
  ### call this every 2 sec
  def Marketlogs.import_orders
    if not Dir.exists?(EXPORT_DIR) then puts "Marketlogs: directory not found #{EXPORT_DIR}"; return false; end
    _start = Time.now
    #puts "Marketlogs.import_orders()"
    purge_old
    update = false
    dir = Dir.new(EXPORT_DIR)
    dir.each do |fname_short|
      next if fname_short == "." or fname_short == ".."   ### skip system dirs

      fname = EXPORT_DIR + fname_short
      region_id, item_id, f_sampled = Marketlogs.file_attrs(fname)
      
      mkt = Market[Region[region_id]][Item[item_id]]
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
    puts "marketlogs imported #{'%i'%((Time.now - _start)*1_000)}ms"
    update
  end

end ### class Marketlogs



###
### prefetch
###

def prefetch(upto = 1024)
  prefetch_start = Time.now
  puts ">>> prefetch.start <<<"
  ###
  ### Phase 1
  ###
  mget_q = []   ### aggregate mget requests
  ### Phase 1: Crest access token
  url = "https://login-tq.eveonline.com/oauth/token/"
  mget_q << [url, Crest.refresh_atoken_opts, Crest.method(:set_atoken)]
  ### Phase 1: Crest root
  mget_q << ['https://crest-tq.eveonline.com/', nil, nil]
  mget_q << ['https://api.eveonline.com/eve/ConquerableStationList.xml.aspx', nil, nil]
  
  #mget_q << ['http://www.google.com', nil, nil]
  #mget_q << ['https://eve-central.com/home/tradefind_display.html?set=1&fromt=30002187&to=30000142&qtype=Systems&age=#{Evecentral::MAX_DELAY}&minprofit=#{Evecentral::MIN_PROFIT}&size=#{Evecentral::MAX_SPACE}&limit=#{Evecentral::MAX_RESULTS}&sort=sprofit&prefer_sec=0', nil, nil]
  ### Phase 1: mget
  Crest.mget_ary(mget_q)
  if upto == 1 then return end
  
  ###
  ### Phase 2
  ###
  mget_q = []
  ### Phase 2: eve-central profitables
  urls1 = [ ### 3 systems
  "https://eve-central.com/home/tradefind_display.html?set=1&fromt=30002187&to=30000142&qtype=Systems&age=#{Evecentral::MAX_DELAY}&minprofit=#{Evecentral::MIN_PROFIT}&size=#{Evecentral::MAX_SPACE}&limit=#{Evecentral::MAX_RESULTS}&sort=sprofit&prefer_sec=0",
  "https://eve-central.com/home/tradefind_display.html?set=1&fromt=30002187&to=30002659&qtype=Systems&age=#{Evecentral::MAX_DELAY}&minprofit=#{Evecentral::MIN_PROFIT}&size=#{Evecentral::MAX_SPACE}&limit=#{Evecentral::MAX_RESULTS}&sort=sprofit&prefer_sec=0",
  "https://eve-central.com/home/tradefind_display.html?set=1&fromt=30000142&to=30002187&qtype=Systems&age=#{Evecentral::MAX_DELAY}&minprofit=#{Evecentral::MIN_PROFIT}&size=#{Evecentral::MAX_SPACE}&limit=#{Evecentral::MAX_RESULTS}&sort=sprofit&prefer_sec=0",
  "https://eve-central.com/home/tradefind_display.html?set=1&fromt=30000142&to=30002659&qtype=Systems&age=#{Evecentral::MAX_DELAY}&minprofit=#{Evecentral::MIN_PROFIT}&size=#{Evecentral::MAX_SPACE}&limit=#{Evecentral::MAX_RESULTS}&sort=sprofit&prefer_sec=0",
  "https://eve-central.com/home/tradefind_display.html?set=1&fromt=30002659&to=30002187&qtype=Systems&age=#{Evecentral::MAX_DELAY}&minprofit=#{Evecentral::MIN_PROFIT}&size=#{Evecentral::MAX_SPACE}&limit=#{Evecentral::MAX_RESULTS}&sort=sprofit&prefer_sec=0",
  "https://eve-central.com/home/tradefind_display.html?set=1&fromt=30002659&to=30000142&qtype=Systems&age=#{Evecentral::MAX_DELAY}&minprofit=#{Evecentral::MIN_PROFIT}&size=#{Evecentral::MAX_SPACE}&limit=#{Evecentral::MAX_RESULTS}&sort=sprofit&prefer_sec=0",
  ]

  urls1 = [ ### 4 regions
  "https://eve-central.com/home/tradefind_display.html?set=1&fromt=10000002&to=10000032&qtype=Regions&age=#{Evecentral::MAX_DELAY}&minprofit=#{Evecentral::MIN_PROFIT}&size=#{Evecentral::MAX_SPACE}&limit=#{Evecentral::MAX_RESULTS}&sort=sprofit&prefer_sec=0",
  "https://eve-central.com/home/tradefind_display.html?set=1&fromt=10000002&to=10000032&qtype=Regions&age=#{Evecentral::MAX_DELAY}&minprofit=#{Evecentral::MIN_PROFIT}&size=#{Evecentral::MAX_SPACE}&limit=#{Evecentral::MAX_RESULTS}&sort=sprofit&prefer_sec=0",
  "https://eve-central.com/home/tradefind_display.html?set=1&fromt=10000002&to=10000043&qtype=Regions&age=#{Evecentral::MAX_DELAY}&minprofit=#{Evecentral::MIN_PROFIT}&size=#{Evecentral::MAX_SPACE}&limit=#{Evecentral::MAX_RESULTS}&sort=sprofit&prefer_sec=0",
  "https://eve-central.com/home/tradefind_display.html?set=1&fromt=10000002&to=10000047&qtype=Regions&age=#{Evecentral::MAX_DELAY}&minprofit=#{Evecentral::MIN_PROFIT}&size=#{Evecentral::MAX_SPACE}&limit=#{Evecentral::MAX_RESULTS}&sort=sprofit&prefer_sec=0",
  "https://eve-central.com/home/tradefind_display.html?set=1&fromt=10000032&to=10000002&qtype=Regions&age=#{Evecentral::MAX_DELAY}&minprofit=#{Evecentral::MIN_PROFIT}&size=#{Evecentral::MAX_SPACE}&limit=#{Evecentral::MAX_RESULTS}&sort=sprofit&prefer_sec=0",
  "https://eve-central.com/home/tradefind_display.html?set=1&fromt=10000032&to=10000043&qtype=Regions&age=#{Evecentral::MAX_DELAY}&minprofit=#{Evecentral::MIN_PROFIT}&size=#{Evecentral::MAX_SPACE}&limit=#{Evecentral::MAX_RESULTS}&sort=sprofit&prefer_sec=0",
  "https://eve-central.com/home/tradefind_display.html?set=1&fromt=10000032&to=10000047&qtype=Regions&age=#{Evecentral::MAX_DELAY}&minprofit=#{Evecentral::MIN_PROFIT}&size=#{Evecentral::MAX_SPACE}&limit=#{Evecentral::MAX_RESULTS}&sort=sprofit&prefer_sec=0",
  "https://eve-central.com/home/tradefind_display.html?set=1&fromt=10000043&to=10000002&qtype=Regions&age=#{Evecentral::MAX_DELAY}&minprofit=#{Evecentral::MIN_PROFIT}&size=#{Evecentral::MAX_SPACE}&limit=#{Evecentral::MAX_RESULTS}&sort=sprofit&prefer_sec=0",
  "https://eve-central.com/home/tradefind_display.html?set=1&fromt=10000043&to=10000032&qtype=Regions&age=#{Evecentral::MAX_DELAY}&minprofit=#{Evecentral::MIN_PROFIT}&size=#{Evecentral::MAX_SPACE}&limit=#{Evecentral::MAX_RESULTS}&sort=sprofit&prefer_sec=0",
  "https://eve-central.com/home/tradefind_display.html?set=1&fromt=10000043&to=10000047&qtype=Regions&age=#{Evecentral::MAX_DELAY}&minprofit=#{Evecentral::MIN_PROFIT}&size=#{Evecentral::MAX_SPACE}&limit=#{Evecentral::MAX_RESULTS}&sort=sprofit&prefer_sec=0",
  "https://eve-central.com/home/tradefind_display.html?set=1&fromt=10000047&to=10000002&qtype=Regions&age=#{Evecentral::MAX_DELAY}&minprofit=#{Evecentral::MIN_PROFIT}&size=#{Evecentral::MAX_SPACE}&limit=#{Evecentral::MAX_RESULTS}&sort=sprofit&prefer_sec=0",
  "https://eve-central.com/home/tradefind_display.html?set=1&fromt=10000047&to=10000032&qtype=Regions&age=#{Evecentral::MAX_DELAY}&minprofit=#{Evecentral::MIN_PROFIT}&size=#{Evecentral::MAX_SPACE}&limit=#{Evecentral::MAX_RESULTS}&sort=sprofit&prefer_sec=0",
  "https://eve-central.com/home/tradefind_display.html?set=1&fromt=10000047&to=10000043&qtype=Regions&age=#{Evecentral::MAX_DELAY}&minprofit=#{Evecentral::MIN_PROFIT}&size=#{Evecentral::MAX_SPACE}&limit=#{Evecentral::MAX_RESULTS}&sort=sprofit&prefer_sec=0",
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
  Crest.mget_ary(mget_q)

  puts "prefetch #{Time.now - prefetch_start}s"
  puts ">>> prefetch.end <<<"
end


class Trade < EveDataCollection
  attr_accessor :id, :from_sid, :to_sid, :bids, :asks, \
                :qty, :profit, :cost, :age, :size, :ppv, :roi
  ref_accessor  :item_id
  def from() Station[@from_sid] end
  def to()   Station[@to_sid]   end
  
  @data = {}           ### Trade[] datastore
  K_SUM = '>>totals<<' ### reserved "item" name for route aggregators (trades[rt][K_SUM] = sum of trades[rt][*])
  def special?
    @item_id == K_SUM
  end
  def Trade.each
    @data.values.uniq.each { |x| yield(x.id, x) unless x.special? }  ### ignore K_SUM entries
  end  
  def Trade.wipe
    @data = {}
  end

  def Trade.data
    @data
  end
  
  @@uid = 1 
  def initialize(ivars={})
    ### instantiate by hash
    @id = @@uid; @@uid+=1  ### not a native Eve type, so need to generate our own UID
    super(ivars)
    ### financials
    @qty = 0
    @profit = 0.0
    @cost = 0.0
    @size = 0.0
    @ppv = 0.0
    @roi = 0.0
    @age = Time.new(0)
    ### market orders are by region, so filter by station
    if not special?
      mkt_from = Market[from.region][item]
      mkt_to   = Market[  to.region][item]
      @asks = mkt_from.sell_orders.select {|o| o.station==from}
      @bids =   mkt_to.buy_orders.select  {|o| o.station==to}
      @age = [mkt_from.sell_sampled, mkt_to.buy_sampled].max  ### most recent
    end
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

  include ExportSql
  def self.export_sql_hdr
    "(trade_id, from_stn, to_stn, item, qty, profit, cost, size, ppv, roi)"
  end
  def export_sql
    ### TODO: esc = method...
    esc = Mysql2::Client.method(:escape)
    from_stn_e = esc.call(from.sname)
    to_stn_e   = esc.call(to.sname)
    itemname_e = esc.call(item.name)
    "(#{id}, '#{from_stn_e}', '#{to_stn_e}', '#{itemname_e}', #{qty}, #{profit}, #{cost}, #{size}, #{ppv}, #{roi})"
  end

  def suspicious?
    known_scams = {
      "Cormack's Modified Armor Thermic Hardener" => true,
      "Draclira's Modified EM Plating" => true,
      "Gotan's Modified EM Plating" => true,
      "Tobias' Modified EM Ward Amplifier" => true,
      "Ahremen's Modified Explosive Plating" => true,
      "Setele's Modified Explosive Plating" => true,
      "Raysere's Modified Mega Beam Laser" => true,
    }

    asks = Market[from.region][item].sell_orders
    bids = Market[  to.region][item].buy_orders
    if item.name.match("^(Improved|Standard|Strong) .* Booster$")
      ### contraband
      true
    elsif profit > 1_000_000_000 and known_scams[item.name] then
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

### calc_trades() -- calculate trades by matching buy/sell orders; also filters for profit, scams, etc.
### in: candidates, Market[][]
### out: trades, Trade[]
def calc_trades(candidates)
  _start = Time.now

  ### sort all orders by price
  Market.each do |_rid, _iid, mkt|
    next if mkt.buy_orders.length == 0 and mkt.sell_orders.length == 0
    mkt.sell_orders.sort! {|o1, o2| o1.price <=> o2.price }
    mkt.buy_orders.sort!  {|o1, o2| o2.price <=> o1.price }
  end

  ### populate trades[[from_stn, to_stn]][item] from candidates[]
  Trade.wipe  ### reset datastore
  trades = {} ### reset
  candidates.each do |(from, to, item)|
    rt = [from, to]
    trades[rt]               ||= {}
    trades[rt][Trade::K_SUM] ||= Trade.new({from_sid:from.id, to_sid:to.id, item_id:Trade::K_SUM})
    trades[rt][item]           = Trade.new({from_sid:from.id, to_sid:to.id, item_id:item.id})
  end
  
  ### match bid/ask orders for each trades[route][item]
  puts "------------\n"
  ### financial parameters
  net_tax = 0.9925  ### Accounting skill level 5
  min_profit = 1
  min_ppv = 1_000
  min_total_profit = 3_000_000
  min_route_profit = 20_000_000
  trades.each_key do |rt| 
    trades[rt].each_key do |item|
      next if item==Trade::K_SUM  ### aggregators

      t = trades[rt][item]
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

      ### delete if no profitable matches (item)
      if (i_ask == 0 and i_bid == 0) then Trade.delete(t.id); trades[rt].delete(item); next end

      ### derived calcs
      t.size = t.qty * item.volume
      t.ppv = t.profit / t.size
      t.roi = t.profit / t.cost

      ### delete if under profit thresholds, known scam, etc. (item)
      if t.profit < min_total_profit  then Trade.delete(t.id); trades[rt].delete(item); next end
      if t.ppv < min_ppv              then Trade.delete(t.id); trades[rt].delete(item); next end
      if t.suspicious?                then Trade.delete(t.id); trades[rt].delete(item); next end
      ### TODO: Trade.ignore
      
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
    end  ### each trade[rt][*iid*]
    
    ### delete if under profit threshold (route)
    if trades[rt][Trade::K_SUM].profit < min_route_profit then
      ### wipe entire route
      trades[rt].each do |item, t| Trade.delete(t.id); trades[rt].delete(item) end
      trades[rt].delete(Trade::K_SUM)
      trades.delete(rt)
    end
  end  ### each trade[*rt*]
  puts "calc_trades() #{'%.3f'%(Time.now - _start)}s"


  
  ### debug: print routes (increasing profit)
  routes_sorted = trades.keys.sort_by {|rt| trades[rt][Trade::K_SUM].profit}
  last_rt = nil
  n = 0
  routes_sorted.each do |rt|
    from_stn, to_stn = rt
    last_rt = rt
    n+=1
    totals = trades[rt][Trade::K_SUM]
    next if totals.profit == 0 
    next if totals.profit < min_route_profit
    #next unless from_stn.hub? or to_stn.hub?  ### only show trades involving 1 hub
    #next unless from_stn.hub? and to_stn.hub? ### only show trades involving 2 hubs
    
    puts "#{n}. #{from_stn.sname}  =>  #{to_stn.sname}"
    puts "=> $#{comma(totals.profit / 1_000_000.0)}M profit, #{comma_i totals.size.to_i} m3, #{'%.3f'%(totals.cost/1_000_000_000.0)}B cost"
    
    ### sort: most profitable first
    sorted = trades[rt].values.sort do |a,b| b.profit <=> a.profit end 
    sorted.each do |t| 
      next if t.special? 
      puts "    $#{'%.1f' % (t.profit / 1_000_000.0)}M, #{comma_i t.qty}x #{t.item.name}, $#{'%.1f' % (t.ppv / 1_000.0)}K/m3"
      if t.profit > 100_000_000
        ### print underlying orders
        t.asks.each {|x| print "    #{x}"; puts x.ignore ? "" : " *"}
        puts "    ---"
        t.bids.each {|x| print "    #{x}"; puts x.ignore ? "" : " *"}
      end
    end
  end  ### trades[route]

  
=begin 
  ### debug: paste last route to clipboard
  from_stn, to_stn = last_rt
  buf = "\n#{from_stn.sname} => #{to_stn.sname}\n\n"
  sorted = trades[last_rt].values.sort do |a,b| b.profit <=> a.profit end 
  sorted.each do |t| 
    next if t.item_id == Trade::K_SUM 
    buf << "#{t.item.name}\n"
  end
  buf << "\n"
  Clipboard.copy(buf)
  puts "\a"   # beep
  puts "------\n"
  puts "Copy to clipboard:\n#{buf}"
  puts "------\n"
=end

  trades
end

class WheatDB
  def initialize(db_name)
    @db_name = db_name
    @db = Mymysql.new(db_name)
  end

  def _export_orders
    touch = {}
    Trade.each do |tid, t|
      t.asks.each do |o| touch[o.id] = true end
      t.bids.each do |o| touch[o.id] = true end
    end
    n = 0
    order_filter = Proc.new {|o| n+=1; touch[o.id]}
    Order.export_sql_table(@db, "orders", order_filter)
    puts "       orders #{'%.1f'%((n - touch.keys.size)*100.0/n)}% chaff (#{comma_i(n - touch.keys.size)})"
  end
  
  def _export_trades
    Trade.export_sql_table(@db, "trades")
  end
  
  def _export_orders_trades
    start = Time.now
    ### wipe table
    @db.query("TRUNCATE `orders_trades`;")
    #puts "DELETE orders_trades #{Time.now - start}s"
    t_del = Time.now - start
    ### insert new rows
    q = SqlQueue.new_insert(@db, "orders_trades", "(order_id, trade_id)")
    n = 0
    start2 = Time.now
    Trade.each do |tid, t|
      t.asks.each do |o1| q << "(#{o1.id}, #{t.id})"; n+=1 end
      t.bids.each do |o2| q << "(#{o2.id}, #{t.id})"; n+=1 end
    end
    t_str = Time.now - start2
    q.flush
    t_ins = Time.now - start
    puts "INSERT orders_trades x#{n} -- #{'%.3f'%t_ins}s (del #{'%.3f'%t_del}s, str #{'%.3f'%t_str}s, ins #{'%.3f'%(t_ins-t_del)}s)"
  end
  
  def export_sql
    _export_orders
    _export_trades
    _export_orders_trades
  end
end


### main loop
while 1
  ### repeat main loop every 60 sec
  next_loop = Time.now + 60
  $timers[:main] = Time.now

  prefetch
  Cache.save

  candidates = Evecentral.get_profitables # returns 3-tuple [from_stn, to_stn, item] 
  Crest.import_orders(candidates)         # populates Market[]
  Marketlogs.import_orders                # populates Market[]
  trades = calc_trades(candidates)        # confirm trades against fresh market data

  
  ### export to DB: orders, trades, orders_trades
	STDERR.puts "------------\n"
  db2 = WheatDB.new("wheat_development")
  db2.export_sql

  
  ### DEBUG: runtime stats
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
  nrefs ||= 0; puts "object datastore hits #{comma_i(EveDataCollection.hits-nrefs)}"; nrefs = EveDataCollection.hits
  $counters[:get] = 0;
	STDERR.puts "------------\n"

  
  ### refresh Marketlogs every 1 sec
  while (Time.now < next_loop)
    if Marketlogs.import_orders then
      calc_trades(candidates)
      db2.export_sql
    end
    sleep 1
  end
end ### while 1
