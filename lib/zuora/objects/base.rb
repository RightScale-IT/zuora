module Zuora::Objects
  # All Zuora objects extend from Zuora::Objects::Base, which provide the fundamental requirements
  # for handling creating, destroying, updating, and querying Zuora.
  class Base
    include Zuora::Attributes
    include Zuora::Validations
    include Zuora::Associations

    # generate a new instance of a Zuora object
    def initialize(attrs={}, &block)
      apply_default_attributes
      self.attributes = attrs
      yield self if block_given?
    end

    def attributes=(attrs={})
      attrs.each do |name, value|
        self.send("#{name.to_s.underscore}=", value)
      end
    end

    # given a soap response hash, initialize a record
    # and ensure they aren't dirty records.
    def self.generate(soap_hash, type)
      result = soap_hash[type][:result]
      return [] if result[:size] == 0
      if result[:records].is_a?(Array)
        result[:records].map do |record|
          (new self.connector.parse_attributes(type, record)).clear_changed_attributes!
        end
      else
        [(new self.connector.parse_attributes(type, result[:records])).clear_changed_attributes!]
      end
    end
    # get all the records
    def self.all(field_list=nil)
      results = self.get_updated(nil,nil,field_list)
      generate({query_response: {result: {done: true, size: results.length, records: results }}}, :query_response)

    end

    def self.get_updated(start_date, end_date, field_list=nil)
      #gets all if passed nil parameters
      keys = field_list.is_a?(Array) ? field_list.dup : (attributes - unselectable_attributes).map(&:to_s).map(&:zuora_camelize)
      
      field_lists = format_query_keys(keys)

      soql_queries = field_lists.flat_map { |fields| generate_query_zoql(fields,remote_name,start_date,end_date) }
      raw_results = soql_queries.flat_map{|soql| self.query_all(soql) }.delete_if{|row| row.nil?}
      
      if field_lists.length > 1 
        results = combine_results(raw_results)
      else 
        results = raw_results
      end
      generate({query_response: {result: {done: true, size: results.length, records: results }}}, :query_response)

    end

    def self.generate_query_zoql(fields,remote_name,start_date,end_date)
      field_string = fields.join(', ')      
      sql = "select #{field_string} from #{remote_name}" 
        unless start_date.nil? && end_date.nil?
         time_filter = " where UpdatedDate >= '#{start_date.new_offset(0).to_s}' and UpdatedDate < '#{end_date.new_offset(0).to_s}'" 
         sql += time_filter
        end
      sql
    end


    # find a record by the id
    def self.find(id)
      where(:id => id).first
    end

    # reload the record from the remote source
    def reload!
      self.class.find(id).attributes.each{|k,v|
        self.send("#{k}=", v)
      }
      @previously_changed = changes
      @changed_attributes.clear
      self
    end

    def self.unselectable_attributes
      class_variable_get(:@@complex_attributes).keys +
      class_variable_get(:@@write_only_attributes) +
      class_variable_get(:@@deferred_attributes)
    end

    def self.namespace(uri)
      Zuora::Api.instance.client.operation(:query).build.send(:namespace_by_uri, uri)
    end

    def self.zns
      namespace('http://api.zuora.com/')
    end

    def zns
      self.class.zns
    end

    def self.ons
      namespace('http://object.api.zuora.com/')
    end

    def ons
      self.class.ons
    end

    # locate objects using a custom where clause, currently arel
    # is not supported as it requires an actual db connection to
    # generate the sql queries. This may be overcome in the future.
    def self.where(where)
      keys = (attributes - unselectable_attributes).map(&:to_s).map(&:zuora_camelize)
      if where.is_a?(Hash)
        # FIXME: improper inject usage.
        where = where.inject([]){|t,v| t << "#{v[0].to_s.zuora_camelize} = '#{v[1]}'"}.sort.join(' and ')
      end

      field_lists = format_query_keys(keys)

      soql_queries = field_lists.flat_map { |fields| "select #{fields.join(', ')} from #{remote_name} where #{where}" }
      raw_results = soql_queries.flat_map{|soql| self.query_all(soql) }.delete_if{|row| row.nil?}
      
      if field_lists.length > 1 
        results = combine_results(raw_results)
      else 
        results = raw_results
      end

      generate({query_response: {result: {done: true, size: results.length, records: results }}}, :query_response)
    end

    def self.query(query_string)
      raw_response = self.connector.query(query_string)
      response_result = raw_response.body[:query_response][:result]
      @records = response_result.delete(:records)
      @metadata = response_result.merge!(:done => response_result[:done])
      return @records
    end

    def self.query_all(query_string)
      query(query_string)

      while !@metadata[:done] && @metadata[:query_locator].present? #&& tries < max_tries
        raw_response = self.connector.query_more(@metadata[:query_locator])
        response_result = raw_response.body[:query_more_response][:result]
        @records += response_result.delete(:records)
        @metadata = response_result.merge!(:done => response_result[:done])
      end

      case
      when @records == nil then
        size = 0
        results = nil
      when @records.is_a?(Hash) then
        size = 1
        results = @records
      else
        size = @records.length
        results = @records
      end
      
      results    
    end

    # has this record not been saved?
    def new_record?
      id.nil?
    end

    # has this record been persisted?
    def persisted?
      !new_record?
    end

    # save the record by updating or creating the record.
    def save
      return false unless valid?
      !!(new_record? ? create : update)
    end

    def save!
      raise StandardError.new(self.errors.map.inspect) unless save
    end

    # create the record remotely
    def create
      result = self.connector.create
      apply_response(result.to_hash, :create_response)
    end

    def update
      result = self.connector.update
      result = apply_response(result.to_hash, :update_response)
      reset_complex_object_cache
      return result
    end

    def format_bulk_update(xml)
      self.connector.generate_update_xml(xml)
    end

    # destroy the remote object
    def destroy
      result = self.connector.destroy
      apply_response(result.to_hash, :delete_response)
    end

    def self.connector_class
      @@connector_class ||= Zuora::SoapConnector
    end

    def self.connector_class=(connector)
      @@connector_class = connector
    end

    def self.connector
      self.connector_class.new(self)
    end

    def connector
      self.class.connector_class.new(self)
    end
    # used to combine multiple results for fields only query-able alone
    def self.combine_results(array_of_results)
      results = [] 
      processing_results = array_of_results.to_set.classify{|row| row[:id]}            
      processing_results.values.each { |row| comb = row.map.reduce{|result,obj| result.merge(obj){|key,old,new| old ? old : new }};  results << comb}
      results
    end

    def self.format_query_keys(keys)
       field_lists = []
       if self.respond_to?(:selectable_only_alone)
        selectable_only_alone.each do |field| 
          if keys.include?(field)
            field_lists << [field]
            keys.delete(field)
          end 
        end
      end

      field_lists << keys

      field_lists
    end

    
    protected

    # When remote data is loaded, remove the locally cached version of the
    # complex objects so that they may be cleanly reloaded on demand.
    def reset_complex_object_cache
      complex_attributes.invert.keys.each{|k| instance_variable_set("@#{k}_cached", false) }
    end

    # to handle new objects with defaults, we need to make the deafults
    # dirty so that they are passed on create requests.
    def apply_default_attributes
      default_attributes.try(:[], 0).try(:each) do |key, value|
        self.send("#{key}_will_change!")
        self.send("#{key}=", value)
      end
    end

    # parse the response and apply returned id attribute or errors
    def apply_response(response_hash, type)
      result = response_hash[type][:result]
      if result[:success]
        self.id = result[:id]
        @previously_changed = changes
        @changed_attributes.clear
        return true
      else
        self.errors.add(:base, result[:errors][:message])
        return false
      end
    end

  end
end
