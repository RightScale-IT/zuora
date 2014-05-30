module Zuora::Objects
  #class to do bulk operations in Zuora
  class Bulk
  	attr_accessor :objects, :operation, :ons, :zns, :remote_name

  	def initialize(remote_name)
  		self.remote_name = remote_name
  	end
  	#do the operation in bulk
  	#take the objects and break them up into groups of 50

    def create(objects)
      result = Zuora::Api.instance.request(:create, { :message => generate_xml(objects) })
      #parse the result. i.e set the id if it success was true
      #what to do if it fails?
      response = result.hash[:envelope][:body][:create_response]
      if response[:errors]
        raise Zuora::Fault.new(:message => "#{response[:errors].inspect}")
      end
      results = response[:result].is_a?(Array) ? response[:result] : Array[response[:result]]

      results.each_with_index do |response, index|
          if response[:success] == true
            objects[index].id = response[:id]
          else
            objects[index].errors.add(:base, response[:errors][:message])
          end
      end
      objects
    end

    def update(objects)
      result = Zuora::Api.instance.request(:update, { :message => generate_update_xml(objects) })
      #parse the result. i.e set the id if it success was true
      #what to do if it fails?
      response = result.hash[:envelope][:body][:update_response]
      if response[:errors]
        raise Zuora::Fault.new(:message => "#{response[:errors].inspect}")
      end

      results = response[:result].is_a?(Array) ? response[:result] : Array[response[:result]]

      results.each_with_index do |response, index|
        object = objects[index]
        if response[:success] == true
          object.changed_attributes.clear
        else
          object.errors.add(:base, response[:errors][:message])
        end
      end
      objects
    end
    #generate the xml for the call
    def generate_update_xml(objects)
    	xml = Builder::XmlMarkup.new
      objects.each do |o|
        generate_update_object(xml, o)
      end
      xml.xml
    end

    #generate xml for an object
    def generate_xml(objects)
    	xml = Builder::XmlMarkup.new
      objects.each do |o|
        generate_object(xml, o)
      end
      xml.xml
    end

    def generate_update_object(builder, object)
    	builder.__send__(self.zns, :zObjects, 'xsi:type' => "ins0:#{self.remote_name}") do |a|
        object.format_bulk_update(a)
    	end
    end

    def generate_object(builder, object)
      builder.__send__(self.zns, :zObjects, 'xsi:type' => "ins1:#{self.remote_name}") do |a|
    		object.to_hash.each do |k,v|
	        a.__send__(self.ons, k.to_s.zuora_camelize.to_sym, convert_value(v)) unless v.nil?
	      end
      end
    end

    def convert_value(value)
      if [Date, Time, DateTime].any? { |klass| value.is_a?(klass) }
        value.strftime('%FT%T')
      else
        value
      end
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
  end
end
