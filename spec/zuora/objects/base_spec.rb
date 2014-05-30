require 'spec_helper'

class SomeExampleObject < Zuora::Objects::Base
end

class SomeExampleConnector
  def initialize(model)
  end
end

describe Zuora::Objects::Base do
  describe :connector do
    it "uses SoapConnector by default" do
      SomeExampleObject.connector.should be_a Zuora::SoapConnector
    end

    it "allows injecting different class for tests" do
      described_class.connector_class = SomeExampleConnector
      SomeExampleObject.connector.should be_a SomeExampleConnector
      #reset for subsequent tests
      described_class.connector_class = Zuora::SoapConnector
    end
  end

  describe :initializer do
    it "allows to overwrite default values" do
      Zuora::Objects::Account.new.auto_pay.should be_false
      Zuora::Objects::Account.new(:auto_pay => true).auto_pay.should be_true
    end

    it "assigns attributes from passed in hash" do
      Zuora::Objects::Account.new(:name => "Test Name").name.should == "Test Name"
    end
  end

  describe "attributes=" do
    it "should assign attributes to an existing instance from passed in hash" do
      account = Zuora::Objects::Account.new(:name => "Test Name")
      account.attributes = {:name => "New Name"}
      account.name.should == "New Name"
    end
  end

  describe "combine results" do
    let(:before_array) {[{id: 1, key1: "test"}, {id: 1, key2: "test"}, {id: 2, key1: "test"}, {id: 3, key1: "test"}, ]}
    let(:after_array) {[{id: 1, key1: "test", key2: "test"}, {id: 2, key1: "test"}, {id: 3, key1: "test"}, ]}

    it "should take an array of hashes, with some having a commmon 'id' key and various other key/value pairs, and return an array with one hash per key, with unique key/value pairs combined" do
      Zuora::Objects::Base.combine_results(before_array).should eq(after_array)
    end

    let(:unique_keys_array) { unique_keys_array = before_array.dup;
                              unique_keys_array.delete_at(0);
                              unique_keys_array}
    it "should take an array of hashes, with none having a commmon 'id' key and various other key/value pairs, and return the same array" do
      Zuora::Objects::Base.combine_results(unique_keys_array).should eq(unique_keys_array)
    end

  end
end
