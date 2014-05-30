require 'spec_helper'

describe Zuora::Objects::Bulk do

	it "should not be null" do
 		subject.should_not be_nil
	end

	it "should be able to create multiple remote objects" do
		MockResponse.responds_with(:accounts_create_success) do
			acc = Zuora::Objects::Account.new
			acc.name = "test"
			acc2 = Zuora::Objects::Account.new
			acc2.name = "test2"
			acc_array = Array.new
			acc_array << acc
			acc_array << acc2

			subject.setup(acc_array)

			resp = subject.create
			#response should all be true
			resp.each do |res|
				res[:success].should be_true
			end
			#ids should be set
			subject.objects.each do |o|
				o.id.should_not be_nil
			end
		end
	end
	
	#describe "should be able to create objects in bulk" do
	#	before do
	#		Zuora.configure(username: "smogger914@yahoo.com", password: 'Zuora002', logger: true, sandbox: true)
	#	end
		
	#	it "should be able to create objects in bulk" do
	#		acc = Zuora::Objects::Account.new
	#		acc.name = "test"
	#		acc2 = Zuora::Objects::Account.new
	#		acc_array = Array.new
	#		acc_array << acc
	#		acc_array << acc2

	#		subject.setup(acc_array)

	#		resp = subject.create
	#		puts resp.to_hash[:create_response][:result].inspect

	#		resp.should be_nil
	#	end
	#end
end