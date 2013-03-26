$LOAD_PATH.unshift(File.dirname(__FILE__))
$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
require 'rubygems'
require 'sonar_connector_filestore'
require 'rspec'
require 'rspec/autorun'
require 'rr'
require 'fileutils'

RSpec.configure do |config|
  config.include(RR::Adapters::RSpec2)
end

TMP_DIR = File.expand_path("../../tmp", __FILE__)
