$LOAD_PATH.unshift(File.dirname(__FILE__))
$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
require 'rubygems'
require 'sonar_connector_filestore'
require 'spec'
require 'spec/autorun'
require 'rr'
require 'fileutils'

Spec::Runner.configure do |config|
  config.mock_with RR::Adapters::Rspec
end

TMP_DIR = File.expand_path("../../tmp", __FILE__)
