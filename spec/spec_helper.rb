# frozen_string_literal: true

$LOAD_PATH.unshift File.expand_path("../../lib", __FILE__)
require "racecar"
require "timecop"
require_relative 'support/mock_env'
require_relative 'support/integration_helper'
Thread.abort_on_exception = true

RSpec.configure do |config|
  config.disable_monkey_patching!
  config.include MockEnv
  config.include IntegrationHelper, type: :integration
  config.filter_run_when_matching :focus
end
