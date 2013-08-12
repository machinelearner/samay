require 'yaml'
require 'resque_scheduler'
require 'resque_scheduler/server'

Resque.schedule = YAML.load_file("/Users/Admin/TW/sprinklr/samay/config/resque_schedule.yml")
