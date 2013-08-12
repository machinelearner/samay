require File.dirname(__FILE__) + '/base'

class EMRCluster
  attr_reader :steps

  def initialize(name='default-emr-cluster', keep_alive=false, config={})
    @emr ||= AWS::EMR.new
    @name = name
    @keep_alive = keep_alive
    @steps = []
    @launched_job_flows = []
    @config = {
        'log_uri' => 's3://sprinklr/ruby-sdk/logs/',
        'instance_count' => 2,
        'master_instance_type' => 'm1.small',
        'slave_instance_type' => 'm1.small',
        'hive_site_xml' => 's3://sprinklr/conf/hive/hive-site.xml'
    }.merge(config)
  end

  def launch(async = false)
    puts 'Launching EMR Cluster ...'

    job_flow = @emr.job_flows.create(@name, {
        :log_uri => @config['log_uri'],
        :instances => {
            :instance_count => @config['instance_count'],
            :master_instance_type => @config['master_instance_type'],
            :slave_instance_type => @config['slave_instance_type'],
            :keep_job_flow_alive_when_no_steps => @keep_alive,
            :ec2_key_name => 'emr'
        },
        :steps => @steps
    })

    emr_job_flow = EMRJobFlow.new(job_flow)
    @launched_job_flows << emr_job_flow
    return emr_job_flow if async

    emr_job_flow.wait_till_ready
    emr_job_flow
  end

  def terminate
    @launched_job_flows.each(&:terminate)
  end

  def with_hive
    @steps += [
        {
            :name => "#{@name}-emr-hive-setup",
            :action_on_failure => 'TERMINATE_JOB_FLOW',
            :hadoop_jar_step => {
                :jar => "s3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar",
                :args => [
                    "s3://us-east-1.elasticmapreduce/libs/hive/hive-script",
                    "--base-path", "s3://us-east-1.elasticmapreduce/libs/hive/",
                    "--install-hive",
                    "--hive-versions", "latest"
                ]
            }
        },
        {
            :name => "#{@name}-emr-hive-site-setup",
            :action_on_failure => 'TERMINATE_JOB_FLOW',
            :hadoop_jar_step => {
                :jar => "s3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar",
                :args => [
                    "s3://us-east-1.elasticmapreduce/libs/hive/hive-script",
                    "--base-path", "s3://us-east-1.elasticmapreduce/libs/hive/",
                    "--install-hive-site", "--hive-site", @config['hive_site_xml'],
                    "--hive-versions", "latest"
                ]
            }
        }]
    self
  end

  def hive_job(name, hive_query_file, config = {})
    throw 'Hive query file location is mandatory' if hive_query_file.nil?
    config = config.merge({
        'action_on_failure' => 'CONTINUE'
    })

    @steps += [
        {
            :name => name,
            :action_on_failure => config['action_on_failure'],
            :hadoop_jar_step => {
                :jar => "s3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar",
                :args => ["s3://us-east-1.elasticmapreduce/libs/hive/hive-script",
                          "--base-path", "s3://us-east-1.elasticmapreduce/libs/hive/",
                          "--hive-versions", "latest",
                          "--run-hive-script", "--args", "-f", hive_query_file]
            }
        }
    ]
    self
  end
end

