class JobFlowCreator
    @queue = :job_flow_creation
    def self.perform()
      @emr_cluster = EMRCluster.new('emr_cluster_test-instance', true)
      @emr_job_flow = @emr_cluster.with_hive.hive_job('test-hive-job', 's3://sprinklr/ruby-sdk/hql/hsql-sample.q',{}).launch
    end
end
