class EMRJobFlow
  def initialize(job_flow)
    @job_flow = job_flow
  end

  def wait_till_ready(timeout = 600)
    total_time = 0
    wait_time = 60
    while total_time <= timeout && @job_flow.state != 'WAITING'
      puts "Sleeping for another #{wait_time} seconds of total #{total_time} seconds for EMR Cluster to be ready. Current state #{@job_flow.state}."
      sleep(wait_time)
      total_time+=wait_time
    end
    throw 'Timeout waiting for EMR cluster' if @job_flow.state != 'WAITING'
    puts "Launched cluster #{@job_flow.id}"
  end

  def state
    @job_flow.state
  end

  def terminate
    @job_flow.terminate
  end

  def step_details
    @job_flow.step_details
  end
end

