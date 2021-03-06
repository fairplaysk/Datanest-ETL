# JobManager - manage ETL jobs
#
# Copyright:: (C) 2009 Knowerce, s.r.o.
# 
# Author:: Stefan Urbanek
# Date:: Oct 2009
#

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

require 'rubygems'
require 'sequel'
require 'yaml'
require 'pathname'
require 'active_record'
require 'lib/job'
require 'lib/job_info'
require 'lib/job_status'
require 'logger'
require 'lib/extraction'
require 'lib/loading'
require 'lib/download_manager'

# Main ETL class that manages all jobs. Use this object to:
# * prepare and queue jobs
# * run jobs
# * schedule jobs
# * configure database connection

class JobManager

attr_reader :connection
attr_reader :dataset_connection
attr_accessor :staging_schema, :dataset_schema
attr_accessor :configuration
attr_accessor :log
attr_accessor :debug
attr_accessor :domains_config
attr_reader :files_path
attr_reader :etl_files_path, :jobs_path
attr_accessor :job_search_path
attr_accessor :mailer_from
attr_accessor :mailer_to
attr_accessor :mailer_server

@@staging_system_columns = [:id, :date_created, :etl_loaded_date]
@@job_superclass = Job

# Create new instance of ETL job manager. Defaults domain for the ETL
# manager is set to _etl_, default path for file storage is ./files and
# jobs are searched in ./jobs
def initialize
	@defaults = ETLDefaults.new("etl")
	@etl_files_path = Pathname.new("files")
    @jobs_path = Pathname.new("jobs")
    log_file = STDERR
	return self
end

def establish_connection(connection_info)
    # Create database connection
    
    @connection_info = connection_info
    @connection = Sequel.mysql(@staging_schema,
            :user => connection_info["username"],
            :password => connection_info["password"], 
            :host => connection_info["host"],
            :encoding => 'utf8'
            )
            
    @dataset_connection = Sequel.mysql(@dataset_schema,
            :user => connection_info["username"],
            :password => connection_info["password"], 
            :host => connection_info["host"],
            :encoding => 'utf8'
            )

    Sequel::MySQL.default_charset = 'utf8'

	if @connection.nil?
		raise "Unable to establish database connection"
	end
	
	ActiveRecord::Base.establish_connection(
	  :adapter => "mysql",
	  :host => @connection_info["host"],
	  :username => @connection_info["username"],
	  :password => @connection_info["password"],
	  :database => @staging_schema,
	  :pool => 10,
	  :encoding => 'utf8')
end

def log_file=(logfile)
    @log = Logger.new(logfile)
    @log.formatter = Logger::Formatter.new
    @log.datetime_format = '%Y-%m-%d %H:%M:%S '
    if @debug
        @log.level = Logger::DEBUG
    else
        @log.level = Logger::INFO
    end
end

def debug=(debug_flag)
    # prevent some other values
    if debug_flag
        @debug = true
    else
        @debug = false
    end
    if @log
        if @debug
            @log.level = Logger::DEBUG
        else
            @log.level = Logger::INFO
        end
     end
end

def staging_schema=(schema)
	@staging_schema = schema
end
def path_for_job(job_name, job_type)

    Dir[@job_search_path].each { |search_path|
        job_path = Pathname.new("#{search_path}/#{job_name}.#{job_type}")
        return job_path if job_path.directory?        
    }
    return nil
end

def load_job_class(job_name, job_type)
	# FIXME: define root directory
    
    job_path = path_for_job(job_name, job_type)
    if not job_path
      @log.error "Unable to find job #{job_name}.#{job_type}"
      return nil
    end

    base_name = "#{job_name.downcase}_#{job_type}.rb"

    script_file = job_path + base_name

    if not script_file.exist?
	    @log.error "Unable to find #{job_type} class file #{script_file}"
	    return nil
    end

    require script_file
    
    class_name = job_name.camelize + job_type.capitalize
    job_class = Kernel.const_get(class_name)

    superclass = job_class.superclass

    while superclass != Object and superclass != @@job_superclass and superclass != nil do
        superclass = superclass.superclass
    end
    
    if superclass != @@job_superclass
        raise RuntimeError, "Class #{job_class} is not a superclass of #{@@job_superclass}"
    end

	return job_class
end

def run_scheduled_jobs(options = nil)
	force = @defaults.bool_value("force_run_all")

    job_type = nil
    if options
        job_type = options[:job_type]
    end

    if job_type
        @log.info "Running all scheduled jobs of type #{job_type}"
    else
        @log.info "Running all scheduled jobs"
    end

	if force
        @log.info "Forcing to run all enabled jobs (ignoring schedule)"
		jobs = JobInfo.find_enabled(:job_type => job_type)
		# FIXME: reset this flag for @production
		# @defaults["force_run_all"] = "false"
	else
		jobs = JobInfo.find_enabled(:job_type => job_type, :scheduled => true)
	end

    if jobs.nil? or jobs.empty?
        @log.info "No jobs to run"
    end

	run_jobs(jobs)
	
    # reset force run flag (make the job to be run according to schedule)
	reset = @defaults.bool_value("reset_force_run_flag")
    if reset
    	jobs.each { |job|
	        job.force_run = 0
	        job.save
    	}
    end
end

def run_job_with_name(job_name, job_type)
    job = JobInfo.find(:first, :conditions => ["name = ? AND job_type = ?",
                                                    job_name, job_type])
    if job
        run_job_with_info(job)
    else
        raise "No job info for job #{job_name} of type #{job_type}"
    end
end

def run_jobs(job_infos)
	job_infos.each {|info|
		@log.info "Running #{info.job_type} job #{info.name} (#{info.id})"
		run_job_with_info(info)
	}
end

def run_job_with_info(job_info)
	error = false

    @log.info "running job #{job_info.name}.#{job_info.job_type}"

    job_start_time = Time.now

	begin
		job_class = load_job_class(job_info.name, job_info.job_type)
		if job_class.nil?
		    raise "Unable to load job class for #{job_info.name} (#{job_info.job_type}), id = #{job_info.id}"
        end
		job = job_class.new(self)
    rescue => exception
        @log.error "Job #{job_info.name}(#{job_info.job_type}) failed: #{$!.message}"
        @log.error exception.backtrace.join("\n")

		fail_job(job_info, $!.message)
		return
	end

	# Prepare job status
	job.info = job_info
	job.job_status = JobStatus.new
	job.job_status.status = "running"
	job.job_status.start_date = Time.now
	job.job_status.job_name = job_info.name
	job.job_status.job_id = job_info.id
	job.job_status.save
	
	# Prepare defaults
	job.defaults_domain = job_info.name if job.defaults_domain.nil?
	
	job.defaults = ETLDefaults.new(job.defaults_domain)
	job.last_run_date = job_info.last_run_date

	job.prepare

    if not @debug
        begin
            job.run
            job.finalize
        rescue => ex
            job.status = "failed"
            job.message = $!.message
            job.finalize
            backtrace = ex.backtrace
        end
    else
        job.run
        job.finalize
    end    
	job_info.last_run_status = job.job_status.status
	job_info.last_run_date = job.job_status.end_date
	job_info.save
	if job.status == "failed"
		@log.error "Job #{job_info.name}(#{job_info.job_type}) failed: #{job.message}"
		@log.error backtrace.join("\n")
	end

    job_elapsed_time = ((Time.now - job_start_time) * 100).round / 100

    @log.info "job #{job_info.name}.#{job_info.job_type} finished. time: #{job_elapsed_time} s status:#{job.status}"
end

def fail_job(job_info, message)
	now = Time.now
	status = JobStatus.new
	status.status = "failed"
	status.start_date = now
	status.end_date = now
	status.job_name = job_info.name
	status.job_id = job_info.id
	status.message = message
	status.save
	job_info.last_run_status = "failed"
	job_info.last_run_date = now
	job_info.save
end
def staging_system_columns
	return @@staging_system_columns
end
def etl_files_path=(path)
    @etl_files_path = Pathname.new(path)
end
def jobs_path=(path)
    @jobs_path = Pathname.new(path)
end
def files_directory_for_job(job)
    path = @etl_files_path + job.name.underscore

    if path.exist?
        if path.directory?
            return path
        else
            raise "Path #{path} is a file, not a directory"
        end
    end
    
    path.mkpath()
    
    return path
end
def logger
    # FIXME: put depreciation warning here
    return @log
end
end
