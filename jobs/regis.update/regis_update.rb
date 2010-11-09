require "pathname"

this_dir = Pathname.new(File.dirname(__FILE__))
require this_dir + "models/regis_main"
require this_dir + "../regis.extraction/regis_extraction"

require 'hpricot'
require 'iconv'
require 'pathname'
require 'open-uri'

class RegisUpdate < Extraction
  include DownloadManagerDelegate
  def setup
    self.phase = "init"
    @update_start_id = defaults.value(:update_start_id, 1).to_i
    @update_daily_limit = defaults.value(:update_daily_limit, 10).to_i
    @download_threads = defaults.value(:download_threads, 5).to_i
    @batch_size = defaults.value(:batch_size, 10).to_i
    @base_url = defaults.value(:base_url, "http://www.statistics.sk/pls/wregis/detail?wxidorg=")
    @file_encoding = "cp1250"
    
    setup_paths

    # Prepare download manager
    @download_manager = DownloadManager.new
    @download_manager.delegate = self
    @download_manager.download_directory = @download_dir
    @download_manager.thread_count = @download_threads
  end
  
  def setup_paths
    @download_dir = files_directory + Time.now.strftime("%Y-%m-%d")
    @download_dir.rmtree if @download_dir.exist?
    
    @download_dir.mkpath

    # Directory where processed files are archived
    @processed_dir = files_directory + "processed"
    @processed_dir.mkpath
  end
  
  def run
    setup
    self.logger.info "regis.update starting for documents with ids #{@update_start_id}-#{@update_start_id+@update_daily_limit}"
    
    # Documents to be downloaded
    @last_processed_id = 0
    @batch_start_id = @update_start_id
    @batch_limit_id = @update_start_id + @update_daily_limit
    
    # Do it!
    @download_manager.download
    update_start_id(@last_processed_id)
  end
  
  def update_start_id(last_doc_id)
    if (last_doc_id) == (@update_start_id+@update_daily_limit)
      defaults[:update_start_id] = last_doc_id
    else
      defaults[:update_start_id] = 1
    end
  end
  
  # delegate methods
  def create_download_batch(manager, batch_id)
    if @batch_start_id > @batch_limit_id
      # self.logger.info "no more files for batch #{batch_id}"
      return nil
    end

    last_id = @batch_start_id + @batch_size
    last_id = @batch_limit_id if last_id > @batch_limit_id

    self.logger.info "batch #{batch_id} range #{@batch_start_id}-#{last_id}"

    urls = (@batch_start_id..last_id).map{|doc_id| document_url(doc_id)}
    
    @batch_start_id = last_id + 1

    return DownloadBatch.new(urls)
  end
  
  def document_url(doc_id)
    element = RegisMain.find_by_doc_id(doc_id)
    element.source_url unless element.nil?
  end
  
  def download_batch_failed(manager, batch)
    self.logger.warn "download batch #{batch.id} failed"
  end
  
  def process_download_batch(manager, batch)
    self.logger.info "process batch #{batch.id}"
    self.logger.info "  count of files #{batch.files.count}"
    batch.files.each do |filename|
      #self.logger.info "process batch #{batch.id} file #{filename}"
      path = Pathname.new(filename)
      document_id = id_from_filename(filename) #filename.basename.to_s.split('.').first.to_i

      result = process_file(path, document_id)

      self.logger.warn "document fail #{document_id} #{result}" unless result == :ok

      next if result == :announcement_not_found

      @last_processed_id = document_id if document_id > @last_processed_id

      # path.rename(@processed_dir + filename.basename)
      path.delete
    end
  end
  
  def process_file(file, document_id)
    file_content = Iconv.conv("utf-8", @file_encoding, File.open(file).read)
    attributes = RegisExtraction.parse(Hpricot(file_content), id_from_filename(file), @base_url)
    self.logger.warn "document update fail #{document_id}" unless insert_into_table(attributes)

    return :ok
  end
  
  def insert_into_table(attributes)
    element = RegisMain.find_by_doc_id(attributes[:doc_id])
    element.update_attributes(attributes) if element
  end
  
  def id_from_filename(filename)
    filename.to_s.gsub(/(.*=)([0-9]+)(\.html)$/,'\2').to_i
  end
end