# RegisExtraction - extraction of register of organisations in Slovakia
#
# Copyright (C) 2009 Knowerce, s.r.o.
# 
# Written by: Stefan Urbanek
# Date: Oct 2009
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

require 'monitor'
require 'iconv'
require 'hpricot'
require 'rexml/document'

class RegisExtraction < Extraction
  include DownloadManagerDelegate

  def initialize(manager)
    super(manager)

    @defaults_domain = 'regis'
    @target_table = "#{@manager.staging_schema}__sta_regis_main".to_sym
    end

    def setup_defaults
    @defaults[:data_source_name] = "Regis"
    @defaults[:data_source_url] = "http://www.statistics.sk/"

    setup_paths

    @download_start_id = defaults.value(:download_start_id, 1).to_i
    @download_daily_limit = defaults.value(:download_daily_limit, 10000).to_i

    @download_threads = defaults.value(:download_threads, 3).to_i
    @batch_size = defaults.value(:batch_size, 10).to_i
    @download_fail_threshold = defaults.value(:download_fail_threshold, 10).to_i

    @base_url = defaults.value(:base_url, "http://www.statistics.sk/pls/wregis/detail?wxidorg=")
    @file_encoding = "cp1250"
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
    setup_defaults

    self.phase = "init"

    # Prepare download manager
    @download_manager = DownloadManager.new
    @download_manager.delegate = self
    @download_manager.download_directory = @download_dir
    @download_manager.thread_count = @download_threads

    # Documents to be downloaded
    @last_processed_id = 0
    @batch_start_id = @download_start_id
    @batch_limit_id = @download_start_id + @download_daily_limit

    # Do it!
    @download_manager.download

    # If we have not downloaded everything, try to crawl slowly by batch-sized
    # chunks in a sinlge thread

    if @last_processed_id == @batch_limit_id
    download_over_limit
    end

    if @last_processed_id > 0
    self.logger.info "new download start id: #{@last_processed_id}"
    defaults[:download_start_id] = @last_processed_id
    end
  end

  def download_over_limit
    # ... in single thread
    @download_manager.thread_count = 1

    loop do
      self.logger.info "more documents than daily limit (#{@batch_limit_id})"

      @batch_start_id = @batch_limit_id
      @batch_limit_id = @batch_limit_id + @batch_size

      # get some more
      @download_manager.download

      break if @last_processed_id < @batch_limit_id
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

    urls = Array.new
    for doc_id in @batch_start_id..last_id
      urls << document_url(doc_id)
    end

    @batch_start_id = last_id + 1

    return DownloadBatch.new(urls)
  end

  def document_url(document_id)
    return "#{@base_url}#{document_id}"
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
    record = parse(Hpricot(file_content), file)

    return :ok
  end

  def parse(doc, filename)
    doc_id = id_from_filename(filename)

    ico = name = legal_form = date_start = date_end = address = region = ''
    (doc/"//div[@class='telo']/table[@class='tabid']/tbody/tr").each do |row|
      if (row/"//td[1]").inner_text.match(/i(Č|č|c)o/i)
        ico = (row/"//td[3]").inner_text
      elsif (row/"//td[1]").inner_text.match(/meno/i)
        name = (row/"//td[3]").inner_text
      elsif (row/"//td[1]").inner_text.match(/forma/i)
        legal_form = (row/"//td[3]").inner_text.split('-')[0].strip
      elsif (row/"//td[1]").inner_text.match(/vzniku/i)
        date_start = (row/"//td[3]").inner_text
      elsif (row/"//td[1]").inner_text.match(/z(a|á|Á)niku/i)
        date_end = (row/"//td[3]").inner_text
      elsif (row/"//td[1]").inner_text.match(/adresa/i)
        address = (row/"//td[3]").inner_text.strip
      elsif (row/"//td[1]").inner_text.match(/okres/i)
        region = (row/"//td[3]").inner_text.strip
      end
    end

    activity1 = activity2 = account_sector = ownership = size = ''
    (doc/"//div[@class='telo']/table[@class='tablist']/tbody/tr").each do |row|
      if (row/"//td[1]").inner_text.match(/SK NACE/i)
        activity1 = (row/"//td[2]").inner_text
      elsif (row/"//td[1]").inner_text.match(/OKE(Č|č|c)/i)
        activity2 = (row/"//td[2]").inner_text
      elsif (row/"//td[1]").inner_text.match(/sektor/i)
        account_sector = (row/"//td[2]").inner_text
      elsif (row/"//td[1]").inner_text.match(/vlastn(i|í|Í)ctva/i)
        ownership = (row/"//td[2]").inner_text
      elsif (row/"//td[1]").inner_text.match(/ve(l|ľ|Ľ)kosti/i)
        size = (row/"//td[2]").inner_text  
      end
    end

    date_start = Date.parse(date_start) rescue nil
    date_end = Date.parse(date_end) rescue nil

    url = @base_url.to_s + doc_id.to_s

    connection[@target_table].insert(
      :doc_id => doc_id,
      :ico => ico,
      :name => name,
      :legal_form => legal_form,
      :date_start => date_start,
      :date_end => date_end,
      :address => address,
      :region => region,
      :activity1 => activity1,
      :activity2 => activity2,
      :account_sector => account_sector,
      :ownership => ownership,
      :size => size,
      :date_created => Time.now,
      :source_url => url )
  end

  def id_from_filename(filename)
    filename.to_s.gsub(/(.*=)([0-9]+)(\.html)$/,'\2').to_i
  end

end
