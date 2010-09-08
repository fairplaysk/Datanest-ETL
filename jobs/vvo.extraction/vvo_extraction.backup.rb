# encoding: utf-8
# Extraction of public procurement
#
# Copyright (C) 2009 Aliancia Fair Play
# 
# Written by: Michal Barla
# Date: August 2009
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

require "pathname"

this_dir = Pathname.new(File.dirname(__FILE__))
require this_dir + "models/procurement"

require 'hpricot'
require 'iconv'
require 'pathname'

class VvoExtraction < Extraction
include DownloadManagerDelegate  

def run
    @defaults[:data_source_name] = "E-Vestnik"
    @defaults[:data_source_url] = "http://www.e-vestnik.sk/"

    @download_dir = files_directory + Time.now.strftime("%Y-%m-%d")
    if @download_dir.exist?
        @download_dir.rmtree
    end
    @download_dir.mkpath

    # Directory where processed files are archived
    @processed_dir = files_directory + "processed"
    @processed_dir.mkpath

    # 2641 - 8933
    @download_start_id = defaults.value(:download_start_id, 2641).to_i
    @download_daily_limit = defaults.value(:download_daily_limit, 100).to_i

    @download_threads = defaults.value(:download_threads, 3).to_i
    @batch_size = defaults.value(:batch_size, 10).to_i

    @base_url = defaults.value(:base_url, 'http://www.e-vestnik.sk/EVestnik/Detail/')

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
# Download manager delegate methods
def download_batch_failed(manager, batch)
    self.logger.warn "download batch #{batch.id} failed"
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

def process_download_batch(manager, batch)
    self.logger.info "process batch #{batch.id}"
    self.logger.info "  count of files #{batch.files.count}"
    batch.files.each { |filename|
        self.logger.info "process batch #{batch.id} file #{filename}"
        path = Pathname.new(filename)
        document_id = filename.basename.to_s.split('.').first.to_i

        result = process_file(path, document_id)

        if result != :ok
            self.logger.warn "document fail #{document_id} #{result}"
        end

        # if result == :unknown_announcement_type
        # self.logger.warn "unknown announcement type in #{document_id}"
        if result == :announcement_not_found
            next
        end

        @last_processed_id = document_id if document_id > @last_processed_id
        
        path.rename(@processed_dir + filename.basename)
    }
end

def process_file(file, document_id)
    Encoding.default_internal = Encoding.find("utf-8")
    
    puts "opening #{file}"
    file_content = File.open(file, "r:cp1250:utf-8").read
    file_content = file_content.gsub("&nbsp;",' ')
    
    doc = Hpricot(file_content)
    
    checked_value = (doc/"//tr[2]/td[@class='typOzn']")
    
    if checked_value.nil?
        #puts "FAILURE: Did not find announcement type, omitting file: #{file}"
        puts "warning: unknown_announcement_type"
        return :unknown_announcement_type
    else
        puts checked_value.inner_text.force_encoding("utf-8")
        if checked_value.inner_text.force_encoding("utf-8").force_encoding("utf-8") == "Oznámenie o výsledku verejného obstarávania"
            record = parse(doc)
            store(record, document_id)
        else
            #puts checked_value.inner_text.force_encoding("utf-8")
            #puts "#{file} is not result announcement"
            if((doc/"//div[@id='innerMain']/div/text()").inner_text.force_encoding("utf-8") == "Oznámenie nebolo nájdené")
                defaults[:download_interval_from] = document_id
                return :announcement_not_found
            end
        end
    end

    return :ok
end

def parse(doc)
    procurement_id = (doc/"//div[@id='innerMain']/div/h2").inner_text.force_encoding("utf-8")
    bulletin_and_year_content = (doc/"//div[@id='innerMain']/div/div").inner_text.force_encoding("utf-8")
    
    md = bulletin_and_year_content.gsub(/ /,'').match(/Vestníkč.(\d*)\/(\d*)/u)

    bulletin_id = md[1] unless md.nil?
    year = md[2] unless md.nil?
    
    customer_ico_content = (doc/"//table[@class='mainTable']/tbody/tr[7]/td/table/tbody/tr[2]/td[2]/table/tbody/tr[2]/td/").inner_text.force_encoding("utf-8")
    
    #we want to be sure, that we selected ICO with the XPath
    md = customer_ico_content.gsub(/ /,'').match(/IČO:(\d*)/u)
    if not md.nil?
        customer_ico = md[1]
    else
        customer_name = (doc/"//table[@class='mainTable']/tbody/tr[7]/td/table/tbody/tr[2]/td[2]/table/tbody/tr[1]/td/").inner_text.force_encoding("utf-8")	
        puts "unable to find ico for #{customer_name}"
        #we should try regis here, but it seems that 2009 procurements are all ok
    end
    
    procurement_subject = (doc/"//table[@class='mainTable']/tbody/tr[9]/td/table/tbody//span[@class='hodnota']").first.inner_text.force_encoding("utf-8") if (doc/"//table[@class='mainTable']/tbody/tr[9]/td/table/tbody//span[@class='hodnota']").first
    
    supplier_content = (doc/"//table[@class='mainTable']/tbody/tr[13]").inner_text.force_encoding("utf-8")
    
    #there could be multiple suppliers, ich supplying part of the procurement with separate price
    md_supp_arr = supplier_content.downcase.gsub(/ /,'').scan(/názovaadresadodávateľa,sktorýmsauzatvorilazmluva\s*^.*$\s*iČo:(\d*)/u)
    
    md_price_arr_from_supp_content = supplier_content.downcase.gsub(/ /,'').scan(/(celkovákonečnáhodnotazákazky:\s*hodnota|hodnota\/najnižšiaponuka\(ktorásabraladoúvahy\)):(\d*[,|.]?\d*)(\w*)\s*(bezdph|sdph|vrátanedph)*/u)
    
    suppliers = Array.new
    
    for i in 0..md_supp_arr.size-1
        supplier_ico = "#{md_supp_arr[i][0]}"
        # if we were able to match price here
        if not md_price_arr_from_supp_content[i].nil? 
            price = md_price_arr_from_supp_content[i][1]
            currency = md_price_arr_from_supp_content[i][2]
            vat_included = true
            vat_included = false if md_price_arr_from_supp_content[i][3] == "bezdph"
        end
        suppliers << {:supplier_ico => supplier_ico, 
                        :price => price, 
                        :currency => currency, 
                        :vat_included => vat_included}
    end
    
    record = { :customer_ico => customer_ico, 
             :suppliers => suppliers,
             :procurement_subject => procurement_subject,
             :year => year,
             :bulletin_id => bulletin_id,
             :procurement_id => procurement_id
            }
    return record
end
    
def store(procurement, document_id)
    procurement[:suppliers].each do |supplier|
    Procurement.create!({
        :document_id => document_id,
        :year => procurement[:year],
        :bulletin_id => procurement[:bulletin_id],
        :procurement_id => procurement[:procurement_id],
        :customer_ico => procurement[:customer_ico],
        :supplier_ico => supplier[:supplier_ico],
        :procurement_subject => procurement[:procurement_subject],
        :price => supplier[:price],
        :currency => supplier[:currency],
        :is_vat_included => supplier[:vat_included],
        :customer_ico_evidence => "",
        :supplier_ico_evidence => "",
        :subject_evidence => "",
        :price_evidence => "",
        :source_url => document_url(document_id),
        :date_created => Time.now})
    end
end 
def document_url(document_id)
    return "#{@base_url}#{document_id}"
end
end
