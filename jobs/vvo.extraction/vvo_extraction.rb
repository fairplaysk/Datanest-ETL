# encoding: utf-8
# Extraction of public procurement
#
# Copyright (C) 2009 Aliancia Fair Play
# 
# Written by: Michal Barla
# Fixed by: Vojto Rinik
#
# Date: August 2009
# Date: July 2010
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
    file_content = File.open(file).read
    file_content = Iconv.conv('utf-8', 'cp1250', file_content)
    file_content = file_content.gsub("&nbsp;",' ')
    
    doc = Hpricot(file_content)

    @table_offset = 0
    checked_value = (doc/"//tr[#{2+@table_offset}]/td[@class='typOzn']")
    if checked_value.inner_text.blank?
      @table_offset = 1
      checked_value = (doc/"//tr[#{2+@table_offset}]/td[@class='typOzn']")
    end
    checked_value = (doc/"//div[@id='innerMain']/div/h2")
    
    if checked_value.nil?
        #puts "FAILURE: Did not find announcement type, omitting file: #{file}"
        puts "\e\[31m"
        puts "warning: unknown_announcement_type"
        puts "\e\[0m"
        return :unknown_announcement_type
    else
	puts checked_value.inner_text
	      document_type = checked_value.inner_text
        #if document_type == "Oznámenie o výsledku verejného obstarávania"
        if document_type.match(/V\w+$/)
            puts "\e\[32m"
            puts "› #{file} is OK (#{document_type})"
            record = parse(doc)
            puts "› Storing data. (# suppliers: #{record[:suppliers].count})"
            # puts "\e\[33m"
            # puts record.to_yaml
            puts "\e\[0m"
            store(record, document_id)
        else
            puts "\e\[31m"
            puts "#{file} is not result announcement (#{document_type})"
            puts "\e\[0m"
            if((doc/"//div[@id='innerMain']/div/text()").inner_text == "Oznámenie nebolo nájdené")
                defaults[:download_interval_from] = document_id
                return :announcement_not_found
            end
        end
    end

    return :ok
end

def parse(doc)
  procurement_id = (doc/"//div[@id='innerMain']/div/h2").inner_text
  
  bulletin_and_year = (doc/"//div[@id='innerMain']/div/div").inner_text
  bulletin_and_year_content = bulletin_and_year.gsub(/ /,'').match(/Vestník.*?(\d*)\/(\d*)/u)
  bulletin_id = bulletin_and_year_content[1] unless bulletin_and_year_content.nil?
  year = bulletin_and_year_content[2] unless bulletin_and_year_content.nil?

  suppliers = []
  
  customer_ico = customer_name = ''

  (doc/"//span[@class='nadpis']").each do |element|
    if element.inner_text.match(/ODDIEL\s+I\W/)
      customer_information = element.following_siblings.first
      customer_name = (customer_information/"/tbody/tr[2]/td[2]/table/tbody/tr[1]/td[@class='hodnota']/span/span").inner_text.strip
      customer_name = (customer_information/"/tbody/tr[2]/td[2]/table/tbody/tr[1]/td[@class='hodnota']/span").inner_text.strip if customer_name.empty?
      customer_ico = (customer_information/"/tbody/tr[2]/td[2]/table/tbody/tr[2]/td[@class='hodnota']//span[@class='hodnota']").inner_text.strip
    elsif element.inner_text.match(/ODDIEL\s+V\W/)
      supplier_information = element.following_siblings.first
      supplier = {}
      (supplier_information/"//td[@class='kod']").each do |code|
        if code.inner_text.match(/V\.*.*?[^\d]1{1}[^\d]$/)
          #supplier[:date] = Date.parse((code.following_siblings.first/"//span[@class='hodnota']").inner_text)
        elsif code.inner_text.match(/V\.*.*?[^\d]3{1}[^\d]/)
          supplier = {}
          supplier_details = code.parent.following_siblings.first/"//td[@class='hodnota']//span[@class='hodnota']"
          supplier[:supplier_name] = supplier_details[0].inner_text; supplier[:supplier_ico] = supplier_details[1].inner_text.gsub(' ', '').to_i; supplier[:supplier_ico_evidence] = "";
        elsif code.inner_text.match(/V\.*.*?[^\d]4{1}[^\d]/)
          code.parent.following_siblings.each do |price_detail|
            break unless (price_detail/"//td[@class='kod']").inner_text.empty?
            if (price_detail/"//span[@class='podnazov']").inner_text.match(/konečná/) || (price_detail/"//span[@class='nazov']").inner_text.match(/konečná/)
              price = (price_detail.following_siblings.first/"//span[@class='hodnota']")
              supplier[:is_price_part_of_range] = (price_detail.following_siblings.first/"//span[@class='podnazov']").inner_html.downcase.match(/najnižšia/) ? true : false
              supplier[:price] = price[0].inner_text.gsub(' ', '').gsub(',','.').to_f
              supplier[:currency] = if price.inner_text.downcase.match(/sk|skk/) then 'SKK' else 'EUR' end
              supplier[:vat_included] = !(price_detail.following_siblings[0]/"//span[@class='hodnota']").inner_text.downcase.match(/bez/) && !(price_detail.following_siblings[1]/"//span[@class='hodnota']").inner_text.downcase.match(/bez/)
              suppliers << supplier
            end
          end
        end
      end
    end
  end
  

  # customer_ico_content = (doc/"//table[@class='mainTable']/tbody/tr[#{7+@table_offset}]/td/table/tbody/tr[2]/td[2]/table/tbody/tr[2]/td/").inner_text
  # customer_ico_content = (doc/"//table[@class='mainTable']/tr[#{7+@table_offset}]/td/table").inner_text if (customer_ico_content.empty?)    

  # 
  # #we want to be sure, that we selected ICO with the XPath
  # md = customer_ico_content.gsub(/ /,'').match(/IČO:(\d*)/u)
  # if not md.nil?
  #     customer_ico = md[1]
  # else
  #     customer_name = (doc/"//table[@class='mainTable']/tbody/tr[#{7+@table_offset}]/td/table/tbody/tr[2]/td[2]/table/tbody/tr[1]/td/").inner_text  
  #     puts "unable to find ico for #{customer_name}"
  #     #we should try regis here, but it seems that 2009 procurements are all ok
  # end

  procurement_subject = (doc/"//table[@class='mainTable']/tbody/tr[#{7+@table_offset}]/td/table/tbody//span[@class='hodnota']")
  procurement_subject = (doc/"//table[@class='mainTable']/tr[#{7+@table_offset}]/td/table//span[@class='hodnota']") if(procurement_subject.size == 0)
  procurement_subject = procurement_subject.first.inner_text.strip if procurement_subject.first

  #     supplier_content = (doc/"//table[@class='mainTable']/tbody/tr[#{13+@table_offset}]").inner_text
  #     supplier_content = (doc/"//table[@class='mainTable']/tr[#{13+@table_offset}]").inner_text if (supplier_content.size == 0)
  # 
  #     
  #     #there could be multiple suppliers, each of them supplying part of the procurement with separate price
  #     
  #     supplier_content_stripped = \
  #       supplier_content.mb_chars.downcase.
  #         gsub('Č','č').
  #         gsub('Á', 'á').
  #         gsub('É', 'é').
  #         gsub('Í', 'í').
  # gsub('Ý', 'ý').
  # gsub('Ľ', 'ľ').
  #         gsub(/ /, '')
  # 
  #     md_supp_arr = supplier_content_stripped.scan(/(názovaadresadodávateľa,sktorýmsauzatvorilazmluva|názovaadresahospodárskehosubjektu,vprospechktoréhosarozhodloozadanízákazky)\s*^.*$\s*ičo:(\d*)/u)
  #     
  #     md_supp_names_arr = supplier_content.scan(/(NÁZOV A ADRESA DODÁVATEĽA, S KTORÝM SA UZATVORILA ZMLUVA|Názov a adresa dodávateľa, s ktorým sa uzatvorila zmluva|NÁZOV A ADRESA HOSPODÁRSKEHO SUBJEKTU, V PROSPECH KTORÉHO SA ROZHODLO O ZADANÍ ZÁKAZKY)\s*^(Úradný názov:)?(.*)/u)
  #     
  #     md_price_arr_from_supp_content = supplier_content.mb_chars.downcase.gsub(/ /, '').scan(/(celkovákonečnáhodnotazákazky:\s*hodnota|hodnota\/najvyššiaponuka\(ktorásabraladoúvahy\)):(\d*[,|.]?\d*)(\w*)\s*(bezdph|sdph|vrátanedph)*/u)
  #     md_price_arr_from_supp_content = supplier_content.mb_chars.downcase.gsub(/ /, '').scan(/(najvyššiaponuka)\(ktorásabraladoúvahy\):(\d*[,|.]\d*)(\w*)\s*(vrátanedph|bezdph)/u) if (md_price_arr_from_supp_content.empty?)
  #     md_price_arr_from_supp_content = supplier_content.mb_chars.downcase.gsub(/ /, '').scan(/(najvyššiaponuka),ktorésazohľadňovali:(\d*[,|.]\d*)(\w*)\s*(vrátanedph|bezdph)/u) if (md_price_arr_from_supp_content.empty?)
  # 
  #     md_date_arr_from_supp_content = supplier_content.mb_chars.downcase.scan(/dátum uzatvorenia zmluvy\n?\s*(\d.*)/)
  #     md_date_arr_from_supp_content = supplier_content.mb_chars.downcase.scan(/dátum rozhodnutia o zadaní zákazky:?\n?\s*(\d.*)/) if md_date_arr_from_supp_content.empty?
  # 
  #     suppliers = []
  # 
  #     for i in 0..md_supp_arr.size-1
  #       supplier_ico = md_supp_arr[i][1]
  #       unless (md_price_arr_from_supp_content[i].nil?) #if we were able to match price here
  #         price = md_price_arr_from_supp_content[i][1].gsub(",", ".")
  #         currency = "EUR" if md_price_arr_from_supp_content[i][2].match(/eur/)
  #         currency = "SKK" if md_price_arr_from_supp_content[i][2].match(/[sk|skk]/)
  #         vat_included = true
  #         vat_included = false if md_price_arr_from_supp_content[i][3] == "bezdph"
  #       end
  #       unless (md_date_arr_from_supp_content[i].nil?) #if we were able to match date here
  #         date = Date.parse(md_date_arr_from_supp_content[i][0])
  #       end
  #       suppliers << {:supplier_ico => supplier_ico.to_i,
  #                     :supplier_name => md_supp_names_arr[i] ? md_supp_names_arr[i][2].gsub(/\302\240/,' ').strip : nil,
  #                     :supplier_ico_evidence => supplier_content,
  #                     :price => price.to_f,
  #                     :currency => currency,
  #                     :vat_included => vat_included,
  #                     :date => date}
  #     end

  {:customer_ico => customer_ico.to_i, :customer_name => customer_name, :customer_ico_evidence => "", :suppliers => suppliers, :procurement_subject => procurement_subject, :year => year.to_i, :bulletin_id => bulletin_id.to_i, :procurement_id => procurement_id}
end
    
def store(procurement, document_id)
    procurement[:suppliers].each do |supplier|
    Procurement.create!({
        :document_id => document_id,
        :year => procurement[:year],
        :bulletin_id => procurement[:bulletin_id],
        :procurement_id => procurement[:procurement_id],
        :customer_ico => procurement[:customer_ico],
        :customer_name => procurement[:customer_name],
        :supplier_ico => supplier[:supplier_ico],
        :supplier_name => supplier[:supplier_name],
        :procurement_subject => procurement[:procurement_subject],
        :price => supplier[:price],
        :is_price_part_of_range => supplier[:is_price_part_of_range],
        :currency => supplier[:currency],
        :is_vat_included => supplier[:vat_included],
        :customer_ico_evidence => procurement[:customer_ico_evidence],
        :supplier_ico_evidence => supplier[:supplier_ico_evidence],
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
