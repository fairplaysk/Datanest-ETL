require "pathname"

this_dir = Pathname.new(File.dirname(__FILE__))
require this_dir + "models/regis_main"
require this_dir + "../regis.extraction/regis_extraction"

require 'hpricot'
require 'iconv'
require 'pathname'
require 'open-uri'

class RegisUpdate < Extraction
  def setup
    @update_start_id = defaults.value(:update_start_id, 1).to_i
    @update_daily_limit = defaults.value(:update_daily_limit, 10).to_i
  end
  
  def run
    setup
    self.logger.info "regis.update starting for documents with ids #{@update_start_id}-#{@update_start_id+@update_daily_limit}"
    
    document_update_count = 0
    last_doc_id = 0
    (@update_start_id..(@update_start_id+@update_daily_limit)).each do |doc_id|
      puts doc_id
      record = RegisMain.find_by_doc_id(doc_id)
      if record
        document_update_count += 1 if update_record(record)
        last_doc_id = doc_id
      end
    end
    update_start_id(last_doc_id)
    self.logger.info "regis.update finished successfully for #{document_update_count} document(s)."
  end
  
  def update_record(record)
    doc = open(record.source_url) { |f| Hpricot(Iconv.conv("utf-8", "cp1250", f.read)) }
    record.update_attributes(RegisExtraction.parse(doc, record.doc_id, nil, record.source_url))
  end
  
  def update_start_id(last_doc_id)
    if (last_doc_id) == (@update_start_id+@update_daily_limit)
      defaults[:update_start_id] = last_doc_id
    else
      defaults[:update_start_id] = 1
    end
  end
end