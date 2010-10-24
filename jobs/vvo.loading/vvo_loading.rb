# Loading for Slovak public procurement
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

class VvoLoading < Loading
  
  def initialize(manager)
      super(manager)
      @defaults_domain = 'vvo'
  end

  def run
      source_table = 'sta_procurements'
      dataset_table = 'ds_procurements'
      joined_table = 'tmp_procurements_joined'
      regis_table = 'sta_regis_main'
      staging_schema = @manager.staging_schema

      self.phase = 'init'

      join = "
          CREATE TABLE #{staging_schema}.#{joined_table} (etl_loaded_date date)
          SELECT
              m.id,
              year,
              bulletin_id,
              procurement_id,
              customer_ico,
              rcust.name customer_company_name,
              substring_index(rcust.address, ',', 1) as customer_company_address,
              substring(substring_index(rcust.address, ',', -1), 9) as customer_company_town,
              supplier_ico,
              rsupp.name supplier_company_name,
              rsupp.region supplier_region,
              substring_index(rsupp.address, ',', 1) as supplier_company_address,
              substring(substring_index(rsupp.address, ',', -1), 9) as supplier_company_town,
              procurement_subject,
              price,
              currency,
              is_vat_included,
              customer_ico_evidence,
              supplier_ico_evidence,
              subject_evidence,
              price_evidence,
              procurement_type_id,
              document_id,
              m.source_url,
              m.date_created,
              is_price_part_of_range,
              customer_name,
              note,
              NULL etl_loaded_date
          FROM #{staging_schema}.#{source_table} m
          LEFT JOIN #{staging_schema}.#{regis_table} rcust ON rcust.ico = customer_ico
          LEFT JOIN #{staging_schema}.#{regis_table} rsupp ON rsupp.ico = supplier_ico
          WHERE m.etl_loaded_date IS NULL
          "
    
      self.logger.info "merging with organisations"
      self.phase = 'merge'

      drop_staging_table(joined_table)
      execute_sql(join)
    
      mapping = create_identity_mapping(joined_table)
      mapping[:batch_record_code] = :document_id
    
      self.logger.info "appending new records to dataset"
      self.phase = 'append'

      append_table_with_map(joined_table, dataset_table, mapping, :condition => "etl_loaded_date IS NULL")
      set_loaded_flag(source_table)
      finalize_dataset_loading(dataset_table)
      update_data_quality(dataset_table)
      self.phase = 'email'
      notify_if_bad_data(ds_procurements)
      self.phase = 'end'
  end

  def notify_if_bad_data(table_name)
    joined_dataset = @dataset_connection[table_name.to_sym]
    records_with_error = joined_dataset.filter('customer_company_name is ? or supplier_company_name is ?',nil,nil)
    if records_with_error.count > 0
      error_listing = records_with_error.map{|e| e[:_record_id] }.join(',')
      send_mail("Pri kopirovani dat do tabulky #{table_name} nastali problemy. #{records_with_error.count} zaznam(ov) s nasledovnymi ID je nutne skontrolovat: #{error_listing}.")
    end
  end
  
end
