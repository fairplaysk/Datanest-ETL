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

class RegisLoading < Loading
	def run
		source_table = 'sta_regis_main'
		dataset_table = 'ds_organisations'
		joined_table = 'sta_regis_joined'
		final_table = 'sta_regis_final'
		diff_table = 'tmp_regis_diff'
		new_table = 'tmp_regis_new'
		changed_table = 'tmp_regis_changed'
		
		staging_schema = @manager.staging_schema

        self.phase = 'mapping'

        drop_staging_table(joined_table)

        join = "
        CREATE TABLE #{staging_schema}.#{joined_table} (etl_loaded_date date)
        SELECT m.id, doc_id, ico, name,
            lf.text legal_form, legal_form legal_form_code,
            m.date_start, m.date_end,
            address, region,
            a1.text activity1, activity1 activity1_code,
            a2.text activity2, activity2 activity2_code,
            acc.text account_sector, account_sector account_sector_code,
            os.text ownership, ownership ownership_code,
            s.text size, size size_code,
            source_url, NULL etl_loaded_date
        FROM #{staging_schema}.#{source_table} m
        LEFT JOIN #{staging_schema}.sta_regis_legal_form lf ON lf.id = m.legal_form
        LEFT JOIN #{staging_schema}.sta_regis_activity1 a1 ON a1.id = m.activity1
        LEFT JOIN #{staging_schema}.sta_regis_activity2 a2 ON a2.id = m.activity2
        LEFT JOIN #{staging_schema}.sta_regis_account_sector acc ON acc.id = m.account_sector
        LEFT JOIN #{staging_schema}.sta_regis_ownership os ON os.id = m.ownership
        LEFT JOIN #{staging_schema}.sta_regis_size s ON s.id = m.size
        "
        
        self.logger.info "joining with enumerations"
        execute_sql(join)
        
        create_staging_table_index(joined_table, "ico")
        
        fields = [:doc_id, :name, :legal_form, :legal_form_code,
            :date_start, :date_end,:address, :region,
            :activity1, :activity1_code, :activity2, :activity2_code,
            :account_sector, :account_sector_code,
            :ownership, :ownership_code, :size, :size_code, :source_url]


        self.logger.info "creating differences"
        create_table_diff(diff_table, @manager.staging_schema, joined_table,
                                            @manager.dataset_schema, dataset_table,
                                            :ico, fields)                                            

        # Remove unchanged - apply diff
        
        self.logger.info "setting diff status"
        # FIXME: add diff flag into table
        #delete_unchanged = "DELETE FROM #{staging_schema}.#{joined_table}
        #                            WHERE ico NOT IN (SELECT ico FROM #{staging_schema}.#{diff_table})"

        # FIXME: this hangs mysql server, i do not know why
        # set_diff_status = "UPDATE #{@manager.staging_schema}.#{joined_table} t,
        #                          #{@manager.staging_schema}.#{diff_table} d
        #                          SET t.etl_diff_status = d.diff
        #                          WHERE t.ico = d.ico"
        # execute_sql(set_diff_status)
        
        create_final = "CREATE TABLE #{staging_schema}.#{final_table} AS
                                SELECT t.*, d.diff etl_diff_status 
                                FROM #{staging_schema}.#{joined_table} t
                                LEFT JOIN #{@manager.staging_schema}.#{diff_table} d
                                            ON d.ico = t.ico"
        drop_staging_table(final_table)
        execute_sql(create_final)
        create_staging_table_index(final_table, "etl_diff_status")
        
		mapping = create_identity_mapping(final_table)
		
		mapping.delete(:etl_diff_status)
		
		news_condition = "etl_loaded_date IS NULL AND etl_diff_status = 'n'"

        self.logger.info "appending new records"
		append_table_with_map(final_table, dataset_table, mapping, :condition => news_condition)

		change_condition = "@TABLE.etl_loaded_date IS NULL 
		                            AND @TABLE.etl_diff_status = 'c'"

        self.logger.info "updating old records"
		update_table_with_map(final_table, dataset_table, mapping, "ico", :condition => change_condition)
        
		set_loaded_flag(source_table)

		finalize_dataset_loading(dataset_table)
		self.phase = 'end'
	end
	
end
