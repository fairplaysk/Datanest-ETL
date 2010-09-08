require 'csv'

class DatacampMirrorDump < Job

def run
    @source = self.config["source"]
    @target = self.config["target"]
    schema_map = self.config["schema_map"]

    @mysqldump_path = self.config["mysqldump_path"]
    @mysqldump_path = "mysqldump" if not @mysqldump_path

    @mysql_path = self.config["mysql_path"]
    @mysql_path = "mysql" if not @mysql_path
    
    schema_map.keys.each { |schema| 
        if not mysql_mirror_schema(schema, schema_map[schema])
            self.fail("unable to mirror schema #{schema}, please check database connections")
            return            
        end
    }

end

def mysql_mirror_schema(src_schema, target_schema)
    self.log.info "mirroring source #{src_schema} to #{target_schema}"

    # FIXME: test connections first

    dump_command = 
        "#{@mysqldump_path} -h #{@source["host"]} --user #{@source["username"]} \
            --password=#{@source["password"]} \
            --default-character-set=utf8 \
            #{src_schema}"

    load_command = "#{@mysql_path} -h #{@target["host"]} --user #{@target["username"]} \
                    --password=#{@target["password"]} --default-character-set=utf8 \
                    #{target_schema}"

    command = "#{dump_command} | #{load_command}"

    result = system(command)
    
    if not result
        self.log.error "mirror failed. check database connections"
        return false
    end
    return true
end

end
