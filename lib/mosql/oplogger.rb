module MoSQL
  class Oplogger
    include MoSQL::Logging

    def self.create_table(db, tablename)
      db.create_table?(tablename) do
        column :id, 'SERIAL'
        column :timestamp,   'INTEGER'
        column :operation, 'TEXT'
        column :collection, 'TEXT'
        column :collection_id, 'TEXT'
        column :data, 'TEXT'
        primary_key [:id]
      end
      db[tablename.to_sym]
    end

    def initialize(table)
      @table   = table
    end

    def write_log(op)
      ns = op['ns']
      operation = op['op']
      id = op['o2']['_id']

      case op['op']
      when 'i'
        operation = 'insert'
      when 'u'
        operation = 'update'
      when 'd'
        operation = 'delete'
      end

      log.debug("inserting {:timestamp => #{op['ts'].seconds}, :operation => #{operation}, :collection => #{ns}, :collection_id => #{id} :data => #{op['o'].to_json}}")

      @table.insert({:timestamp => op['ts'].seconds, :operation => operation, :collection => ns, :collection_id => id, :data => op['o'].to_json})
    end
  end
end
