module InetData
  module Source
    class CT < Base

      def manual?
        true
      end

      def ct_request(url)
        target = URI.parse(url)
        tries  = 0
        begin

          tries += 1
          http   = Net::HTTP.new(target.host, target.port)

          if url.index("https") == 0
            http.use_ssl = true
          end

          req    = Net::HTTP::Get.new(target.request_uri)
          res    = http.request(req)

          unless (res and res.code.to_i == 200 and res['Content-Type'].index('application/json'))
            if res
              raise RuntimeError.new("Unexpected reply: #{res.code} - #{res['Content-Type']} - #{res.body.inspect}")
            else
              raise RuntimeError.new("Unexpected reply: #{res.code} - #{res['Content-Type']} - #{res.body.inspect}")
            end
          end

          return JSON.parse(res.body)

        rescue ::Interrupt
          raise $!
        rescue ::Exception
          if tries < self.max_tries
            log("CT request failed: #{url} #{$!.class} #{$!}, retrying...")
            sleep(30)
            retry
          else
            fail("CT request failed: #{url} #{$!.class} #{$!} after #{tries} attempts")
          end
        end
      end


      def ct_sync(log_name, log_base)

        nrecs = 0
        state = nil


        meta_file = File.join(storage_path, "#{log_name}_meta.json")
        if File.exists?(meta_file)
          state = JSON.parse(File.read(meta_file))
        end
        state ||= { 'entries' => 0 }

        # Data files are in the format of <log>_data_<start-record>.json
        data_file = File.join(storage_path, "#{log_name}_data_#{state['entries']}.json")

        sth = ct_request(log_base + '/ct/v1/get-sth')
        return unless sth and sth['tree_size']

        if sth['tree_size'] == state['entries']
          log("#{log_name} is already synchronized with #{state['entries']} entries")
          return
        end

        log("#{log_name} has #{sth['tree_size']} total records available")

        while state['entries'] < (sth['tree_size'] - 1)

          entry_beg = state['entries']
          entry_end = [ state['entries'] + 2000, sth['tree_size'] - 1 ].min

          get_url = log_base + "/ct/v1/get-entries?start=#{entry_beg}&end=#{entry_end}"
          data = ct_request(get_url)
          if not (data && data['entries'])
            fail("#{log_name} returned bad data: #{data.inspect}")
            return
          end

          # Write the CT response data
          File.open(data_file, "ab") do |fd|
            data['entries'].each do |entry|
              fd.puts(entry.to_json)
            end
          end

          state['entries'] += data['entries'].length
          nrecs += data['entries'].length

          # Update the meta file
          File.open(meta_file, "w") do |fd|
            fd.puts(state.to_json)
          end

          log("#{log_name} downloaded #{state['entries']}/#{sth['tree_size']} records")
        end

        # Compress the data file if new records were downloaded
        if nrecs > 0
          log("#{log_name} compressing data file containing #{nrecs} records: #{data_file}")
          system("nice #{gzip_command} #{Shellwords.shellescape(data_file)}")
        end

        log("#{log_name} synchronized with #{nrecs} new entries (#{state['entries']} total)")
      end

      def download
        dir  = storage_path
        FileUtils.mkdir_p(dir)

        ct_logs = config['ct_logs']

        ct_threads = []
        ct_logs.each_pair do |log_name, log_base|
          ct_threads << Thread.new{ ct_sync(log_name, log_base) }
        end

        ct_threads.each {|t| t.join }
      end

      def normalize
        data = storage_path
        norm = File.join(data, "normalized")
        FileUtils.mkdir_p(norm)

        unless inetdata_parsers_available?
          log("The inetdata-parsers tools are not in the execution path, aborting normalization")
          return false
        end

        Dir["#{data}/*_data_*.json.gz"].sort.each do |src|
          dst = File.join(norm, File.basename(src).sub(/\.json\.gz$/, '.mtbl'))
          next if File.exists?(dst)

          host_cmd =
            "nice #{gzip_command} -dc #{Shellwords.shellescape(src)} | " +
            "nice inetdata-ct2mtbl #{Shellwords.shellescape(dst)}"

          log("Processing #{src} with command: #{host_cmd}")
          system(host_cmd)
        end
      end

    end
  end
end
