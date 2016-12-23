module InetData
  module Source
    class PremiumDrops < Base

      def available?
        config['premiumdrops_username'].to_s.length > 0 &&
        config['premiumdrops_password'].to_s.length > 0
      end

      def obtain_session_id(username, password)
        target = URI.parse('https://www.premiumdrops.com/user.php')

        tries = 0
        session_id = nil

        begin
          tries += 1
          http = Net::HTTP.new(target.host, target.port)

          # Unpleasantness due to shoddy SSL configuration at premiumdrops
          http.use_ssl = true
          http.verify_mode = OpenSSL::SSL::VERIFY_NONE

          req = Net::HTTP::Post.new(target.path)
          req.set_form_data({
            'email' => username,
            'password' => password,
            'Submit2' => '  Login  ',
            'a2' => 'login'
          })

          res = http.request(req)

          unless (res and res.code.to_i == 200 and res['Set-Cookie'].to_s =~ /session=([^\s;]+)/)
            if res
              raise RuntimeError.new("#{res.code} #{res.message} #{res['Set-Cookie']} #{res.body}")
            else
              raise RuntimeError.new("No response")
            end
          end

          session_id = $1
        rescue ::Interrupt
          raise $1
        rescue ::Exception
          if tries < self.max_tries
            log("Authentication failed: #{$!.class} #{$!}, retrying...")
            sleep(30)
            retry
          else
            fail("Authentication failed: #{$!.class} #{$!} after #{tries} attempts")
          end
        end

        session_id
      end

      def download_file(session_id, src, dst)
        tmp    = dst + ".tmp"
        target = URI.parse(src)
        size   = 0
        ims    = false

        http = Net::HTTP.new(target.host, target.port)
        req = Net::HTTP::Get.new(target.request_uri)
        req['Cookie'] = 'session=' + session_id

        if File.exists?(dst)
          req['If-Modified-Since'] = File.stat(dst).mtime.rfc2822
          ims = true
        end

        http.request(req) do |res|

          if ims && res.code.to_i == 304
            log(" > Skipped downloading of #{dst} due to not modified response")
            return true
          end

          if ims && res['Content-Length']
            if res['Content-Length'].to_i == File.size(dst)
              log(" > Skipped downloading of #{dst} with same size of #{res['Content-Length']} bytes")
              return true
            end
          end

          if res.code.to_i != 200
            log(" > Skipped downloading of #{dst} due to server response of #{res.code} #{res.message}")
            return true
          end

          outp = File.open(tmp, "wb")
          res.read_body do |chunk|
            outp.write(chunk)
            size += chunk.length
          end
          outp.close
        end

        File.rename(tmp, dst)

        log(" > Downloading of #{dst} completed with #{size} bytes")
      end

      def download
        config['premiumdrops_urls'].each do |url|
          session_id = obtain_session_id(config['premiumdrops_username'], config['premiumdrops_password'])
          target = URI(url)
          params = CGI.parse(target.query)

          date = Time.now.strftime("%Y%m%d")
          path = nil
          zone = params['f'].first
          format = case params['a'].first
            when 'request_full_zone'
              'full'
            when 'request_zone'
              'names'
            when 'request_zone_changes'
              params['t'].first == 'diff' ? 'del' : 'add'
            end

          ext = ['full', 'names'].include?(format) ? '.gz' : ''
          dir = File.expand_path(File.join(storage_path, date))
          dst = File.join(dir,  "#{zone}_#{format}#{ext}")

          FileUtils.mkdir_p(dir)

          # SSL is not enabled for the file download paths (!)
          http = Net::HTTP.new(target.host, target.port)
          req = Net::HTTP::Get.new(target.path + '?' + target.query)
          req['Cookie'] = 'session=' + session_id

          res = http.request(req)
          unless res['Location']
            fail("No redirect for download of #{url}")
          end

          log("Dowloading #{dst}")
          download_file(session_id, 'http://www.premiumdrops.com/' + res['Location'], dst)
        end
      end

      #
      # Normalize the latest premium drops zones
      #
      def normalize
        data = latest_data
        norm = File.join(data, "normalized")
        FileUtils.mkdir_p(norm)

        if File.exists?(File.join(norm, "_normalized_"))
          log("Normalized data is already present for #{data}")
          return true
        end

        unless inetdata_parsers_available?
          log("The inetdata-parsers tools are not in the execution path, aborting normalization")
          return false
        end

        zone_index = 0
        zone_files = Dir["#{data}/*_full.gz"]
        zone_files.each do |zone_file|

          zone_index += 1
          log("Extracting records from [#{zone_index}/#{zone_files.length}] #{zone_file}...")
          origin = zone_file.split('/').last.split("_").first

          csv_cmd = "nice " +
            ((origin == "sk") ? "cat" : "#{gzip_command} -dc") + " #{Shellwords.shellescape(zone_file)} | " +
            "nice inetdata-zone2csv | " +
            "nice inetdata-csvsplit -t #{get_tempdir} -m #{(get_total_ram/4.0).to_i} #{norm}/#{origin}"

          log("Running #{csv_cmd}\n")
          system(csv_cmd)

          [
            "#{norm}/#{origin}-names.gz",
            "#{norm}/#{origin}-names-inverse.gz"
          ].each do |f|
            o = f.sub(".gz", ".mtbl")
            mtbl_cmd = "nice #{gzip_command} -dc #{Shellwords.shellescape(f)} | nice inetdata-dns2mtbl -t #{get_tempdir} -m #{(get_total_ram/4.0).to_i} #{o}"
            log("Running #{mtbl_cmd}")
            system(mtbl_cmd)
          end
        end
        File.open(File.join(norm, "_normalized_"), "wb") {|fd|}
      end

      #
      # Find the most recent dataset
      #
      def latest_data
        path = Dir["#{storage_path}/[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]"].
               sort{|a,b| b.split("/")[-1].to_i <=> a.split("/")[-1].to_i}.
               first

        if not path
          raise RuntimeError, "No dataset available for #{self.name}"
        end

        path
      end

    end
  end
end
