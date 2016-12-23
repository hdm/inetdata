module InetData
  module Source
    class CZDS < Base

      def available?
        config['czds_token'].to_s.length > 0
      end

      def download_zone_list
        target = URI.parse(config['czds_base_url'] + '/en/user-zone-data-urls.json?token=' + config['czds_token'])

        tries = 0
        begin

          tries += 1
          http = Net::HTTP.new(target.host, target.port)
          http.use_ssl = true

          req = Net::HTTP::Get.new(target.request_uri)
          res = http.request(req)

          unless (res and res.code.to_i == 200 and res['Content-Type'] == 'application/json')
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
            log("Zone list failed: #{$!.class} #{$!}, retrying...")
            sleep(30)
            retry
          else
            fail("Zone list failed: #{$!.class} #{$!} after #{tries} attempts")
          end
        end
      end

      def download_file(src, dst)
        tmp    = dst + ".tmp"
        target = URI.parse(src)
        size   = 0
        ims    = false
        http   = Net::HTTP.new(target.host, target.port)
        http.use_ssl = true

        req = Net::HTTP::Get.new(target.request_uri)

        if File.exists?(dst)
          req['If-Modified-Since'] = File.stat(dst).mtime.rfc2822
          ims = true
        end

        # Short-circuit the download if the local file exists due to the number of files
        if ims
          log(" > Skipped downloading of #{dst} due to existing file on disk")
          return true
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
        zones = download_zone_list
        zones.each do |url|
          unless url.index('http') == 0
            url = config['czds_base_url'] + url
          end

          target  = URI.parse(url)
          zone_id = target.path.split("/").last

          date = Time.now.strftime("%Y%m%d")
          ext  = ".txt.gz"
          dir  = File.expand_path(File.join(storage_path, date))
          dst  = File.join(dir, "#{zone_id}#{ext}")

          FileUtils.mkdir_p(dir)

          log("Dowloading #{dst}")
          download_file(url, dst)
        end
      end

      #
      # Normalize the latest CZDS zones
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

        csv_cmd = "nice #{gzip_command} -dc #{data}/*.gz | " +
          "nice inetdata-zone2csv | " +
          "nice inetdata-csvsplit -t #{get_tempdir} -m #{(get_total_ram/4.0).to_i} #{norm}/czds"

        log("Running #{csv_cmd}\n")
        system(csv_cmd)

        [
          "#{norm}/czds-names.gz",
          "#{norm}/czds-names-inverse.gz"
        ].each do |f|
          o = f.sub(".gz", ".mtbl")
          mtbl_cmd = "nice #{gzip_command} -dc #{Shellwords.shellescape(f)} | inetdata-dns2mtbl -t #{get_tempdir} -m #{(get_total_ram/4.0).to_i} #{o}"
          log("Running #{mtbl_cmd}")
          system(mtbl_cmd)
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
