module InetData
  module Source
    class WhoisXMLAPI < Base

      def available?
        config['whoisxmlapi_username'].to_s.length > 0 &&
        config['whoisxmlapi_password'].to_s.length > 0
      end

      def download_index(prefix)
        target = URI.parse(config['whoisxmlapi_base_url'] + prefix)

        tries = 0
        begin

          tries += 1
          http = Net::HTTP.new(target.host, target.port)
          http.use_ssl = true if config['whoisxmlapi_base_url'].index("https") == 0

          req = Net::HTTP::Get.new(target.request_uri)
          req.basic_auth(config['whoisxmlapi_username'], config['whoisxmlapi_password'])

          res = http.request(req)

          unless (res and res.code.to_i == 200 and res.body.to_s.index("Last modified"))
            if res
              raise RuntimeError.new("Unexpected reply: #{res.code} - #{res['Content-Type']} - #{res.body.inspect}")
            else
              raise RuntimeError.new("Unexpected reply: #{res.code} - #{res['Content-Type']} - #{res.body.inspect}")
            end
          end

          res.body.to_s.scan(/a href=\"((\d+_\d+_\d+_|full_)[^\"]+)\"/m).map{|m| m.first}

        rescue ::Interrupt
          raise $!
        rescue ::Exception
          if tries < self.max_tries
            log("Index download failed: #{prefix} #{$!.class} #{$!}, #{$!.backtrace} retrying...")
            sleep(30)
            retry
          else
            fail("Index download failed: #{prefix} #{$!.class} #{$!} after #{tries} attempts")
          end
        end
      end

      def download_file(src, dst)
        target = URI.parse(src)
        size   = 0
        ims    = false
        tmp    = dst + ".tmp"

        http = Net::HTTP.new(target.host, target.port)
        http.use_ssl = true if src.index("https") == 0

        req = Net::HTTP::Get.new(target.request_uri)
        req.basic_auth(config['whoisxmlapi_username'], config['whoisxmlapi_password'])

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
        config['whoisxmlapi_datasets'].each_pair do |dname,prefix|
          files = download_index(prefix)
          files.each do |fname|
            url = config['whoisxmlapi_base_url'] + prefix + fname

            target  = URI.parse(url)

            tld  = nil
            date = nil

            case fname
            when /^full_(\d+_\d+_\d+)_(.*)\.csv\.gz/
              tld  = $2
              date = $1.gsub(/[^[0-9]]/, '')
            when /^full_(.*)_(\d+[_\-]\d+[\-_]\d+).csv.gz/
              tld = $1
              date = $2.gsub(/[^[0-9]]/, '')
            when /^(\d+_\d+_\d+)_(.*)\.csv\.gz/
              tld  = $2
              date = $1.gsub(/[^[0-9]]/, '')
            else
              log("Unknown file name format: #{fname}")
              next
            end

            dir  = File.expand_path(File.join(storage_path, dname, date))
            dst  = File.join(dir, fname.gsub("/", ""))

            FileUtils.mkdir_p(dir)

            log("Dowloading #{dst}")
            download_file(url, dst)
          end
        end
      end

    end
  end
end
