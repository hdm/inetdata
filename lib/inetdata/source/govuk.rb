module InetData
  module Source
    class GOVUK < Base

      def download_file(src, dst)
        tmp   = dst + ".tmp"
        ims   = false
        tries = 0

        begin
          tries += 1
          target = URI.parse(src)
          size   = 0
          csize  = nil

          http = Net::HTTP.new(target.host, target.port)
          if src.index("https") == 0
            http.use_ssl = true
          end

          req = Net::HTTP::Get.new(target.request_uri)

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

            if res.code.to_i >= 500 && res.code.to_i < 600
              raise RuntimeError, "Server Error: #{res.code} #{res.message}"
            end

            if res.code.to_i != 200
              log(" > Skipped downloading of #{dst} due to server response of #{res.code} #{res.message}")
              return true
            end

            log("Download started from #{src} to #{dst}")
            outp = File.open(tmp, "wb")
            res.read_body do |chunk|
              outp.write(chunk)
              size += chunk.length
            end
            outp.close
          end

          File.rename(tmp, dst)

        rescue ::Interrupt
          raise $!
        rescue ::Exception
          if tries < self.max_tries
            log("Download failed: #{src} -> #{dst} : #{$!.class} #{$!}, retrying...")
            sleep(30)
            retry
          else
            fail("Download failed: #{src} -> #{dst} : #{$!.class} #{$!} after #{tries} attempts")
          end
        end
        log("Download completed from #{src} to #{dst}")
      end

      def download_index(url)
        target = URI.parse(url)

        tries = 0
        begin

          tries += 1
          http = Net::HTTP.new(target.host, target.port)
          http.use_ssl = true if url.index("https") == 0

          req = Net::HTTP::Get.new(target.request_uri)
          res = http.request(req)

          unless (res and res.code.to_i == 200 and res.body.to_s.index("List of .gov.uk domain names"))
            if res
              raise RuntimeError.new("Unexpected reply: #{res.code} - #{res['Content-Type']} - #{res.body.inspect}")
            else
              raise RuntimeError.new("Unexpected reply: #{res.code} - #{res['Content-Type']} - #{res.body.inspect}")
            end
          end

          res.body.to_s.scan(/a href=\"(\/[^\"]+)\"/m).map{|m| m.first}.select{|m| m =~ /\.csv$/}

        rescue ::Interrupt
          raise $!
        rescue ::Exception
          if tries < self.max_tries
            log("Index download failed: #{path} #{$!.class} #{$!}, #{$!.backtrace} retrying...")
            sleep(30)
            retry
          else
            fail("Index download failed: #{path} #{$!.class} #{$!} after #{tries} attempts")
          end
        end
      end

      def download
        download_index(config['govuk_domains_base_url']).each do |item|
          targ = URI.parse(config['govuk_domains_base_url'])
          targ.path = item
          file = item.split("/").last
          dir  = storage_path
          dst  = File.join(dir, file)
          FileUtils.mkdir_p(dir)
          download_file(targ.to_s, dst)
        end
      end

      #
      # Normalize the latest data file
      #
      def normalize
        src  = latest_data
        data = File.dirname(src)
        norm = File.join(data, "normalized")
        FileUtils.mkdir_p(norm)

        dst = File.join(norm, "domains.txt")
        tmp = dst + ".tmp"

        if File.exists?(dst) && File.mtime(src) <= File.mtime(dst)
          log("Normalized data is already present for #{src}")
          return
        end

        File.open(tmp, "wb") do |fd|
          File.open(src, "rb") do |r|
            r.read.gsub(/\r\n?/, "\n").each_line do |line|
              next unless line.index(".gov.uk,")
              dname = validate_domain(line.strip.downcase.split(",").first.to_s)
              if dname
                fd.puts dname
              else
                log("Invalid hostname in #{self.name} : #{src} -> #{line.strip}")
              end
            end
          end
        end
        uniq_sort_file(tmp)
        File.rename(tmp, dst)
      end

      #
      # Find the most recent dataset
      #
      def latest_data
        files = Dir["#{storage_path}/*.csv"]
        path  = nil

        Time.now.year.downto(2013) do |year|
          path = files.select{|f| f =~ /_#{year}\.csv$/}.first
          break if path
        end

        path ||= files.first

        if not path
          raise RuntimeError, "No dataset available for #{self.name}"
        end

        path
      end

    end
  end
end
