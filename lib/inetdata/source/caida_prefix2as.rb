module InetData
  module Source
    class CAIDA_Prefix2AS < Base

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

          # Invalid SSL certificate as of 12/1/2016
          if src.index("https") == 0
            http.use_ssl = true
            http.verify_mode = OpenSSL::SSL::VERIFY_NONE
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

          # Invalid SSL certificate as of 12/1/2016
          if url.index("https") == 0
            http.use_ssl = true
            http.verify_mode = OpenSSL::SSL::VERIFY_NONE
          end

          req = Net::HTTP::Get.new(target.request_uri)
          res = http.request(req)

          unless (res and res.code.to_i == 200 and res.body.to_s.index("Index of /"))
            if res
              raise RuntimeError.new("Unexpected reply: #{res.code} - #{res['Content-Type']} - #{res.body.inspect}")
            else
              raise RuntimeError.new("Unexpected reply: #{res.code} - #{res['Content-Type']} - #{res.body.inspect}")
            end
          end

          res.body.to_s.scan(/a href=\"(routeviews[^\"]+)\"/m).map{|m| m.first}

        rescue ::Interrupt
          raise $!
        rescue ::Exception
          if tries < self.max_tries
            log("Index download failed: #{url} #{$!.class} #{$!}, #{$!.backtrace} retrying...")
            sleep(30)
            retry
          else
            fail("Index download failed: #{url} #{$!.class} #{$!} after #{tries} attempts")
          end
        end
      end

      def download
        path = Time.now.strftime("/%Y/%m/")
        ipv4 = config['caida_prefix2as_ipv4_base_url'] + path
        ipv6 = config['caida_prefix2as_ipv6_base_url'] + path

        [ipv4, ipv6].each do |rindex|
          download_index(rindex).each do |item|
            url  = rindex + item
            targ = URI.parse(url)
            file = targ.path.split("/").last
            date = Time.now.strftime("%Y%m")
            dir  = File.expand_path(File.join(storage_path, date))
            dst  = File.join(dir, file)
            FileUtils.mkdir_p(dir)
            download_file(url, dst)
          end
        end
      end

    end
  end
end
