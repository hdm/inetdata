module InetData
  module Source
    class RIR < Base

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

      def download
        config['rir_delegation_urls'].each do |url|
          targ = URI.parse(url)
          file = url.split("/").last
          date = Time.now.strftime("%Y%m%d")
          dir  = File.expand_path(File.join(storage_path, date))
          dst  = File.join(dir, file)
          FileUtils.mkdir_p(dir)
          download_file(url, dst)
        end
      end

      #
      # RIR files are considered already normalized
      #
      def normalize
      end

    end
  end
end
