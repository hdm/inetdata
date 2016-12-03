module InetData
  module Source
    class WWWSIO < Base

      def available?
        config['wwwsio_username'].to_s.length > 0 &&
        config['wwwsio_password'].to_s.length > 0
      end

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
        %W{ full new deleted }.each do |list|
          url = config['wwwsio_base_url'] + "/#{list}/all_zones/#{(config['wwwsio_username'])}/#{(config['wwwsio_password'])}"
          targ = URI.parse(url)
          file = "all_zones_#{list}.txt"
          date = Time.now.strftime("%Y%m%d")
          dir  = File.expand_path(File.join(storage_path, date))
          dst  = File.join(dir, file)
          FileUtils.mkdir_p(dir)
          download_file(url, dst)
        end
      end

      #
      # Normalize the latest data file
      #
      def normalize
        data = latest_data
        norm = File.join(data, "normalized")
        FileUtils.mkdir_p(norm)

        %W{ full new deleted }.each do |list|
          src = File.join(data, "all_zones_#{list}.txt")
          dst = File.join(norm, "all_zones_#{list}.txt")
          tmp = dst + ".tmp"

          if File.exists?(dst)
            log("Normalized data is already present for #{data}")
            return
          end

          FileUtils.cp(src, tmp)
          uniq_sort_file(tmp)
          File.rename(tmp, dst)
        end

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
