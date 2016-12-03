module InetData
  module Source
    class GOV < Base

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

      #
      # Download the latest data file
      #
      def download
        url = config['gov_domains_url']
        targ = URI.parse(url)
        file = datafile_name
        date = Time.now.strftime("%Y%m%d")
        dir  = File.expand_path(File.join(storage_path, date))
        dst  = File.join(dir, file)
        FileUtils.mkdir_p(dir)
        download_file(url, dst)
      end

      #
      # Normalize the latest data file
      #
      def normalize
        data = latest_data
        norm = File.join(data, "normalized")
        FileUtils.mkdir_p(norm)

        if File.exists?(File.join(norm, "domains.txt"))
          log("Normalized data is already present for #{data}")
          return
        end

        src = File.join(data, datafile_name)
        dst = File.join(norm, "domains.txt")
        tmp = dst + ".tmp"

        File.open(tmp, "wb") do |fd|
          File.open(src, "rb") do |r|
            r.each_line do |line|
              next if line =~ /^Domain Name,/
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
        path = Dir["#{storage_path}/[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]/#{datafile_name}"].
               sort{|a,b| b.split("/")[-2].to_i <=> a.split("/")[-2].to_i}.
               first

        if not path
          raise RuntimeError, "No dataset available for #{self.name}"
        end

        File.dirname(path)
      end

      #
      # The local name of the data file
      #
      def datafile_name
        "current-full.csv"
      end

    end
  end
end
