module InetData

  module Source
    class ARIN < Base

      def available?
        config['arin_api_key'].to_s.length > 0
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
        date = Time.now.strftime("%Y%m%d")
        dir  = File.expand_path(File.join(storage_path, date))
        FileUtils.mkdir_p(dir)

	      %W{ nets asns orgs pocs }.each do |ftype|
          name   = "#{ftype}.xml"
          url    = "https://www.arin.net/public/secure/downloads/bulkwhois/#{name}?apikey=" + config['arin_api_key']
          dst    = File.join(dir, name)
          dst_gz = dst + ".gz"
          tmp    = dst + ".tmp"
          tmp_gz = "#{tmp}.gz"

          if File.exists?(dst_gz)
            log("File already exists, skipping: #{dst_gz}")
            next
          end

          log("Dowloading #{dst}")
          download_file(url, dst)
          cmd = "nice pigz #{tmp}"
          log("Running #{cmd}\n")
          system(cmd)
          FileUtils.rename(tmp_gz, dst_gz)
        end
      end

      #
      # Normalize the latest ARIN data
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

        %W{ nets asns orgs pocs }.each do |ftype|
          cmd = "nice pigz -d #{data}/#{ftype}.xml.gz | nice inetdata-arin-xml2json /dev/stdin | nice pigz -c > #{norm}/#{ftype}.json.gz"
          log("Running #{cmd}\n")
          system(cmd)
        end

        %W{ nets asns orgs pocs }.each do |ftype|
          cmd = "nice pigz -d #{data}/#{ftype}.xml.gz | nice inetdata-arin-xml2csv /dev/stdin | nice pigz -c > #{norm}/#{ftype}.csv.gz"
          log("Running #{cmd}\n")
          system(cmd)
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
