module InetData
  module Source
    class Sonar < Base

      def download_file(src, dst)
        tmp    = dst + ".tmp"
        target = URI.parse(src)
        size   = 0
        ims    = false
        http   = Net::HTTP.new(target.host, target.port)

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

      def download_index
        target = URI.parse(config['sonar_base_url'] + '/json')
        tries  = 0
        begin

          tries += 1
          http   = Net::HTTP.new(target.host, target.port)
          http.use_ssl = true

          req = Net::HTTP::Get.new(target.request_uri)
          res = http.request(req)

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
            log("Index failed: #{$!.class} #{$!}, retrying...")
            sleep(30)
            retry
          else
            fail("Index failed: #{$!.class} #{$!} after #{tries} attempts")
          end
        end
      end

      def download
        meta = download_index
        dir  = storage_path
        FileUtils.mkdir_p(dir)
        queue = []
        if meta['studies']

          fdns = meta['studies'].select{|x| x['uniqid'] == 'sonar.fdns_v2' }
          if fdns && fdns.last && fdns.last['files'] && fdns.last['files'].last && fdns.last['files'].last['name']
            queue << fdns.last['files'].last['name']
          end

          rdns = meta['studies'].select{|x| x['uniqid'] == 'sonar.rdns_v2' }
          if rdns && rdns.last && rdns.last['files'] && rdns.last['files'].last && rdns.last['files'].last['name']
            queue << rdns.last['files'].last['name']
          end

          ssl = meta['studies'].select{|x| x['uniqid'] == 'sonar.ssl' }
          if ssl && ssl.last && ssl.last['files']
            names = ssl.last['files'].select{|x| x['name'].to_s =~ /_names.gz$/}
            if names && names.last && names.last['name']
              queue << names.last['name']
            end
          end

          sslm = meta['studies'].select{|x| x['uniqid'] == 'sonar.moressl' }
          if sslm && ssl.last && sslm.last['files']
            names = sslm.last['files'].select{|x| x['name'].to_s =~ /_names.gz$/}
            last_date = names.map{|x| x['name'].split("/").last.split("_").first.to_i }.sort.last.to_s
            names.select{|x| x['name'].index("/#{last_date}_")}.each do |f|
              queue << f['name']
            end
          end

        end

        queue.each do |url|
          dst = File.join(dir, url.split("/").last)
          download_file(url, dst)
        end
      end

      def normalize
        data = storage_path
        norm = File.join(data, "normalized")
        FileUtils.mkdir_p(norm)

        unless inetdata_parsers_available?
          log("The inetdata-parsers tools are not in the execution path, aborting normalization")
          return false
        end

        fdns_file = latest_fdns_data
        fdns_mtbl = File.join(norm, File.basename(fdns_file).sub(".json.gz", "-names-inverse.mtbl"))

        rdns_file = latest_rdns_data
        rdns_mtbl = File.join(norm, File.basename(rdns_file).sub(".json.gz", "-names-inverse.mtbl"))

        if File.exists?(fdns_mtbl) && File.size(fdns_mtbl) > 0
          log("Normalized data is already present for FDNS #{data} at #{fdns_mtbl}")
        else
          output_base = File.join(norm, File.basename(fdns_file).sub(".json.gz", ""))

          csv_cmd = "nice #{gzip_command} -dc #{Shellwords.shellescape(fdns_file)} | nice inetdata-sonardnsv2-split -t #{get_tempdir} -m #{(get_total_ram/8.0).to_i} #{output_base}"
          log("Running #{csv_cmd}")
          system(csv_cmd)
          [
            "#{output_base}-names.gz",
            "#{output_base}-names-inverse.gz"
          ].each do |f|
            o = f.sub(".gz", ".mtbl")
            mtbl_cmd = "nice #{gzip_command} -dc #{Shellwords.shellescape(f)} | inetdata-dns2mtbl -t #{get_tempdir} -m #{(get_total_ram/8.0).to_i} #{o}"
            log("Running #{mtbl_cmd}")
            system(mtbl_cmd)
          end
        end

        if File.exists?(rdns_mtbl) && File.size(rdns_mtbl) > 0
          log("Normalized data is already present for RDNS #{data} at #{rdns_mtbl}")
        else
          output_base = File.join(norm, File.basename(rdns_file).sub(".json.gz", ""))

          csv_cmd = "nice #{gzip_command} -dc #{Shellwords.shellescape(rdns_file)} | nice inetdata-sonardnsv2-split -t #{get_tempdir} -m #{(get_total_ram/8.0).to_i} #{output_base}"
          log("Running #{csv_cmd}")
          system(csv_cmd)
          [
            "#{output_base}-names.gz",
            "#{output_base}-names-inverse.gz"
          ].each do |f|
            o = f.sub(".gz", ".mtbl")
            mtbl_cmd = "nice #{gzip_command} -dc #{Shellwords.shellescape(f)} | inetdata-dns2mtbl -t #{get_tempdir} -m #{(get_total_ram/8.0).to_i} #{o}"
            log("Running #{mtbl_cmd}")
            system(mtbl_cmd)
          end
        end

      end

      #
      # Find the most recent dataset
      #
      def latest_data(dtype)
        path = Dir["#{storage_path}/*#{dtype}.gz"].sort { |a,b|
          File.basename(b).split(/[^\d]+/).first.to_i <=>
          File.basename(a).split(/[^\d]+/).first.to_i
        }.first

        if not path
          raise RuntimeError, "No #{dtype} dataset available for #{self.name}"
        end

        path
      end

      def latest_fdns_data
        latest_data("-fdns.json")
      end

      def latest_rdns_data
        latest_data("-rdns.json")
      end

      #
      # Find the most recent normalized dataset
      #
      def latest_normalized_data(dtype)
        path = Dir["#{storage_path}/normalized/*#{dtype}"].sort { |a,b|
          File.basename(b).split(/[^\d]+/).first.to_i <=>
          File.basename(a).split(/[^\d]+/).first.to_i
        }.first

        if not path
          raise RuntimeError, "No #{dtype} normalized_dataset available for #{self.name}"
        end

        path
      end

      def latest_normalized_fdns_names_mtbl
        latest_normalized_data("_fdns-names.mtbl")
      end

      def latest_normalized_fdns_names_inverse_mtbl
        latest_normalized_data("_fdns-names-inverse.mtbl")
      end

      def latest_normalized_rdns_names_mtbl
        latest_normalized_data("-rdns.mtbl")
      end

      def latest_normalized_rdns_names_inverse_mtbl
        latest_normalized_data("-rdns-inverse.mtbl")
      end
    end
  end
end
