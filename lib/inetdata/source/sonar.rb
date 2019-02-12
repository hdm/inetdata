module InetData
  module Source
    class Sonar < Base

      def download_file(src, dst,redirect_count=0)
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

          if [301, 302].include?(res.code.to_i)

            if redirect_count > 3
              log(" > Skipped downloading of #{dst} due to rediret count being over limit: #{redirect_count}")
              return true
            end

            new_src = res['Location'].to_s

            if new_src.length == 0
              log(" > Skipped downloading of #{dst} due to server redirect with no location")
              return true
            end

            log(" > Download of #{src} moved to #{new_src}...")
            return download_file(new_src, dst, redirect_count + 1)
          end

          if res.code.to_i != 200
            log(" > Skipped downloading of #{dst} due to server response of #{res.code} #{res.message} #{res['Location']}")
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

      def download_index(dset)
        target = URI.parse(config['sonar_base_url'] + dset)
        tries  = 0
        begin

          tries += 1
          http   = Net::HTTP.new(target.host, target.port)
          http.use_ssl = true

          req = Net::HTTP::Get.new(target.request_uri)
          res = http.request(req)

          unless (res and res.code.to_i == 200 and res.body.to_s.index('SHA1-Fingerprint'))
            if res
              raise RuntimeError.new("Unexpected reply: #{res.code} - #{res['Content-Type']} - #{res.body.inspect}")
            else
              raise RuntimeError.new("Unexpected reply: #{res.code} - #{res['Content-Type']} - #{res.body.inspect}")
            end
          end

          links = []
          res.body.scan(/href=\"#{dset}(\d+\-\d+\-\d+\-\d+\-[^\"]+)\"/).each do |link|
            link = link.first
            if link =~ /\.json.gz/
              links << ( config['sonar_base_url'] + link )
            end
          end

          return links

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

      def download_fdns_index
        download_index('/sonar.fdns_v2/')
      end

      def download_rdns_index
        download_index('/sonar.rdns_v2/')
      end

      def download
        dir  = storage_path
        FileUtils.mkdir_p(dir)

        fdns_links = download_fdns_index
        rdns_links = download_rdns_index

        queue = []
        queue += rdns_links
        queue += fdns_links

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
            o = f.sub(".gz", ".mtbl.tmp")
            mtbl_cmd = "nice #{gzip_command} -dc #{Shellwords.shellescape(f)} | inetdata-dns2mtbl -t #{get_tempdir} -m #{(get_total_ram/8.0).to_i} #{o}"
            log("Running #{mtbl_cmd}")
            system(mtbl_cmd)
            File.rename(o, o.gsub(/\.tmp$/, ''))
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
            o = f.sub(".gz", ".mtbl.tmp")
            mtbl_cmd = "nice #{gzip_command} -dc #{Shellwords.shellescape(f)} | inetdata-dns2mtbl -t #{get_tempdir} -m #{(get_total_ram/8.0).to_i} #{o}"
            log("Running #{mtbl_cmd}")
            system(mtbl_cmd)
            File.rename(o, o.gsub(/\.tmp$/, ''))
          end
        end

      end

      #
      # Find the most recent dataset
      #
      def latest_data(dtype)
        path = Dir["#{storage_path}/*#{dtype}.gz"].sort { |a,b|
          File.basename(b).split(/[^\d\-]+/).first.gsub("-", '')[0,8].to_i <=>
          File.basename(a).split(/[^\d\-]+/).first.gsub("-", '')[0,8].to_i
        }.first

        if not path
          raise RuntimeError, "No #{dtype} dataset available for #{self.name}"
        end

        path
      end

      def latest_fdns_data
        latest_data("-fdns*.json")
      end

      def latest_rdns_data
        latest_data("-rdns.json")
      end

      #
      # Find the most recent normalized dataset
      #
      def latest_normalized_data(dtype)
        path = Dir["#{storage_path}/normalized/*#{dtype}"].sort { |a,b|
          File.basename(b).split(/[^\d\-]+/).first.gsub("-", '')[0,8].to_i <=>
          File.basename(a).split(/[^\d\-]+/).first.gsub("-", '')[0,8].to_i
        }.first

        if not path
          raise RuntimeError, "No #{dtype} normalized_dataset available for #{self.name}"
        end

        path
      end

      def latest_normalized_fdns_names_mtbl
        latest_normalized_data("-fdns-names.mtbl")
      end

      def latest_normalized_fdns_names_inverse_mtbl
        latest_normalized_data("-fdns-names-inverse.mtbl")
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
