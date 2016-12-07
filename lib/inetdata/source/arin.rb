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
          name = "#{ftype}.xml"
          url  = "https://www.arin.net/public/secure/downloads/bulkwhois/#{name}?apikey=" + config['arin_api_key']
          dst  = File.join(dir, name)
          log("Dowloading #{dst}")
          download_file(url, dst)
        end
      end

      #
      # Normalize the latest ARIN data
      #
      def normalize
        data = latest_data
        norm = File.join(data, "normalized")
        FileUtils.mkdir_p(norm)

        return

        if File.exists?(File.join(norm, "_normalized_"))
          log("Normalized data is already present for #{data}")
          return
        end

        out_domains = File.join(norm, "domains.txt")
        out_domains_tmp = out_domains + ".tmp"

        out_ipv4 = File.join(norm, "ipv4.txt")
        out_ipv4_tmp = out_ipv4 + ".tmp"

        out_ipv6 = File.join(norm, "ipv6.txt")
        out_ipv6_tmp = out_ipv6 + ".tmp"

        out_hosts = File.join(norm, "hosts.txt")
        out_hosts_tmp = out_hosts + ".tmp"

        out_domains_fd = File.open(out_domains_tmp, "wb")
        out_ipv4_fd = File.open(out_ipv4_tmp, "wb")
        out_ipv6_fd = File.open(out_ipv6_tmp, "wb")
        out_hosts_fd = File.open(out_hosts_tmp, "wb")

        zone_index = 0
        zone_files = Dir["#{data}/*.gz"]
        zone_files.each do |zone_file|
          zone_index += 1
          log("Extracting records from [#{zone_index}/#{zone_files.length}] #{zone_file}...")
          decompress_gzfile(zone_file) do |pipe|
            pipe.each_line do |line|
              bits = line.downcase.strip.split(/\s+/)
              next if (bits[3] == 'nsec3' || bits[3] == 'rrsig')
              bits[0] = bits[0].to_s.sub(/\.$/, '')
              bits[4] = bits[4].to_s.sub(/\.$/, '')

              case bits[3]
                when 'ns'
                  out_domains_fd.puts bits[0]
                  out_hosts_fd.puts bits[4]
                  expand_domains(bits[4]).each do |dom|
                    out_domains_fd.puts dom
                  end

                when 'a'
                  out_ipv4_fd.puts [ bits[4], bits[0] ].join("\t")
                  out_hosts_fd.puts bits[0]
                  expand_domains(bits[0]).each do |dom|
                    out_domains_fd.puts dom
                  end

                when 'aaaa'
                  out_ipv6_fd.puts [ bits[4], bits[0] ].join("\t")
                  out_hosts_fd.puts bits[0]
                  expand_domains(bits[0]).each do |dom|
                    out_domains_fd.puts dom
                  end
              end
            end
          end
        end

        out_domains_fd.close
        out_ipv4_fd.close
        out_ipv6_fd.close
        out_hosts_fd.close

        log("Sorting extracted records from #{data}...")

        uniq_sort_file(out_domains_tmp)
        File.rename(out_domains_tmp, out_domains)

        uniq_sort_file(out_ipv4_tmp)
        File.rename(out_ipv4_tmp, out_ipv4)

        uniq_sort_file(out_ipv6_tmp)
        File.rename(out_ipv6_tmp, out_ipv6)

        uniq_sort_file(out_hosts_tmp)
        File.rename(out_hosts_tmp, out_hosts)

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
