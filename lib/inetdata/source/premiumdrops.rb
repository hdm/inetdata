module InetData
  module Source
    class PremiumDrops < Base

      def available?
        config['premiumdrops_username'].to_s.length > 0 &&
        config['premiumdrops_password'].to_s.length > 0
      end

      def obtain_session_id(username, password)
        target = URI.parse('https://www.premiumdrops.com/user.php')

        tries = 0
        session_id = nil

        begin
          tries += 1
          http = Net::HTTP.new(target.host, target.port)

          # Unpleasantness due to shoddy SSL configuration at premiumdrops
          http.use_ssl = true
          http.verify_mode = OpenSSL::SSL::VERIFY_NONE

          req = Net::HTTP::Post.new(target.path)
          req.set_form_data({
            'email' => username,
            'password' => password,
            'Submit2' => '  Login  ',
            'a2' => 'login'
          })

          res = http.request(req)

          unless (res and res.code.to_i == 200 and res['Set-Cookie'].to_s =~ /session=([^\s;]+)/)
            if res
              raise RuntimeError.new("#{res.code} #{res.message} #{res['Set-Cookie']} #{res.body}")
            else
              raise RuntimeError.new("No response")
            end
          end

          session_id = $1
        rescue ::Interrupt
          raise $1
        rescue ::Exception
          if tries < self.max_tries
            log("Authentication failed: #{$!.class} #{$!}, retrying...")
            sleep(30)
            retry
          else
            fail("Authentication failed: #{$!.class} #{$!} after #{tries} attempts")
          end
        end

        session_id
      end

      def download_file(session_id, src, dst)
        tmp    = dst + ".tmp"
        target = URI.parse(src)
        size   = 0
        ims    = false

        http = Net::HTTP.new(target.host, target.port)
        req = Net::HTTP::Get.new(target.request_uri)
        req['Cookie'] = 'session=' + session_id

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

      def download
        config['premiumdrops_urls'].each do |url|
          session_id = obtain_session_id(config['premiumdrops_username'], config['premiumdrops_password'])
          target = URI(url)
          params = CGI.parse(target.query)

          date = Time.now.strftime("%Y%m%d")
          path = nil
          zone = params['f'].first
          format = case params['a'].first
            when 'request_full_zone'
              'full'
            when 'request_zone'
              'names'
            when 'request_zone_changes'
              params['t'].first == 'diff' ? 'del' : 'add'
            end

          ext = ['full', 'names'].include?(format) ? '.gz' : ''
          dir = File.expand_path(File.join(storage_path, date))
          dst = File.join(dir,  "#{zone}_#{format}#{ext}")

          FileUtils.mkdir_p(dir)

          # SSL is not enabled for the file download paths (!)
          http = Net::HTTP.new(target.host, target.port)
          req = Net::HTTP::Get.new(target.path + '?' + target.query)
          req['Cookie'] = 'session=' + session_id

          res = http.request(req)
          unless res['Location']
            fail("No redirect for download of #{url}")
          end

          log("Dowloading #{dst}")
          download_file(session_id, 'http://www.premiumdrops.com/' + res['Location'], dst)
        end
      end


      #
      # Normalize the latest CZDS zones
      #
      def normalize
        data = latest_data
        norm = File.join(data, "normalized")
        FileUtils.mkdir_p(norm)

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
        zone_files = Dir["#{data}/*_full.gz"]
        zone_files.each do |zone_file|

          zone_index += 1
          log("Extracting records from [#{zone_index}/#{zone_files.length}] #{zone_file}...")
          origin = zone_file.split('/').last.split("_").first

          case origin
          when "sk"
            File.open(zone_file, "rb") do |input|
              # 1000hier.sk;IPEK-0001;HELE-0056;NEW;DOM_OK;ns.webglobe.sk;ns2.webglobe.sk;ns3.webglobe.sk;;;19.12.2011
              input.each_line do |line|
                bits = line.downcase.strip.split(/;/)
                next unless bits[0] =~ /\.sk$/
                nameservers = bits[5,4].select{|x| x.to_s.length > 0}
                out_domains_fd.puts bits[0]
                nameservers.each do |ns|
                  out_hosts_fd.puts ns
                  expand_domains(ns).each do |dom|
                    out_domains_fd.puts dom
                  end
                end
              end
            end
          when "biz", "xxx"
            decompress_gzfile(zone_file) do |pipe|
              pipe.each_line do |line|
                # 01O.BIZ.                7200    IN      NS      NS1.DSREDIRECTS.COM.
                bits = line.downcase.strip.split(/\s+/)
                next unless bits[4]

                bits[0] = bits[0].sub(/\.$/, '')
                bits[4] = bits[4].sub(/\.$/, '')

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
          when "us"
            decompress_gzfile(zone_file) do |pipe|
              pipe.each_line do |line|
                # NS4.007POKER IN A 136.243.106.135
                # no trailing dot means append origin
                bits = line.downcase.strip.split(/\s+/)
                next unless bits[1] == "in"

                if bits[0][-1, 1] != "."
                  bits[0] << ".us"
                end

                bits[0] = bits[0].sub(/\.$/, '')
                bits[3] = bits[3].sub(/\.$/, '')

                case bits[2]
                  when 'ns'
                    out_domains_fd.puts bits[0]
                    out_hosts_fd.puts bits[3]
                    expand_domains(bits[3]).each do |dom|
                      out_domains_fd.puts dom
                    end

                  when 'a'
                    out_ipv4_fd.puts [ bits[3], bits[0] ].join("\t")
                    out_hosts_fd.puts bits[0]
                    expand_domains(bits[0]).each do |dom|
                      out_domains_fd.puts dom
                    end

                  when 'aaaa'
                    out_ipv6_fd.puts [ bits[3], bits[0] ].join("\t")
                    out_hosts_fd.puts bits[0]
                    expand_domains(bits[0]).each do |dom|
                      out_domains_fd.puts dom
                    end
                end
              end
            end
          # The rest of the zones are fairly standard: com, info, mobi, net, org
          # No trailing dot means append origin to the label
          else
            decompress_gzfile(zone_file) do |pipe|
              pipe.each_line do |line|

                bits = line.downcase.strip.split(/\s+/)
                next unless bits.length == 3
                next unless %w{ns a aaaa}.include?(bits[1])

                if bits[0][-1, 1] != "."
                  bits[0] << ".#{origin}"
                end

                if bits[2][-1, 1] != "."
                  bits[2] << ".#{origin}"
                end

                bits[0] = bits[0].sub(/\.$/, '')
                bits[2] = bits[2].sub(/\.$/, '')

                case bits[1]
                  when 'ns'
                    out_domains_fd.puts bits[0]
                    out_hosts_fd.puts bits[2]
                    expand_domains(bits[2]).each do |dom|
                      out_domains_fd.puts dom
                    end

                  when 'a'
                    out_ipv4_fd.puts [ bits[2], bits[0] ].join("\t")
                    out_hosts_fd.puts bits[0]
                    expand_domains(bits[0]).each do |dom|
                      out_domains_fd.puts dom
                    end

                  when 'aaaa'
                    out_ipv6_fd.puts [ bits[2], bits[0] ].join("\t")
                    out_hosts_fd.puts bits[0]
                    expand_domains(bits[0]).each do |dom|
                      out_domains_fd.puts dom
                    end
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
