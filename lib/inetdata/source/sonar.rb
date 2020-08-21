require 'typhoeus'

module InetData
  module Source
    class Sonar < Base

      def download_files(queue)
        hydra = Typhoeus::Hydra.hydra
        dir   = storage_path
        FileUtils.mkdir_p(dir)

        queue.each do |url|
          filename = File.join(dir, url.split("/").last.split("?").first)
          dst = File.open(filename, 'wb')
          req = Typhoeus::Request.new(url, followlocation: true)

          req.on_headers do |res|
            raise "Request failed: #{url}" unless res.code == 200
          end

          req.on_body do |chunk|
            dst.write(chunk)
          end

          req.on_complete do |res|
            dst.close
            size = File.size(filename)
            log(" > Downloading of #{filename} completed with #{size} bytes")
          end

          hydra.queue req
        end

        hydra.run
      end

      def download_index(dset)
        unless config['sonar_api_key'].strip.empty?
          based_url = config['sonar_api_base_url'] + dset
          target = URI.parse(based_url)
        else
          target = URI.parse(config['sonar_base_url'] + dset)
        end

        tries  = 0
        begin

          #
          # Acquire a listing of the dataset archives
          #
          tries += 1
          http   = Net::HTTP.new(target.host, target.port)
          http.use_ssl = true

          req = Net::HTTP::Get.new(target.request_uri)
          req['X-Api-Key'] = config['sonar_api_key'] unless config['sonar_api_key'].strip.empty?
          res = http.request(req)

          links = []
          if !config['sonar_api_key'].strip.empty?
            unless (res and res.code.to_i == 200 and res.body)
              raise RuntimeError.new("Unexpected 'studies' API reply: #{res.code} - #{res['Content-Type']} - #{res.body.inspect}")
            end

            #
            # Find the newest archives
            #
            archives = {}
            if dset.include? 'rdns'
              archives['rdns'] = JSON.parse(res.body)['sonarfile_set'].shift
            else
              JSON.parse(res.body)['sonarfile_set'].each do |archive|
                next unless archive.include? '_'
                record = (archive.split /_|\.json\.gz/).last
                archives[record] = archive unless archives[record]
              end
            end

            #
            # Generate a download URL for a file (https://opendata.rapid7.com/apihelp/)
            #
            archives.values.each do |filename|
              target  = URI.parse("#{based_url}#{filename}/download/")
              http    = Net::HTTP.new(target.host, target.port)
              http.use_ssl = true

              req = Net::HTTP::Get.new(target.request_uri)
              req['X-Api-Key'] = config['sonar_api_key']
              res = http.request(req)

              unless (res and res.code.to_i == 200 and res.body)
                raise RuntimeError.new("Unexpected 'download' API reply: #{res.code} - #{res['Content-Type']} - #{res.body.inspect}")
              end

              links << ( JSON.parse(res.body)['url'] )
            end
          else
            unless (res and res.code.to_i == 200 and res.body.to_s.index('SHA1-Fingerprint'))
              raise RuntimeError.new("Unexpected reply: #{res.code} - #{res['Content-Type']} - #{res.body.inspect}")
            end

            res.body.scan(/href=\"(#{dset}\d+\-\d+\-\d+\-\d+\-[^\"]+)\"/).each do |link|
              link = link.first
              if link =~ /\.json.gz/
                links << ( config['sonar_base_url'] + link )
              end
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
        fdns_links = download_fdns_index
        rdns_links = download_rdns_index

        queue = []
        queue += rdns_links
        queue += fdns_links

        download_files(queue)
      end

      def normalize
        data = storage_path
        norm = File.join(data, "normalized")
        FileUtils.mkdir_p(norm)

        unless inetdata_parsers_available?
          log("The inetdata-parsers tools are not in the execution path, aborting normalization")
          return false
        end

        sonar_files = sonar_datafiles
        sonar_files.each do |sonar_file|
          sonar_mtbl = File.join(norm, File.basename(sonar_file).sub(".json.gz", "-names-inverse.mtbl"))
          if File.exists?(sonar_mtbl) && File.size(sonar_mtbl) > 0
            next
          end

          output_base = File.join(norm, File.basename(sonar_file).sub(".json.gz", ""))
          csv_cmd = "nice #{gzip_command} -dc #{Shellwords.shellescape(sonar_file)} | nice inetdata-sonardnsv2-split -t #{get_tempdir} -m #{(get_total_ram/8.0).to_i} #{output_base}"
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
      # Find all sonar datafiles
      #
      def sonar_datafiles
        paths = Dir["#{storage_path}/*.json.gz"].sort { |a,b|
          File.basename(a).split(/[^\d\-]+/).first.gsub("-", '')[0,8].to_i <=>
          File.basename(b).split(/[^\d\-]+/).first.gsub("-", '')[0,8].to_i
        }
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
