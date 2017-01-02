module InetData
  module Source
    class Censys_Certs < Base

      def manual?
        true
      end

      def available?
        config['censys_api_id'].to_s.length > 0 &&
        config['censys_secret'].to_s.length > 0
      end

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

          log("Downloading #{src} with #{res['Content-Length']} bytes to #{dst}...")
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

      def query_json(src)
        target = URI.parse(src)
        tries  = 0

        begin
          tries += 1
          http   = Net::HTTP.new(target.host, target.port)
          http.use_ssl = true

          req = Net::HTTP::Get.new(target.request_uri)
          req.basic_auth(config['censys_api_id'], config['censys_secret'])

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
            log("Query of #{path} failed: #{$!.class} #{$!}, retrying...")
            sleep(30)
            retry
          else
            fail("Query of #{path} failed: #{$!.class} #{$!} after #{tries} attempts")
          end
        end
      end

      def download
        meta = query_json(config['censys_base_url'] + '/data')
        dir  = storage_path
        FileUtils.mkdir_p(dir)

        mqueue = []
        dqueue = {}
        if dbase = meta['primary_series']
          ['All X.509 Certificates'].each do |dtype|
            if dbase[dtype] &&
               dbase[dtype]['latest_result'] &&
               dbase[dtype]['latest_result']['details_url']
              mqueue << dbase[dtype]['latest_result']['details_url']
            end
          end
        end

        mqueue.each do |mpath|
          info = query_json(mpath)

          if info &&
            info['series'] &&
            info['series']['id'] &&
            info['primary_file'] &&
            info['primary_file']['compressed_download_path'] &&
            info['timestamp']

            fname = ( [ info['series']['id'], info['timestamp'].to_s ].join("-") +
                      ".json." + info['primary_file']['compressed_download_path'].split('.').last
                    ).gsub("/", "_")

            dst = File.join(dir, fname)
            dqueue[info['primary_file']['compressed_download_path']] = dst
          end
        end

        dqueue.each_pair do |src,dst|
          download_file(src, dst)
        end
      end

    end
  end
end
