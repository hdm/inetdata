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

          fdns = meta['studies'].select{|x| x['uniqid'] == 'sonar.fdns' }
          if fdns && fdns.last && fdns.last['files'] && fdns.last['files'].last && fdns.last['files'].last['name']
            queue << fdns.last['files'].last['name']
          end

          rdns = meta['studies'].select{|x| x['uniqid'] == 'sonar.rdns' }
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

    end
  end
end
