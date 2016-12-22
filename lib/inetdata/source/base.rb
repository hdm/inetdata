module InetData
  module Source
    class Base

      VALID_HOSTNAME = /^([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])(\.([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\-]{0,61}[a-zA-Z0-9]))*$/
      MATCH_IPV6 = /^\s*((([0-9A-Fa-f]{1,4}:){7}([0-9A-Fa-f]{1,4}|:))|(([0-9A-Fa-f]{1,4}:){6}(:[0-9A-Fa-f]{1,4}|((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3})|:))|(([0-9A-Fa-f]{1,4}:){5}(((:[0-9A-Fa-f]{1,4}){1,2})|:((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3})|:))|(([0-9A-Fa-f]{1,4}:){4}(((:[0-9A-Fa-f]{1,4}){1,3})|((:[0-9A-Fa-f]{1,4})?:((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3}))|:))|(([0-9A-Fa-f]{1,4}:){3}(((:[0-9A-Fa-f]{1,4}){1,4})|((:[0-9A-Fa-f]{1,4}){0,2}:((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3}))|:))|(([0-9A-Fa-f]{1,4}:){2}(((:[0-9A-Fa-f]{1,4}){1,5})|((:[0-9A-Fa-f]{1,4}){0,3}:((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3}))|:))|(([0-9A-Fa-f]{1,4}:){1}(((:[0-9A-Fa-f]{1,4}){1,6})|((:[0-9A-Fa-f]{1,4}){0,4}:((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3}))|:))|(:(((:[0-9A-Fa-f]{1,4}){1,7})|((:[0-9A-Fa-f]{1,4}){0,5}:((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3}))|:)))(%.+)?\s*$/
      MATCH_IPV4 = /^\s*(?:(?:25[0-5]|2[0-4][0-9]|[0-1]?[0-9]{1,2})[.](?:25[0-5]|2[0-4][0-9]|[0-1]?[0-9]{1,2})[.](?:25[0-5]|2[0-4][0-9]|[0-1]?[0-9]{1,2})[.](?:25[0-5]|2[0-4][0-9]|[0-1]?[0-9]{1,2}))\s*$/
      MATCH_IPV4_PRIVATE = /^\s*(?:10\.|192\.168|172.(?:1[6-9]|2[0-9]|3[01])\.|169\.254)/

      attr_accessor :config

      def self.name
        self.to_s.split('::').last.downcase
      end

      def name
        self.class.name
      end

      def initialize(config)
        self.config = config
      end

      def max_tries
        5
      end

      def log(msg)
        config[:logger].log("[#{self.name}] #{msg}")
      end

      def dlog(msg)
        config[:logger].dlog("[#{self.name}] #{msg}")
      end

      def fail(reason)
        log("ERROR: #{reason}")
	      raise RuntimeError, "[#{self.name}] FATAL ERROR: #{reason}"
      end

      def available?
        true
      end

      def storage_path
        File.expand_path(File.join(config['storage'], self.name))
      end

      def reports_path
        File.expand_path(File.join(config['reports'], self.name))
      end

      def download
        raise RuntimeError.new, "Download not implemented for source #{self.name}"
      end

      def normalize
        raise RuntimeError.new, "Normalize not implemented for source #{self.name}"
      end

      def validate_domain(dname)
        return false unless dname =~ VALID_HOSTNAME
        return false unless dname.index(".")
        dname.sub(/\.$/, '')
      end

      def inetdata_parsers_available?
        utils = %W{
            inetdata-arin-org2cidrs inetdata-csv2mtbl inetdata-csvrollup inetdata-csvsplit inetdata-dns2mtbl
            inetdata-hostnames2domains inetdata-json2mtbl inetdata-lines2mtbl inetdata-zone2csv inetdata-arin-xml2json
            mq
        }
        utils.each do |name|
          unless `which #{name}`.length > 0
            dlog("Missing inetdata-parsers command: #{name}")
            return
          end
        end
        true
      end

      def gzip_command
        @gzip_command ||= (`which pigz`.length > 0) ? "pigz" : "gzip"
      end

      def decompress_gzfile(path)
        cmd = [gzip_command, "-dc"]
        cmd.push(path)

        dlog("Decompressing #{path} with #{cmd} for #{self.name}...")
        if block_given?
          IO.popen(cmd, "rb") do |pipe|
            yield(pipe)
          end
        else
          return IO.popen(cmd)
        end
      end

      def expand_domains(hostname)
        return [] if hostname =~ MATCH_IPV4

        bits = hostname.split('.').select{|x| x.length > 0}
        outp = []
        bits.shift

        while bits.length > 1
          outp << bits.join(".")
          bits.shift
        end

        outp
      end

      def uniq_sort_file(path, keep=false)
        pre = "LC_ALL=C nice sort #{get_sort_options} -u "
        dst = path + ".sorted"
        err = path + ".sort.err"
        old = path + ".unsorted"

        cmd = ("#{pre} #{Shellwords.shellescape(path)} >#{Shellwords.shellescape(dst)} 2>#{Shellwords.shellescape(err)}")
        dlog("Unique sorting #{path} for #{self.name}")
        ok  = system(cmd)

        unless ok
          raise RuntimeError.new("Unique sort of #{path} triggered an error, stored in #{err}")
        end

        if keep
          File.rename(path, old)
        end

        if File.exists?(err) && File.size(err) == 0
          File.unlink(err)
        end

        File.rename(dst, path)
        true
      end

      def get_tempdir
        ENV['HOME'] || "/tmp"
      end

      def get_sort_options
        "-S #{get_max_ram_sort} --parallel=#{get_max_cores}"
      end

      def get_max_ram_sort
        config['max_ram'] || '50%'
      end

      def get_total_ram
        @max_total_ram ||= `free -g | grep ^Mem`.split(/\s+/)[1].to_i
      end

      def get_max_cores
        config['max_cores'] || File.read("/proc/cpuinfo").scan(/^processor\s+:/).length
      end

    end
  end
end
