module InetData
  class Logger
    attr_accessor :config, :lname, :fd, :lock

    def initialize(config, lname)
      self.config = config
      self.lname = lname.gsub(/[^a-z0-9A-F_\-]+/, '')
      self.config[:logger] = self
      self.lock = Mutex.new
    end

    def log(msg)
      self.lock.synchronize do
        entry = "#{Time.now.strftime("%Y-%m-%d %H:%M:%S")} [#{lname}] #{msg}"
        if config['log_stderr']
          $stderr.puts entry
          $stderr.flush
        end

        unless self.fd
          FileUtils.mkdir_p(config['logs'])
          self.fd = File.open(File.join(config['logs'], self.lname + ".txt"), "wb")
        end

        self.fd.puts(entry)
        self.fd.flush
      end
    end

    def dlog(msg)
      return unless config['log_debug']
      log("DEBUG: #{msg}")
    end

  end
end
