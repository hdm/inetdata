module InetData
  class Config < Hash

    def initialize(path=nil)
      root = File.expand_path(File.join(File.dirname(__FILE__), "..", ".."))
      unless path
        path = File.join(root, "conf", "inetdata.json")
      end

      unless File.exists?(path) && File.readable?(path)
        raise RuntimeError, "Missing configuration file: #{path}"
      end

      self.merge!(JSON.parse(File.read(path)))
      self['root'] = root

      %W{ storage logs reports }.each do |k|
        unless self[k].to_s.length > 0
          raise RuntimeError, "Missing configuration path for #{k}"
        end
        self[k] = File.expand_path(self[k].gsub(/^\.\//, self['root'] + '/'))
      end
    end

    def self.raise_rlimit_nofiles(nofiles)
      Process.setrlimit(Process::RLIMIT_NOFILE, nofiles)
      Process.getrlimit(Process::RLIMIT_NOFILE).first
    end

  end
end
