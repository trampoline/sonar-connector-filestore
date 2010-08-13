require 'fileutils'
require 'uuidtools'
require 'logger'
require 'set'

module Sonar
  module Connector

    # a FileStore has an on-disk directory structure :
    #
    # - root, effectively a parent directory
    # - name : the filestore directory name
    # - areas : names of acceptable sub-directories in the FileStore directory
    # so a filestore with (@root=="/foo", @name==:bar, @areas=[:area51, :area52])
    # would have directories :
    #
    #  /foo
    #  /foo/bar
    #  /foo/bar/area51
    #  /foo/bar/area52
    class FileStore
      class << self
        # the default logger...
        attr_accessor :logger
      end
      FileStore.logger = Logger.new($stdout)
      FileStore.logger.level = Logger::INFO

      attr_reader :root
      attr_reader :name
      attr_reader :areas
      attr_writer :logger

      def self.valid_filestore_name(name)
        ordinary_directory_name(name)
      end

      def self.ordinary_directory_name(name)
        name !~ /^\./
      end

      def initialize(root, name, areas, opts={})
        raise "directory '#{root}' does not exist or is not a directory" if !File.directory?(root)
        @root = root

        raise "#{name} is not a valid filestore name" if !FileStore.valid_filestore_name(name)
        @name = name
        FileUtils.mkdir_p(filestore_path)

        @areas = Set.new([*areas])
        raise ":tmp is not a valid area name" if @areas.include?(:tmp)
        @areas.each{|area| FileUtils.mkdir_p(area_path(area))}

        @logger = opts[:logger]
      end

      def logger
        @logger || FileStore.logger
      end

      def destroy!
        FileUtils.rm_r(filestore_path)
      end

      def filestore_path
        File.join(root, name.to_s)
      end

      def check_area(area)
        raise "no such area: #{area}" if !@areas.include?(area) && area!=:tmp
      end
      
      def area_path(area)
        check_area(area)
        File.join(filestore_path, area.to_s)
      end

      def file_path(area, filename)
        File.join(area_path(area), filename)
      end

      # marker exception to tell process and process_batch to 
      # leave files in the source area
      class LeaveInSourceArea < RuntimeError
      end

      # process files from source_area. move it to error_area if the block
      # raises an exception and to success_area if the block completes. if
      # LeaveInSourceArea is raised, don't do anything with the files
      def process(source_area, error_area=nil, success_area=nil)
        raise "i need a block" if !block_given?
        
        files = area_files(source_area)
        files.each do |f|
          begin
            yield f
            if success_area
              move(source_area, f, success_area)
            else
              delete(source_area, f)
            end
          rescue LeaveInSourceArea=>e
            logger.info("leaving files in #{source_area}")
            raise
          rescue Exception=>e
            logger.warn(FileStore.to_s){[e.class.to_s, e.message, *e.backtrace].join("\n")}
            if error_area
              move(source_area, f, error_area)
            else
              delete(source_area, f)
            end
            raise
          end
        end
      end

      # process a batch of files from source_area. move them to error_area if
      # the block raises and exception, and to success_area if the block completes,
      # and leave where they are if LeaveInSourceArea is raised.
      # returns the number of items processed, 0 if all work is done.
      def process_batch(batch_size, source_area, error_area=nil, success_area=nil)
        raise "i need a block" if !block_given?

        batch = area_files(source_area, batch_size)
        return 0 if batch.size==0
        begin
          yield batch
          if success_area
            batch.each{|p| move(source_area, p, success_area)}
          else
            batch.each{|p| delete(source_area, p)}
          end
        rescue LeaveInSourceArea=>e
          logger.info("leaving files in #{source_area}")
          raise
        rescue Exception=>e
          logger.warn(FileStore.to_s){[e.class.to_s, e.message, *e.backtrace].join("\n")}
          if error_area
            batch.each{|p| move(source_area, p, error_area)}
          else
            batch.each{|p| delete(source_area, p)}
          end
          raise
        end
        return batch.size
      end

      # fetch at most max regular file paths from an area
      def area_files(area, max=nil)
        relative_file_paths(area_path(area), max)
      end

      # number of items in an area
      def count(area)
        ap = area_path(area)
        Dir[File.join(ap, "*")].length
      end

      # hash of counts keyed by area
      def area_count
        @areas.reduce({}){|h,area| h[area]=count(area) ; h}
      end

      # disk usage of an area in kb
      def size(area)
        ap = area_path(area)
        `du -k #{ap}`.gsub(/\W+tmp\W*$/m,'').to_i
      end

      # hash of sizes keyed by area
      def area_size
        @areas.reduce({}){|h,area| h[area]=size(area) ; h}
      end

      # iterate over all files in top level of an area, calling a block on each
      def for_each(area)
        ap = area_path(area)
        Dir.foreach(area_path(area)) do |f|
          yield f if File.file?(f) || FileStore.ordinary_directory_name(f)
        end
      end

      # write a file to an area
      def write(area, filename, content)
        ensure_area_directory(area, filename)
        File.open(file_path(area, filename), "w"){ |io| io << content }
      end

      # read a file from an area
      def read(area, filename)
        File.read(file_path(area, filename))
      end

      # remove a file from an area
      def delete(area, filename)
        FileUtils.rm_r(file_path(area, filename))
      end

      # move a file from one area to another
      def move(from_area, filename, to_area)
        move_file(area_path(from_area), filename, area_path(to_area))
      end

      # remove any empty directories from an area
      def scrub!(area)
        scrub_path(area_path(area), false)
      end

      # flip files from an area into a sub-directory of an area 
      # in another
      # filestore, named by the name of this filestore
      # thus 
      # fs1.flip(:complete, fs2, :working ) moves
      # fs1/complete/* => fs2/working/fs1/*
      def flip(area, filestore, to_area)
        ap = area_path(area)
        paths = []

        scrub!(area) # only move what we need to

        # collect all moveable paths
        for_each(area) do |f|
          paths << File.join(ap, f)
        end
        filestore.receive_flip(name, to_area, paths) if paths.length>0
      end

      # receive a flip... move all paths to be flipped 
      # into a temporary directory, and then move that
      # directory into place in one atomic move operation
      def receive_flip(from_filestore_name, to_area, paths)
        ap = area_path(to_area)
        to_path = File.join(ap, from_filestore_name.to_s)

        # a path for receiving all flips for an area
        tmp_area_path = File.join(area_path(:tmp), to_area.to_s)

        # first move all moveable paths to a unique named tmp area within the receive area
        tmp_path = File.join(tmp_area_path, UUIDTools::UUID.timestamp_create)
        if paths.length>0
          FileUtils.mkdir_p(tmp_path)
          paths.each do |path|
            FileUtils.mv(path, tmp_path)
          end
        end

        # move everything from the receive area... recovers interrupted receive_flips too
        Dir.foreach(tmp_area_path) do |path|
          mv_path = File.join(tmp_area_path, path)
          FileUtils.mv(mv_path, to_path) if File.file?(mv_path) || (File.directory?(mv_path) && FileStore.ordinary_directory_name(path))
        end
      end

      private

      # depth first search
      def scrub_path(dir, scrub)
        empty = scrub
        Dir.foreach(dir) do |f|
          path = File.join(dir, f)
          if File.directory?(path) 
            # want to descend : so avoid short-cut evaluation
            empty = scrub_path(path, true) && empty if FileStore.ordinary_directory_name(f)
          else
            empty = false
          end
        end
        FileUtils.rm_rf(dir) if empty
      end

      # fetch at most max relative regular file paths from a directory hierarchy
      # rooted at dir
      def relative_file_paths(dir, max=nil)
        file_paths(dir, max).map{|p| p.gsub(/^#{dir}#{File::SEPARATOR}/,'')}
      end

      # fetch at most max regular file paths from a directory hierarchy
      # rooted at dir
      def file_paths(dir, max=nil)
        paths = []
        Dir.foreach(dir) do |f|
          return paths if max && paths.size >= max
          path = File.join(dir, f)
          if File.directory?(path)
            paths += file_paths(path, max) if FileStore.ordinary_directory_name(f)
          elsif File.file?(path)
            paths << path
          end
        end
        paths
      end

      # move a file named relative to filename dir
      # to the same filename relative to to_dir
      def move_file(from_dir, filename, to_dir)
        f1 = File.join(from_dir, filename)
        f2 = File.join(to_dir, filename)
        ensure_directory(to_dir, filename)
        FileUtils.mv(f1, f2)
      end

      # ensure that the directory of a filename exists in the given area
      def ensure_area_directory(area, filename)
        # create a directory in the destination area if necessary
        ensure_directory(area_path(area), filename)
      end

      # given a directory, and a filename relative to it, ensure
      # that the directory containing the actual file exists
      # e.g. given dir==/a/b/c and filename==d/e/f.txt
      # then ensure directory /a/b/c/d/e exists
      def ensure_directory(dir, filename)
        file_dir = File.expand_path(File.join(dir, File.dirname(filename)))
        FileUtils.mkdir_p(file_dir)
      end
    end
  end
end
