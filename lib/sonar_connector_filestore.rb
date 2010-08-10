require 'fileutils'

module Sonar
  module Connector

    # a FileStore has an on-disk directory structure :
    #
    # - root, effectively a parent directory
    # - name : the filestore directory name
    # - areas : names of acceptable sub-directories in the FileStore
    class FileStore
      attr_reader :root
      attr_reader :name
      attr_reader :areas
      
      def initialize(root, name, *areas)
        raise "directory '#{root}' does not exist" if !File.directory?(root)
        @root = root

        @name = name
        FileUtils.mkdir_p(filestore_path)

        @areas = Set.new(areas)
        @areas.each{|area| FileUtils.mkdir_p(area_path(area))}
      end

      def destroy!
        FileUtils.rm_r(filestore_path)
      end

      def filestore_path
        File.join(root, name.to_s)
      end

      def check_area(area)
        raise "no such area: #{area}" if !@areas.include?(area)
      end
      
      def area_path(area)
        check_area(area)
        File.join(filestore_path, area.to_s)
      end

      def file_path(area, filename)
        File.join(area_path(area), filename)
      end

      # process files from source_area. move it to error_area if the block
      # raises an exception and to success_area if the block completes
      def process(source_area, error_area=nil, success_area=nil)
        raise "i need a block" if !block_given?
        
        ap = area_path(source_area)
        for_each(source_area) do |f|
          begin
            yield File.join(ap, f)
            if success_area
              move(source_area, f, success_area)
            else
              delete(source_area, f)
            end
          rescue Exception=>e
            $stderr << [e.message, *e.backtrace].join("\n") << "\n"
            if error_area
              move(source_area, f, error_area)
            else
              delete(source_area, f)
            end
          end
        end
      end

      # number of items in the area
      def count(area)
        ap = area_path(area)
        Dir[File.join(ap, "*")].length
      end

      def area_count
        @areas.reduce({}){|h,area| h[area]=count(area) ; h}
      end

      # disk usage of an area in kb
      def size(area)
        ap = area_path(area)
        `du -k #{ap}`.gsub(/\W+tmp\W*$/m,'').to_i
      end
      
      def area_size
        @areas.reduce({}){|h,area| h[area]=size(area) ; h}
      end

      # iterate over all files in an area, calling a block on each
      def for_each(area)
        ap = area_path(area)
        Dir.foreach(area_path(area)) do |f|
          yield f if f !~ /^\./
        end
      end

      # write a file to an area
      def write(area, filename, content)
        File.open(file_path(area, filename), "w"){ |io| io << content }
      end

      def read(area, filename)
        File.read(file_path(area, filename))
      end

      # move a path into an area of the filestore
      def receive(path, to_area)
        check_area(to_area)
        ap = area_path(to_area)
        FileUtils.mv(path, to_area)
      end

      # remove a file from an area
      def delete(area, filename)
        FileUtils.rm_r(file_path(area, filename))
      end

      # move a file from one area to another
      def move(from_area, filename, to_area)
        f1 = file_path(from_area, filename)
        f2 = file_path(to_area, filename)
        FileUtils.mv(f1, f2)
      end

      # flip files from an area into a sub-directory of an area 
      # in another
      # filestore, named by the name of this filestore
      # thus 
      # fs1.flip(:complete, fs2, :working ) moves
      # fs1/complete => fs2/working/fs1
      def flip(area, filestore, to_area)
        ap = area_path(area)
        for_each(area) do |f|
          filestore.receive_flip(name, File.join(ap,f), to_area)
        end
      end

      # receive a flip
      def receive_flip(from_filestore_name, path, to_area)
        ap = area_path(to_area)
        to_path = File.join(ap, from_filestore_name.to_s)
        FileUtils.mkdir_p(to_path)
        FileUtils.mv(path, to_path)
      end
    end
  end
end
