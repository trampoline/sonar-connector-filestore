require File.expand_path(File.dirname(__FILE__) + '/spec_helper')
require 'set'
require 'fileutils'

module Sonar
  module Connector
    describe "SonarConnectorFilestore" do

      before(:all) do
        FileStore::logger.level = Logger::FATAL
      end

      before(:each) do
        FileUtils.rm_rf(TMP_DIR)
        FileUtils.mkdir_p(TMP_DIR)
      end

      after(:each) do
      end

      def create_testfs(*areas)
        areas = [:foo, :bar] if areas.empty?
        FileStore.new(TMP_DIR, :testfs, areas)
      end

      it "should initialize with a root, a name and areas, and create directories" do
        fs=create_testfs
        fs.root.should == TMP_DIR
        fs.name.should == :testfs
        fs.areas.should == [:foo, :bar].to_set
        File.directory?(File.join(TMP_DIR, "testfs")).should == true
        File.directory?(File.join(TMP_DIR, "testfs", "foo")).should == true
        File.directory?(File.join(TMP_DIR, "testfs", "bar")).should == true
      end

      it "should destroy itself cleanly" do
        fs=create_testfs
        fs.destroy!
        File.exist?(File.join(TMP_DIR, "testfs")).should == false
      end

      it "should write files to an area" do
        fs = create_testfs
        fs.write(:foo, "testfile.txt", "one two three")
        File.read(File.join(TMP_DIR, "testfs", "foo", "testfile.txt")).should == "one two three"
      end
      
      it "should count files in an area" do
        fs = create_testfs
        fs.write(:foo, "testfile.txt", "one two three")
        fs.write(:foo, "testfile2.txt", "one two three")
        fs.count(:foo).should == 2
      end

      it "should count files across all areas" do
        fs = create_testfs
        fs.write(:foo, "testfile.txt", "one two three")
        fs.write(:bar, "testfile2.txt", "one two three")
        fs.area_count.should == {:foo=>1, :bar=>1}
      end
      
      it "should give a kb based disk usage for an area" do
        # don't actually know what disk page size is
        # so test that each file occupies >0 space, and that two
        # files the same size occupy twice as much as one
        fs = create_testfs
        fs.write(:foo, "testfile.txt", "one two three")
        sz1 = fs.size(:foo)
        sz1.should > 0
        fs.write(:foo, "testfile2.txt", "one two three")
        fs.size(:foo).should == 2*sz1
      end

      it "should give kb based disk usage across all areas" do
        fs = create_testfs
        fs.write(:foo, "testfile.txt", "one two three")
        sz1 = fs.size(:foo)
        sz1.should > 0
        fs.write(:bar, "testfile.txt", "one two three")
        fs.write(:bar, "testfile2.txt", "one two three")
        fs.area_size.should == {:foo=>sz1, :bar=>2*sz1}
      end

      it "should scrub empty directories from an area" do
        fs = create_testfs
        ap = fs.area_path(:foo)
        FileUtils.mkdir_p(File.join(ap, "bar", "baz"))
        FileUtils.mkdir_p(File.join(ap, "woo"))
        FileUtils.mkdir_p(File.join(ap, "waz"))
        fs.write(:foo, File.join("waz", "testfile.txt"), "one two three")

        fs.scrub!(:foo)

        File.exist?(File.join(ap, "bar")).should == false
        File.exist?(File.join(ap, "woo")).should == false
        File.exist?(File.join(ap, "waz", "testfile.txt")).should == true
      end


      it "should iterate over all files in an area" do
        fs = create_testfs
        fs.write(:foo, "testfile.txt", "one two three")
        fs.write(:foo, "testfile2.txt", "four five six")

        texts = Set.new
        ap = fs.area_path(:foo)
        fs.for_each(:foo) do |f|
          texts << File.read(File.join(ap, f))
        end

        texts.should == ["one two three", "four five six"].to_set
      end

      it "should ignore . and .. files when iterating" do
        fs = create_testfs(:foo, :bar, :baz)

        stub(Dir).foreach do |path, proc| 
          [".", "..", "foo", "bar"].each{ |p| proc.call(p)}
        end
        
        files = Set.new
        fs.for_each(:foo){|f| files << f}
        files.should == ["foo", "bar"].to_set
        
      end

      describe "process" do
        before do
          @fs = create_testfs(:foo, :bar, :baz)
          @fs.write(:foo, "testfile.txt", "one two three")
          @fs.write(:foo, "testfile2.txt", "four five six")
          @fs.write(:foo, "testfile3.txt", "seven eight nine")
        end

        it "should process all files in an area" do
          texts = Set.new
          @fs.process(:foo) do |f|
            texts << File.read(@fs.file_path(:foo, f))
          end
          texts.should == ["one two three", "four five six", "seven eight nine"].to_set
          @fs.count(:foo).should == 0
        end

        it "should move failed processings to the error_area" do
          texts = Set.new
          lambda {
            @fs.process(:foo, :bar) do |f|
              s = File.read(@fs.file_path(:foo, f))
              raise "five" if s =~ /five/
              texts << s
            end
          }.should raise_error("five")
          @fs.count(:foo).should == 1
          @fs.count(:bar).should == 1
          @fs.read(:bar, "testfile2.txt").should == "four five six"
        end

        it "should move completed processings to the success_area" do
          texts = Set.new
          lambda {
            @fs.process(:foo, :bar, :baz) do |f|
              s = File.read(@fs.file_path(:foo, f))
              raise "five" if s =~ /five/
              texts << s
            end
          }.should raise_error("five")
          @fs.count(:foo).should == 1
          @fs.count(:bar).should == 1
          @fs.count(:baz).should == 1
          @fs.read(:bar, "testfile2.txt").should == "four five six"
        end
      end

      describe "process_batch" do
        before do
          @fs = create_testfs(:foo, :bar, :baz)
          FileUtils.mkdir_p(File.join(@fs.area_path(:foo), "a", "b"))
          FileUtils.mkdir_p(File.join(@fs.area_path(:foo), "c", "d"))
          @fs.write(:foo, "a/b/testfile.txt", "one two three")
          @fs.write(:foo, "a/testfile2.txt", "four five six")
          @fs.write(:foo, "c/d/testfile3.txt", "seven eight nine")
          @fs.write(:foo, "c/testfile3.txt", "ten eleven twelve")

          @files = ["a/b/testfile.txt", "a/testfile2.txt", "c/d/testfile3.txt", "c/testfile3.txt"].to_set
          @processed = Set.new
        end

        def process_batch(fs, size, source_area, error_area=nil, success_area=nil)
          fs.process_batch(size, source_area, error_area, success_area) do |batch|
            @processed += batch
            batch.each do |f| 
              raise "#{f} not in @files" if !@files.delete?(f) 
            end
            if block_given?
              batch.each do |f|
                yield f
              end
            end
          end
        end

        def check_files(fs, area, paths, test)
          paths.each{|f| File.exist?(fs.file_path(area, f)).should == test}
        end

        it "should process and delete a limited batch of files" do
          process_batch(@fs, 2, :foo).should == 2
          
          @processed.size.should == 2
          @files.size.should == 2
          check_files(@fs, :foo, @processed, false)
          check_files(@fs, :foo, @files, true)

          process_batch(@fs, 3, :foo).should == 2

          @processed.size.should == 4
          @files.size.should == 0
          check_files(@fs, :foo, @processed, false)

          process_batch(@fs, 2, :foo).should == 0
        end

        it "should move failed batches to error_area if given" do
          lambda {
            process_batch(@fs, 2, :foo, :bar){|f| raise "foo"}
          }.should raise_error("foo")
          
          @files.size.should == 2
          check_files(@fs, :foo, @files, true)
          check_files(@fs, :bar, @processed, true)

          @ok = Set.new
          process_batch(@fs, 2, :foo, :bar){|f| @ok << f}.should == 2
          @files.size.should == 0
          check_files(@fs, :foo, @ok, false)
          check_files(@fs, :bar, @ok, false)

          process_batch(@fs, 2, :foo, :bar).should == 0
        end

        it "should move successful batches to success_area if given" do
          process_batch(@fs, 2, :foo, :bar, :baz).should == 2
          @files.size.should == 2
          check_files(@fs, :foo, @files, true)
          check_files(@fs, :baz, @processed, true)

          process_batch(@fs, 2, :foo, :bar, :baz).should == 2
          check_files(@fs, :baz, @processed, true)
          
          process_batch(@fs, 2, :foo, :bar, :baz).should == 0
        end

        it "should leave files in source area if LeaveInSourceArea is thrown" do
          lambda {
            process_batch(@fs, 2, :foo, :bar){|f| raise FileStore::LeaveInSourceArea }
          }.should raise_error(FileStore::LeaveInSourceArea)
          check_files(@fs, :foo, @files, true)
          check_files(@fs, :baz, @processed, false)
          check_files(@fs, :foo, @processed, true)
        end

      end

      describe "flip" do
        before do
          @testfs = create_testfs(:foo, :bar, :baz)
          @testfs.write(:foo, "testfile.txt", "one two three")
          
          @targetfs = FileStore.new(TMP_DIR, :targetfs, [:a, :b])
        end


        it "should flip from testfs to targetfs" do
          @testfs.flip(:foo, @targetfs, :a)

          File.exists?(File.join(@targetfs.area_path(:a), @testfs.name.to_s, "testfile.txt")).should == true

          # should recreate are in flipped source, so source is
          # still valid
          File.exists?(File.join(@testfs.area_path(:foo)))
        end

      end

      describe "area_files" do
        before do
          @fs = create_testfs(:foo)
          FileUtils.mkdir_p(File.join(@fs.area_path(:foo), "a", "b"))
          FileUtils.mkdir_p(File.join(@fs.area_path(:foo), "c", "d"))
          @fs.write(:foo, "a/b/testfile.txt", "one two three")
          @fs.write(:foo, "a/testfile2.txt", "four five six")
          @fs.write(:foo, "c/d/testfile3.txt", "seven eight nine")
          @fs.write(:foo, "c/testfile3.txt", "ten eleven twelve")

          @files = ["a/b/testfile.txt", "a/testfile2.txt", "c/d/testfile3.txt", "c/testfile3.txt"].to_set
        end

        it "should fetch all paths if max not given" do 
          @fs.area_files(:foo).to_set.should == @files
        end

        it "should fetch a limited number of paths if max given" do
          fs = @fs.area_files(:foo, 2).to_set
          fs.size.should == 2
          (@files - fs).size.should == 2
        end
      end

      describe "logger" do
        it "should write to the class logger if no instance logger given" do
          fs = create_testfs(:foo)
          mock(FileStore.logger).debug("foo")
          fs.logger.debug("foo")
        end

        it "should write to the instance logger if given" do
          fs = create_testfs(:foo)
          mock(logger = Object.new).debug("foo")
          fs.logger = logger
          fs.logger.debug("foo")
        end
      end

    end
  end
end
