
require 'fileutils'

task :default => :build

task :build => :clean do

  sh "6g *.go && 6l -o glaive *.6"
  #sh "8g *.go && 8l -o glaive *.8"
end

task :clean do

  Dir['./**/*.6'].each { |path| FileUtils.rm(path) }
  Dir['./**/*.8'].each { |path| FileUtils.rm(path) }
  FileUtils.rm_f('glaive')
end

task :format do

  Dir['./**/*.go'].each do |path|
    sh "gofmt #{path} > #{path}.fmt"
    sh "mv #{path}.fmt #{path}"
  end
end

task :test do

  sh "gotest" rescue nil
end

task :serve => :build do
  sh "./glaive"
end

