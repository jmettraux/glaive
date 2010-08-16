
require 'fileutils'

task :default => :build

task :build => :clean do

  sh "6g src/*.go && 6l -o glaive *.6 && rm *.6"
  #sh "8g src/*.go && 8l -o glaive *.8 && rm *.8"
end

task :clean do

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

