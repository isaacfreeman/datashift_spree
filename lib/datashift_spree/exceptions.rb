# Copyright:: (c) Autotelik Media Ltd 2014
# Author ::   Tom Statter
# Date ::     June 2014
# License::   Free, Open Source.
#

module DataShiftSpree

  class ProductLoadError < Exception
    def initialize( msg )
      super( msg )
    end
  end


end
