<?php

class PHPState
{
	const RECORDSIZE=8;
    private $dir = "/var/lib/phptimeseries/";
    private $log;
    
    private $writebuffer = array();

    /**
     * Constructor
     *
     * @api
    */
    public function __construct($settings)
    {
        if (isset($settings['datadir'])) $this->dir = $settings['datadir'];
        $this->log = new EmonLogger(__FILE__);
    }

// #### \/ Below are required methods
    /**
     * Create feed
     *
     * @param integer $feedid The id of the feed to be created
     * @param array $options for the engine
    */
    
    public function create($feedid,$options)
    {
    	$this->log->info("create() feedid=$feedid");
    	 
    	$fh = @fopen($this->dir."feed_$feedid.state", 'a');
        if (!$fh) {
            $msg = "could not write data file " . error_get_last()['message'];
            $this->log->error("create() ".$msg);
            return $msg;
        }

        if (!flock($fh, LOCK_EX)) {
            $msg = "data file '".$this->dir.$feedname."' is locked by another process";
            $this->log->error("create() ".$msg);
            fclose($fh);
            return $msg;
        }
        fclose($fh);
        if (file_exists($this->dir."feed_$feedid.state")) return true;
        return false;
    }

    /**
     * Delete feed
     *
     * @param integer $feedid The id of the feed to be created
    */
    public function delete($feedid)
    {
        unlink($this->dir."feed_$feedid.state");
    }

    /**
     * Gets engine metadata
     *
     * @param integer $feedid The id of the feed to be created
    */
    public function get_meta($feedid)
    {
        $feedid = (int) $feedid;
        $feedname = "feed_$feedid.state";

        if (!file_exists($this->dir.$feedname)) {
            $this->log->warn("get_meta() feed file does not exist '".$this->dir.$feedname."'");
            return false;
        }

		// not much meta information to create            
        $meta = new stdClass();
        $meta->id=$feedid;
        return $meta;
    }

    /**
     * Returns engine occupied size in bytes
     *
     * @param integer $feedid The id of the feed to be created
    */
    public function get_feed_size($feedid)
    {
        return filesize($this->dir."feed_$feedid.state");
    }


    
    // POST OR UPDATE
    //
    // - fix if filesize is incorrect (not a multiple of PHPState::RECORDSIZE)
    // - append if file is empty
    // - append if datapoint is in the future
    // - update if datapoint is older than last datapoint value
    /**
     * Adds a data point to the feed
     *
     * @param integer $feedid The id of the feed to add to
     * @param integer $time The unix timestamp of the data point, in seconds
     * @param float $value The value of the data point
     * @param array $arg optional padding mode argument
    */
    public function post($feedid,$time,$value,$arg=null)
    {
        $meta=$this->get_meta($feedid);
        $this->log->info("post() feedid=$feedid time=$time value=$value");
        
        // Get last value
        $fh = @fopen($this->dir."feed_$feedid.state", 'rb');
        if (!$fh) {
            $this->log->warn("post() could not open data file feedid=$feedid");
            return false;
        }
        
        $filesize = filesize($this->dir."feed_$feedid.state");
        
        $csize = floor($filesize / PHPState::RECORDSIZE)*PHPState::RECORDSIZE;
        if ($csize!=$filesize) { 	// always write if file is not multiple of PHPState::RECORDSIZE bytes
        
            $this->log->warn("post() filesize not integer multiple of ". PHPState::RECORDSIZE . " bytes, correcting feedid=$feedid");
            // correct corrupt data
            fclose($fh);

            // extend file by required number of bytes
            if (!$fh = $this->fopendata($this->dir."feed_$feedid.state", 'wb')) return false;
            fseek($fh,$csize);
            fwrite($fh, pack("If",$time,$value));
            fclose($fh);
            return $value;
        }
        fclose($fh);
        
   		$last = $this->lastvalue($feedid);
   		// state variable - only write if different from last and time is higher
		// normalize the value because of rounding errors
		$value=unpack("fvalue",pack("f", $value))['value'];
        if ((!isset($last['time']) || ($time > $last['time'])  && $value != $last['value'])) {				// append new value
        	if (!$fh = $this->fopendata($this->dir."feed_$feedid.state", 'a')) return false;
        	fwrite($fh, pack("If",$time,$value));
        	fclose($fh);
   		} else if ($time <= $last['time']) {	// time is in range of current data point
   			$pos=$this->binarysearch_exact($fh, $time, $filesize);
   			if ($pos>=0) {						// exact match is found
   				fclose($fh);
   				if (!$fh = $this->fopendata($this->dir."feed_$feedid.state", 'c+')) return false;
   				fseek($fh, $pos);
   				fwrite($fh, pack("If",$time,$value));
   				fclose($fh);
   			}
   		}
        return $value;
    }

    /**
     * Updates a data point in the feed
     *
     * @param integer $feedid The id of the feed to add to
     * @param integer $time The unix timestamp of the data point, in seconds
     * @param float $value The value of the data point
    */
    public function update($feedid,$time,$value)
    {
      return $this->post($feedid,$time,$value);
    }

    /**
     * Get array with last time and value from a feed
     *
     * @param integer $feedid The id of the feed
    */
    public function lastvalue($feedid)
    {
        $feedid = (int)$feedid;
        $this->log->info("lastvalue() $feedid");
        
        if (!file_exists($this->dir."feed_$feedid.state"))  return false;
        
        $array = false;
        $fh = fopen($this->dir."feed_$feedid.state", 'rb');
        $filesize = filesize($this->dir."feed_$feedid.state");
        if ($filesize>=PHPState::RECORDSIZE)
        {
            fseek($fh,$filesize-PHPState::RECORDSIZE);
            $array = unpack("Itime/fvalue",fread($fh,PHPState::RECORDSIZE));
        }
        fclose($fh);
        return $array;
    }
    
    /**
     * Return the data for the given timerange
     *
     * @param integer $feedid The id of the feed to fetch from
     * @param integer $start The unix timestamp in ms of the start of the data range
     * @param integer $end The unix timestamp in ms of the end of the data range
     * @param integer $interval The number os seconds for each data point to return (used by some engines)
     * @param integer $skipmissing Skip null values from returned data (used by some engines)
     * @param integer $limitinterval Limit datapoints returned to this value (used by some engines)
     * @param integer mode: 0 = cumulative data, 1=difference, 2=difference / interval (derivative)
    */

    public function get_data($feedid,$start,$end,$interval,$skipmissing,$limitinterval, $mode=0)
    {
        $meta=$this->get_meta($feedid);
    	$start = intval($start/1000);
        $end = intval($end/1000);
        $interval= (int) $interval;
        if ($interval == 0) {		// return real points
	        $fh = fopen($this->dir."feed_$feedid.state", 'rb');
	        $filesize = filesize($this->dir."feed_$feedid.state");
	        $data = array();
	        $time = 0; $i = 0;
	        $atime = 0;
	        $pos = $this->binarysearch($fh,$start,$filesize);
	        $this->log->info("get_data($feedid, $start, $end, $interval, $skipmissing, $limitinterval) pos=$pos filesize=$filesize");
	        fseek($fh, $pos);
	        while ($pos >=0 && $pos <$filesize) {
	            $d = fread($fh,PHPState::RECORDSIZE);
	            $array = @unpack("Itime/fvalue",$d);
	            if ($array['time']<=$end && !is_nan($array['value'])) {
	            	$data[]=array($array['time']*1000,$array['value']);
	            }
	            $pos=$pos+PHPState::RECORDSIZE;
	        }
        } else {
     		// Minimum interval
        		if ($interval<1) $interval = 1;
        		// Maximum request size
        		$req_dp = round(($end-$start) / $interval);
        		if ($req_dp>8928) return array("success"=>false, "message"=>"request datapoint limit reached (8928), increase request interval or time range, requested datapoints = $req_dp");
        		
        		$fh = fopen($this->dir."feed_$feedid.state", 'rb');
        		$filesize = filesize($this->dir."feed_$feedid.state");
        		
        		$data = array();
        		$time = 0; $i = 0;
        		$atime = 0;
        		
        		while ($time<=$end)
        		{
        			$time = $start + ($interval * $i);
        			$pos = $this->binarysearch($fh,$time,$filesize);
        			if ($pos>=0 && $pos < $filesize) {
        				fseek($fh,$pos);
	        			$d = fread($fh,PHPState::RECORDSIZE);
	        			$array = @unpack("Itime/fvalue",$d);
	        			$atime = $array['time'];
	        			$value = $array['value'];
       					if ($value!==null || $skipmissing===0) $data[] = array($atime*1000,$value);
        			}
        			$i++;
        		}
        }
        return $data;
    }
    
    public function get_data_DMY($id,$start,$end,$mode,$timezone) 
    {
        $start = intval($start/1000);
        $end = intval($end/1000);
        
        $data = array();
        
        $date = new DateTime();
        if ($timezone===0) $timezone = "UTC";
        $date->setTimezone(new DateTimeZone($timezone));
        $date->setTimestamp($start);
        $date->modify("midnight");
        $date->modify("+1 day");

        $fh = fopen($this->dir."feed_$id.state", 'rb');
        $filesize = filesize($this->dir."feed_$id.state");

        $n = 0;
        $array = array("time"=>0, "value"=>0);
        while($n<10000) // max itterations
        {
            $time = $date->getTimestamp();
            if ($time>$end) break;
            
            $pos = $this->binarysearch($fh,$time,$filesize);
            fseek($fh,$pos);
            $d = fread($fh,PHPState::RECORDSIZE);
            
            $lastarray = $array;
            $array = unpack("x/Itime/fvalue",$d);
            
            if ($array['time']!=$lastarray['time']) {
                $data[] = array($array['time']*1000,$array['value']);
            }
            $date->modify("+1 day");
            $n++;
        }
        
        fclose($fh);
        
        return $data;
    }


    public function export($feedid,$start)
    {
        $feedid = (int) $feedid;
        $start = (int) $start;

        $feedname = "feed_$feedid.state";

        // There is no need for the browser to cache the output
        header("Cache-Control: no-cache, no-store, must-revalidate");

        // Tell the browser to handle output as a csv file to be downloaded
        header('Content-Description: File Transfer');
        header("Content-type: application/octet-stream");
        header("Content-Disposition: attachment; filename={$feedname}");

        header("Expires: 0");
        header("Pragma: no-cache");

        // Write to output stream
        $fh = @fopen( 'php://output', 'w' );

        $primaryfeedname = $this->dir.$feedname;
        $primary = fopen($primaryfeedname, 'rb');
        $primarysize = filesize($primaryfeedname);

        //$localsize = intval((($start - $meta['start']) / $meta['interval']) * 4);

        $localsize = $start;
        $localsize = floor($localsize/PHPState::RECORDSIZE)*PHPState::RECORDSIZE;
        if ($localsize<0) $localsize = 0;

        fseek($primary,$localsize);
        $left_to_read = $primarysize - $localsize;
        if ($left_to_read>0){
            do
            {
                if ($left_to_read>8192) $readsize = 8192; else $readsize = $left_to_read;
                $left_to_read -= $readsize;

                $data = fread($primary,$readsize);
                fwrite($fh,$data);
            }
            while ($left_to_read>0);
        }
        fclose($primary);
        fclose($fh);
        exit;
    }

    public function csv_export($feedid,$start,$end,$outinterval,$usertimezone)
    {
        global $csv_decimal_places, $csv_decimal_place_separator, $csv_field_separator;

        require_once "Modules/feed/engine/shared_helper.php";
        $helperclass = new SharedHelper();

        $feedid = (int) $feedid;
        $start = (int) $start;
        $end = (int) $end;
        $outinterval = (int) $outinterval;
        
        if ($outinterval<1) $outinterval = 1;
        $dp = ceil(($end - $start) / $outinterval);
        $end = $start + ($dp * $outinterval);
        if ($dp<1) return false;

        $fh = fopen($this->dir."feed_$feedid.state", 'rb');
        $filesize = filesize($this->dir."feed_$feedid.state");

        $pos = $this->binarysearch($fh,$start,$filesize);

        $interval = ($end - $start) / $dp;

        // Ensure that interval request is less than 1
        // adjust number of datapoints to request if $interval = 1;
        if ($interval<1) {
            $interval = 1;
            $dp = ($end - $start) / $interval;
        }

        $data = array();

        $time = 0;
        
        // There is no need for the browser to cache the output
        header("Cache-Control: no-cache, no-store, must-revalidate");

        // Tell the browser to handle output as a csv file to be downloaded
        header('Content-Description: File Transfer');
        header("Content-type: application/octet-stream");
        $filename = $feedid.".csv";
        header("Content-Disposition: attachment; filename={$filename}");

        header("Expires: 0");
        header("Pragma: no-cache");

        // Write to output stream
        $exportfh = @fopen( 'php://output', 'w' );

        for ($i=0; $i<$dp; $i++)
        {
            $pos = $this->binarysearch($fh,$start+($i*$interval),$filesize);

            fseek($fh,$pos);

            // Read the datapoint at this position
            $d = fread($fh,PHPState::RECORDSIZE);

            // Itime = unsigned integer (I) assign to 'time'
            // fvalue = float (f) assign to 'value'
            $array = unpack("x/Itime/fvalue",$d);

            $last_time = $time;
            $time = $array['time'];
            $timenew = $helperclass->getTimeZoneFormated($time,$usertimezone);
            // $last_time = 0 only occur in the first run
            if (($time!=$last_time && $time>$last_time) || $last_time==0) {
                fwrite($exportfh, $timenew.$csv_field_separator.number_format($array['value'],$csv_decimal_places,$csv_decimal_place_separator,'')."\n");
            }
        }
        fclose($exportfh);
        exit;
    }

// #### /\ Above are required methods


// #### \/ Below are buffer write methods

    // Insert data in post write buffer, parameters like post()
    public function post_bulk_prepare($feedid,$timestamp,$value,$arg=null)
    {
		$meta=$this->get_meta($feedid);
    	$feedid = (int) $feedid;
        $timestamp = (int) $timestamp;
        $value =  unpack("fvalue",pack("f",(float)$value))['value'];		// normalize value because of rounding

        if (!isset($this->writebuffer[$feedid])) {
            $this->writebuffer[$feedid] = "";
        }
        
        if ($this->get_npoints($feedid)>=1) {
        	static $lastvalue_static_cache = array(); // Array to hold the cache
        	if (!isset($lastvalue_static_cache[$feedid])) { // Not set, cache it from file data
        		$lastvalue_static_cache[$feedid] = $this->lastvalue($feedid);
        	}
        	if ($timestamp<=$lastvalue_static_cache[$feedid]['time'] || $lastvalue_static_cache[$feedid]['value']==$value) {
        		// if data is in past, its not supported, could call update here to fix on file before continuing
        		// if value is equal to last value also skip it
        		return $value;
        	}
        }
       	$this->writebuffer[$feedid] .= pack("If",$timestamp,$value);
       	$lastvalue_static_cache[$feedid] = array('time'=>$timestamp,'value'=>$value); // Set static cache last value
       	return $value;
    }

    // Saves post buffer to engine in bulk
    // Writing data in larger blocks saves reduces disk write load
    public function post_bulk_save()
    {
        $byteswritten = 0;
        foreach ($this->writebuffer as $feedid=>$data)
        {
            $filename = $this->dir."feed_$feedid.state";
            // Auto-correction if something happens to the datafile, it gets partitally written to
            // this will correct the file size to always be an integer number of 4 bytes.
            clearstatcache($filename);
            if (@filesize($filename)%PHPState::RECORDSIZE != 0) {
                $npoints = filesize($filename)>>3;
                $fh = fopen($filename,"c");
                fseek($fh,$npoints*PHPState::RECORDSIZE);
                fwrite($fh,$data);
                fclose($fh);
                print "PHPState: FIXED DATAFILE WITH INCORRECT LENGHT\n";
                $this->log->warn("post_bulk_save() FIXED DATAFILE WITH INCORRECT LENGHT '$filename'");
            }
            else
            {
                $fh = fopen($filename,"ab");
                fwrite($fh,$data);
                fclose($fh);
            }
            
            $byteswritten += strlen($data);
        }
        $this->writebuffer = array(); // Clear writebuffer

        return $byteswritten;
    }
        
// #### \/ Below engine public specific methods


// #### \/ Below are engine private methods    

    private function get_npoints($feedid)
    {
        $bytesize = 0;
        $filename = "feed_$feedid.state";

        if (file_exists($this->dir.$filename)) {
            clearstatcache($this->dir.$filename);
            $bytesize += filesize($this->dir.$filename);
        }
            
        if (isset($this->writebuffer[$feedid]))
            $bytesize += strlen($this->writebuffer[$feedid]);
            
        return floor($bytesize / PHPState::RECORDSIZE);
    } 


    private function fopendata($filename,$mode)
    {
        $fh = @fopen($filename,$mode);

        if (!$fh) {
            $this->log->warn("PHPTimeSeries:fopendata could not open $filename");
            return false;
        }
        
        if (!flock($fh, LOCK_EX)) {
            $this->log->warn("PHPTimeSeries:fopendata $filename locked by another process");
            fclose($fh);
            return false;
        }
        
        return $fh;
    }


    private function binarysearch($fh,$time,$filesize)
    {
        // Binary search works by finding the file midpoint and then asking if
        // the datapoint we want is in the first half or the second half
        // it then finds the mid point of the half it was in and asks which half
        // of this new range its in, until it narrows down on the value.
        // This approach usuall finds the datapoint you want in around 20
        // itterations compared to the brute force method which may need to
        // go through the whole file that may be millions of lines to find a
        // datapoint.
        $lo = 0; $hi = (int)($filesize/PHPState::RECORDSIZE)-1;
		$mid=-1;
        while ($lo <= $hi) {
            // Get the value in the middle of our range
            $mid = (int)(($hi-$lo)>>1)+$lo;
            fseek($fh,$mid*PHPState::RECORDSIZE);
            $d = fread($fh,PHPState::RECORDSIZE);
            $array = @unpack("Itime/fvalue",$d);

            $this->log->info("lo:$lo hi:$hi mid:$mid time: $time ".$array['time']." ".($time-$array['time'])."\n");

            // If it is the value we want then exit
            if ($time==$array['time']) 
            	return $mid*PHPState::RECORDSIZE;
            else if ($time<$array['time'])
            	$hi=$mid-1;
            else
            	$lo=$mid+1;
        }
        return $mid*PHPState::RECORDSIZE;
    }

    private function binarysearch_exact($fh,$time,$filesize)
    {
        if ($filesize==0) return -1;
        $start = 0; $end = $filesize-PHPState::RECORDSIZE;
        for ($i=0; $i<30; $i++)
        {
            $mid = $start + round(($end-$start)/(2*PHPState::RECORDSIZE))*PHPState::RECORDSIZE;
        	fseek($fh,$mid);
            $d = fread($fh,PHPState::RECORDSIZE);
            $array = unpack("Itime/fvalue",$d);
            if ($time==$array['time']) return $mid;
            if (($end-$start)==PHPState::RECORDSIZE) return -1;
            if ($time>$array['time']) $start = $mid; else $end = $mid;
        }
        return -1;
    }

}
