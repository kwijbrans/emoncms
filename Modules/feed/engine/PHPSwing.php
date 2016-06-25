<?php

function tostr($var,$index) {
	if (isset($var[$index]))
		return strval($var[$index]);
		else
			return "null";
}

class PHPSwing
{
	const RECORDSIZE=8;
    private $dir = "/var/lib/phptimeseries/";
    private $log;
    private $redis;
    
    private $writebuffer = array();

    /**
     * Constructor.
     *
     * @api
    */
    public function __construct($redis, $settings)
    {
        if (isset($settings['datadir'])) $this->dir = $settings['datadir'];
        $this->log = new EmonLogger(__FILE__);
        $this->redis=$redis;
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
    	$epsilon=floatval($options['epsilon']);
    	$percentage=floatval($options['percentage'])/100.0;
    	$this->log->info("create() feedid=$feedid epsilon=$epsilon percentage=$percentage " . trim(preg_replace('/[\r\n]/', ' ', print_r($options, true))));
    	 
    	$fh = @fopen($this->dir."feed_$feedid.swing", 'a');
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
		$fh=@fopen($this->dir."feed_$feedid.meta", 'a');
		fwrite($fh, pack("ff", $epsilon, $percentage));
		fclose($fh);
        if (file_exists($this->dir."feed_$feedid.swing") && file_exists($this->dir."feed_$feedid.meta")) return true;
        return false;
    }

    /**
     * Delete feed
     *
     * @param integer $feedid The id of the feed to be created
    */
    public function delete($feedid)
    {
        unlink($this->dir."feed_$feedid.swing");
        unlink($this->dir."feed_$feedid.meta");
        $this->redis->delete('pla:$feedid:pla'); // remove buffer
    }

    /**
     * Gets engine metadata
     *
     * @param integer $feedid The id of the feed to be created
    */
    public function get_meta($feedid)
    {
        $feedid = (int) $feedid;
        $feedname = "feed_$feedid.meta";

        if (!file_exists($this->dir.$feedname)) {
            $this->log->warn("get_meta() feed meta file does not exist '".$this->dir.$feedname."'");
            return false;
        }
            
        static $metadata_cache = array(); // Array to hold the cache
        if (isset($metadata_cache[$feedid])) {
        	return $metadata_cache[$feedid]; // Retrieve from static cache
        } else {
            // Open and read meta data file
            // The type and interval are saved as two consecutive unsigned integers
            $meta = new stdClass();
            $metafile = fopen($this->dir.$feedname, 'rb');
            $tmp = unpack("feps/fperc",fread($metafile,8)); 
            $meta->id=$feedid;
           	$meta->epsilon=$tmp['eps'];
           	$meta->percentage=$tmp['perc'];
            fclose($metafile);

            $metadata_cache[$feedid] = $meta; // Cache it
            return $meta;
        }
    }

    /**
     * Returns engine occupied size in bytes
     *
     * @param integer $feedid The id of the feed to be created
    */
    public function get_feed_size($feedid)
    {
        return filesize($this->dir."feed_$feedid.swing");
    }


    // POST OR UPDATE
    //
    // - fix if filesize is incorrect (not a multiple of PHPSwing::RECORDSIZE)
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
        
        // Get last value
        $fh = @fopen($this->dir."feed_$feedid.swing", 'rb');
        if (!$fh) {
            $this->log->warn("post() could not open data file feedid=$feedid");
            return false;
        }
        
        clearstatcache($this->dir."feed_$feedid.swing");
        $filesize = filesize($this->dir."feed_$feedid.swing");

        $csize = floor($filesize / PHPSwing::RECORDSIZE)*PHPSwing::RECORDSIZE;
        if ($csize!=$filesize) { 	// always write if file is not multiple of PHPSwing::RECORDSIZE bytes
        
            $this->log->warn("post() filesize not integer multiple of ". PHPSwing::RECORDSIZE . " bytes, correcting feedid=$feedid");
            // correct corrupt data
            fclose($fh);

            // extend file by required number of bytes
            if (!$fh = $this->fopendata($this->dir."feed_$feedid.swing", 'wb')) return false;
            fseek($fh,$csize);
            fwrite($fh, pack("If",$time,$value));
            fclose($fh);
            return $value;
        }
        $record = $this->swing($feedid, $time, $value, ($meta->epsilon+$meta->percentage*$value)); 
        if (isset($record['value'],$record['time'])) {			// append data point
//$this->log->info("post(append) feedid=$feedid time=$time value=$value epsilon=$meta->epsilon record=" . $record['time'].",".$record['value']);
        				
	        if (!$fh = $this->fopendata($this->dir."feed_$feedid.swing", 'a')) return false;
        	fwrite($fh, pack("If",$record['time'],$record['value']));
        	fclose($fh);
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
        
        if (!file_exists($this->dir."feed_$feedid.swing"))  return false;
        
        $array = false;
        $fh = fopen($this->dir."feed_$feedid.swing", 'rb');
        $filesize = filesize($this->dir."feed_$feedid.swing");
        if ($filesize>=PHPSwing::RECORDSIZE)
        {
            fseek($fh,$filesize-PHPSwing::RECORDSIZE);
            $array = unpack("Itime/fvalue",fread($fh,PHPSwing::RECORDSIZE));
        }
        fclose($fh);
//        $this->log->info("lastvalue() $feedid $filesize=(" . $array['time'].", ".$array['value'].")");
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

    private function getInterval($fh, $feedid, $time, $filesize)
    {
    	$pos = $this->binarysearch($fh,$time,$filesize);					// A[pos] >= $time
    	if ($pos >= PHPSwing::RECORDSIZE) $pos=$pos-PHPSwing::RECORDSIZE;
        fseek($fh,$pos);
	    $d = fread($fh,PHPSwing::RECORDSIZE);
    	$a0 = @unpack("Itime/fvalue",$d);
    	if ($pos >= $filesize-PHPSwing::RECORDSIZE) {
 			$rediskey='pla:'.$feedid.'pla';
    		$feeddata=$this->redis->hGetAll($rediskey);
    		if (isset($feeddata['time'])&& isset($feeddata['value']))
    			$a1=array('time' => $feeddata['time'], 'value'=>$feeddata['value']);		// get last (unwritten) point from Redis
    		else {
    			$a1=$a0;
     		}
        } else {
        	$d = fread($fh,PHPSwing::RECORDSIZE);
	       	$a1 = @unpack("Itime/fvalue",$d);
        }
	    return array('t0' => $a0['time'], 'v0'=>$a0['value'], 't1'=> $a1['time'], 'v1' => $a1['value']);
    }
    
    public function get_data($feedid,$start,$end,$interval,$skipmissing,$limitinterval, $mode=0)
    {
    	$meta=$this->get_meta($feedid);
    	$start = intval($start/1000);
        $end = intval($end/1000);
        $interval= (int) $interval;
        if ($interval == 0) {		// return real points
	        $fh = fopen($this->dir."feed_$feedid.swing", 'rb');
	        $filesize = filesize($this->dir."feed_$feedid.swing");
	        $data = array();
	        $time = 0; $i = 0;
	        $atime = 0;
	        $pos = $this->binarysearch($fh,$start,$filesize);
	        fseek($fh, $pos);
	        while ($pos >=0 && $pos < $filesize) {
	            $d = fread($fh,PHPSwing::RECORDSIZE);
	            $array = @unpack("Itime/fvalue",$d);
	            if ($array['time']<=$end) {
	            	$data[]=array($array['time']*1000,$array['value']);
	            }
	            $pos=$pos+PHPSwing::RECORDSIZE;
	        }
	        // get last point from redis
	        $rediskey='pla:'.$feedid.':pla';
	        $feeddata=$this->redis->hGetAll($rediskey);
	        if (isset($feeddata['time'],$feeddata['value']) && $feeddata['time']<=$end)
	        	$data[]=array($feeddata['time']*1000, floatval($feeddata['value']));		// get last (unwritten) point from Redis
	        		 
        } else {
            // Minimum interval
		    if ($interval<1) $interval = 1;
		    // Maximum request size
		    $req_dp = round(($end-$start) / $interval);
		    if ($req_dp>8928) return array("success"=>false, "message"=>"request datapoint limit reached (8928), increase request interval or time range, requested datapoints = $req_dp");
		        
		    $fh = fopen($this->dir."feed_$feedid.swing", 'rb');
		    $filesize = filesize($this->dir."feed_$feedid.swing");
		    $endpos = $this->binarysearch($fh, $end, $filesize+PHPSwing::RECORDSIZE);		// calculate end position to optimize search
		
		    $data = array();
		    $time = $start; $i = 0;
			$aa=$this->getInterval($fh, $feedid, $time, $endpos);							// no need to perform binary search beyond end position
			while ($time<=$end) 
		    {
		        $time = $start + ($interval * $i);
				if ($time < $aa['t0']) {			// before range
		        	$value=NULL;
		        }else if ($time <=$aa['t1']){		// in range
		        	$value=$this->interpolate($time, $aa);
		        }else {
		        	$aa=$this->getInterval($fh, $feedid, $time, $endpos);
		        	if ($time <= $aa['t1'])
		        		$value=$this->interpolate($time, $aa);
		        	else {
		        		$value=NULL;
		        		$aa['t0']=$end+1;			// end of file reached, point beyond end
		        	}
		        }
				if ($value!==null || $skipmissing===0) $data[] = array($time*1000,$value);
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

        $fh = fopen($this->dir."feed_$id.swing", 'rb');
        $filesize = filesize($this->dir."feed_$id.swing");

        $n = 0;
        $array = array("time"=>0, "value"=>0);
        while($n<10000) // max itterations
        {
            $time = $date->getTimestamp();
            if ($time>$end) break;
            
            $pos = $this->binarysearch($fh,$time,$filesize);
            fseek($fh,$pos);
            $d = fread($fh,PHPSwing::RECORDSIZE);
            
            $lastarray = $array;
            $array = unpack("Itime/fvalue",$d);
            
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

        $feedname = "feed_$feedid.swing";

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
        $localsize = floor($localsize/PHPSwing::RECORDSIZE)*PHPSwing::RECORDSIZE;
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

        $fh = fopen($this->dir."feed_$feedid.swing", 'rb');
        $filesize = filesize($this->dir."feed_$feedid.swing");

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
            $d = fread($fh,PHPSwing::RECORDSIZE);

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
        $value = (float) $value;

        $filename = "feed_$feedid.swing";
        $npoints = $this->get_npoints($feedid);

        if (!isset($this->writebuffer[$feedid])) {
            $this->writebuffer[$feedid] = "";
        }

		if (swing($feedid, $timestamp, $value, $meta->precision)) {
        	$this->writebuffer[$feedid] .= pack("If",$timestamp,$value);
		}
        return $value;
    }

    // Saves post buffer to engine in bulk
    // Writing data in larger blocks saves reduces disk write load
    public function post_bulk_save()
    {
        $byteswritten = 0;
        foreach ($this->writebuffer as $feedid=>$data)
        {
            $filename = $this->dir."feed_$feedid.swing";
            // Auto-correction if something happens to the datafile, it gets partitally written to
            // this will correct the file size to always be an integer number of 4 bytes.
            clearstatcache($filename);
            if (@filesize($filename)%PHPSwing::RECORDSIZE != 0) {
                $npoints = filesize($filename)>>3;
                $fh = fopen($filename,"c");
                fseek($fh,$npoints*PHPSwing::RECORDSIZE);
                fwrite($fh,$data);
                fclose($fh);
                print "PHPTIMESERIESPLA: FIXED DATAFILE WITH INCORRECT LENGHT\n";
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
        $filename = "feed_$feedid.swing";

        if (file_exists($this->dir.$filename)) {
            clearstatcache($this->dir.$filename);
            $bytesize += filesize($this->dir.$filename);
        }
            
        if (isset($this->writebuffer[$feedid]))
            $bytesize += strlen($this->writebuffer[$feedid]);
            
        return floor($bytesize / PHPSwing::RECORDSIZE);
    } 


    private function fopendata($filename,$mode)
    {
        $fh = @fopen($filename,$mode);

        if (!$fh) {
            $this->log->warn("fopendata could not open $filename");
            return false;
        }
        
        if (!flock($fh, LOCK_EX)) {
            $this->log->warn("fopendata $filename locked by another process");
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
        $lo = 0; $hi = (int)($filesize/PHPSwing::RECORDSIZE)-1;
		$mid=-1;
        while ($lo <= $hi) {
            // Get the value in the middle of our range
            $mid = (int)(($hi-$lo)>>1)+$lo;
            fseek($fh,$mid*PHPSwing::RECORDSIZE);
            $d = fread($fh,PHPSwing::RECORDSIZE);
            $array = @unpack("Itime/fvalue",$d);
            // If it is the value we want then exit
			if ($array['time']>=$time)
            	$hi=$mid-1;
            else
            	$lo=$mid+1;
        }
        return $lo*PHPSwing::RECORDSIZE;
    }

    private function binarysearch_exact($fh,$time,$filesize)
    {
        if ($filesize==0) return -1;
        $start = 0; $end = $filesize-PHPSwing::RECORDSIZE;
        for ($i=0; $i<30; $i++)
        {
            $mid = $start + round(($end-$start)/(2*PHPSwing::RECORDSIZE))*PHPSwing::RECORDSIZE;
            fseek($fh,$mid);
            $d = fread($fh,PHPSwing::RECORDSIZE);
            $array = unpack("Itime/fvalue",$d);
            if ($time==$array['time']) return $mid;
            if (($end-$start)==PHPSwing::RECORDSIZE) return -1;
            if ($time>$array['time']) $start = $mid; else $end = $mid;
        }
        return -1;
    }

    // interpolate
    //
    // - determine value at time $time given t0, v0, t1, v1
    
    private function interpolate($time, $aa)
    {
    	//    	$this->log->info("get_interpolate($time," . tostr($aa,'t0') . "," . tostr($aa,'v0') .", ". tostr($aa,'t1') . "," . tostr($aa,'v1'));
    	if (isset($aa['t0']) && isset($aa['v0']) && isset($aa['t1']) && isset($aa['v1']) && $aa['t0'] != $aa['t1']) {
    		return ($aa['v0'] + ($aa['v1']-$aa['v0'])*($time-$aa['t0'])/($aa['t1']-$aa['t0']));
    	}
    	// return no value if interpolation cannot be calculated
    	return NULL;
    }
    
    private function swing($feedid, $time, $value, $epsilon) {
    	$result=NULL;
    	// first get data from the cache
    	$rediskey='pla:'.$feedid.':pla';
    	$feeddata=$this->redis->hGetAll($rediskey);
//$this->log->info("swing($feedid, $time, $value, $epsilon)" . trim(preg_replace('/[\r\n]/', ' ', print_r($feeddata, true))) );
    	 
    	// if no data in redis try to get it from the file
    	if (!isset($feeddata['segtime'], $feeddata['segvalue'])) {
    		// no start of segment set in cache, read last value from file
    		$this->redis->delete($rediskey);			// clear cache
    		$last = $this->lastvalue($feedid);
//$this->log->warn("swing(last) ". $last['time'].", ". $last['value'] );
    		if (isset($last['time']) && isset($last['value']) && !is_nan(last['value'])) {
    			$feeddata=array('segtime'=>$last['time'], 'segvalue' => $last['value']);
    		}
    	}
    
    	// $value is valid
    	if (!is_nan($value)) {
    		// if no data in redis register the first point
    		if (!isset($feeddata['segtime'], $feeddata['segvalue'])) {				// first segment, write this entry and set first entry
    			$feeddata=array('segtime'=>$time, 'segvalue' => $value);
    			$this->redis->delete($rediskey);			// clear cache
    			$this->redis->hMSet($rediskey, $feeddata);  // add segment start to cache
    			$result = array('time' => $time, 'value' => $value);						// write this segment
    		} else if ($time > $feeddata['segtime'] &&
    				!isset($feeddata['time'], $feeddata['upper'], $feeddata['lower'], $feeddata['value'])) {
    			// if first datapoint in redis but not second (i.e., any part missing) register the second point
    			$feeddata['time']=$time;
    			$feeddata['value']=$value;
    			$feeddata['lower']=($value - $epsilon - $feeddata['segvalue']) / ($time - $feeddata['segtime']);
    			$feeddata['upper']=($value + $epsilon - $feeddata['segvalue']) / ($time - $feeddata['segtime']);
//$this->log->info("swing-2nd($feedid, $time, $value, $epsilon)" . trim(preg_replace('/[\r\n]/', ' ', print_r($feeddata, true))) );
    			$this->redis->hMSet($rediskey, $feeddata);  // add segment start to cache
    		} else if ($time > $feeddata['segtime'] && $time > $feeddata['time']) {
    			// check whether new point is in bounds of upper and lower boundary.
    			$upper = $feeddata['segvalue']+($time - $feeddata['segtime'])*$feeddata['upper'];
    			$lower = $feeddata['segvalue']+($time - $feeddata['segtime'])*$feeddata['lower'];
    			if ( ($value > $upper + $epsilon) ||
    				 ($value < $lower - $epsilon) ) {
    				// value outside the boundaries. Start new segment
    				$this->redis->delete($rediskey);			// clear cache
    				$rc = min($feeddata['upper'], max($feeddata['lower'], ($feeddata['value']-$feeddata['segvalue'])/($feeddata['time']-$feeddata['segtime'])));
    				$feeddata['segvalue']=$feeddata['segvalue']+$rc*($feeddata['time']-$feeddata['segtime']);		// interpolate last point
    				$feeddata['segtime'] = $feeddata['time'];
    				$feeddata['lower']=($value - $epsilon - $feeddata['segvalue']) / ($time - $feeddata['segtime']);
    				$feeddata['upper']=($value + $epsilon - $feeddata['segvalue']) / ($time - $feeddata['segtime']);
    				$result = array('time' => $feeddata['segtime'], 'value' => $feeddata['segvalue']);		// indicate to write the current data point to the file
//$this->log->info("swing-new($feedid, $time, $value, $epsilon) rc=$rc feeddata=" . trim(preg_replace('/[\r\n]/', ' ', print_r($feeddata, true)))."result=" . trim(preg_replace('/[\r\n]/', ' ', print_r($result, true))) );
    				 } else {
	    			// If necessary, shift upper and lower boundary
	    			if ($value > $lower + $epsilon) {
	    				$feeddata['lower']=($value - $epsilon - $feeddata['segvalue']) / ($time - $feeddata['segtime']);
	    				//	    			$this->log->info("swing(up) feedid=$feedid time=$time value=$value epsilon=$epsilon feeddata(tk,vk,t,v,u,l)=(".
	    				//	    					tostr($feeddata['segtime']).", ". tostr($feeddata['segvalue']) .", ".tostr($feeddata['time']).", ". tostr($feeddata['value']) .", ".tostr($feeddata['upper']) .", ".tostr($feeddata['lower']).")");
	    			}
	    			if ($value < $upper - $epsilon) {
	    				$feeddata['upper']=($value + $epsilon - $feeddata['segvalue']) / ($time - $feeddata['segtime']);
	    				//	    			$this->log->info("swing(down) feedid=$feedid time=$time value=$value epsilon=$epsilon feeddata(tk,vk,t,v,u,l)=(".
	    				//	    					tostr($feeddata['segtime']).", ". tostr($feeddata['segvalue']) .", ".tostr($feeddata['time']).", ". tostr($feeddata['value']) .", ".tostr($feeddata['upper']) .", ".tostr($feeddata['lower']).")");
	    			}
//$this->log->info("swing-updown($feedid, $time, $value, $epsilon)" . trim(preg_replace('/[\r\n]/', ' ', print_r($feeddata, true))) );
    			}	
	    		$feeddata['time']=$time;
	    		$feeddata['value']=$value;
    			$this->redis->hMSet($rediskey, $feeddata);  // add segment start to cache
    		}
    	} else {
    		$this->redis->delete($rediskey);			// clear cache
    		if (isset($feeddata['segvalue'], $feeddata['segtime'], $feeddata['time'], $feeddata['value'])) {			// $value = NaN (i.e., getNull, close segment)
	    		$rc = min($feeddata['upper'], max($feeddata['lower'], ($feeddata['value']-$feeddata['segvalue'])/($feeddata['time']-$feeddata['segtime'])));
	    		$feeddata['segtime'] = $feeddata['time'];
	    		$feeddata['segvalue']=$feeddata['segvalue']+$rc*($feeddata['time']-$feeddata['segtime']);		// interpolate last point
	    		$result = array('time' => $feeddata['segtime'], 'value' => $feeddata['segvalue']);		// indicate to write the current data point to the file
    	   	}
    	}
//$this->log->info("swing-end($feedid, $time, $value, $epsilon) feeddata=" . trim(preg_replace('/[\r\n]/', ' ', print_r($feeddata, true)))."result=" . trim(preg_replace('/[\r\n]/', ' ', print_r($result, true))) );
    	return $result;
    }
    
    
    
}
