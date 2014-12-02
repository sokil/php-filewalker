<?php

class FileWalker
{
    public function iterateForked($filename, $callable, $workerNumber = 8, $chunkLength = 50)
    {
        define('MIN_CHUNK_SIZE', 1);
        define('CHUNK_FILENAME_PREFIX', '/tmp/chunk');

        // get length of id list
        $lineListLength = (int) exec('wc -l ' . $filename);

        // do not split file if list length less than MIN_CHUNK_SIZE ids per chunk
        if($lineListLength < MIN_CHUNK_SIZE * $workerNumber) {
            $this->iterate($filename, $callable, $chunkLength);
            return;
        }

        // get number of lines per worker
        $lineLengthPerWorker = ceil($lineListLength / $workerNumber);

        // get real worker number
        $workerNumber = ceil($lineListLength / $lineLengthPerWorker);

        // split file to chunks - one chunk per worker
        exec('rm -rf ' . CHUNK_FILENAME_PREFIX . '*');
        exec(sprintf(
            'split -a 3 -d -l %s %s %s',
            $lineLengthPerWorker,
            $filename,
            CHUNK_FILENAME_PREFIX
        ));


        // fork workers
        $workerPidList = array();
        for($workerId = 0; $workerId < $workerNumber; $workerId++) {
            $pid = pcntl_fork();
            if(-1 === $pid) {
                die('Error while forking new worker');
            } elseif(0 === $pid) {
                // child
                $chunkFilename = CHUNK_FILENAME_PREFIX . str_pad($workerId, 3, '0', STR_PAD_LEFT);
                $this->iterate($chunkFilename, $callable, $chunkLength);
                return;
            } else {
                // parent
                $workerPidList[] = $pid;
            }
        }

        // monitore status of childs
        $stderr = fopen('php://stderr', 'w+');
        while(count($workerPidList)) {
            foreach($workerPidList as $workerId => $pid) {
                $status = null;
                $result = pcntl_waitpid($pid, $status, WNOHANG);

                if(-1 == $result || 0 == $result) {
                    unset($workerPidList[$workerId]);
                    fputs(
                        $stderr,
                        'Child process #' . $workerId . ' with pid ' . $pid . ' exited with code ' . pcntl_wexitstatus($status) . PHP_EOL
                    );
                }
            }
        }
        fclose($stderr);

    }

    public function iterate($filename, $callable, $chunkLength = 50)
    {
        if(!file_exists($filename)) {
            throw new \Exception('File ' . $filename . ' not found');
        }

        $fh = fopen($filename, 'r');
        while(!feof($fh)) {

            // get chunk of ids
            $linesChunk = array();
            for($i = 0; $i < $chunkLength; $i++) {

                // get chunk of users
                $line = fgets($fh);
                if(false === $line) {
                    break;
                }

                $linesChunk[] = trim($line);
            }

            if(!$linesChunk) {
                break;
            }

            call_user_func($callable, $linesChunk);
        }

        fclose($fh);
    }
}
