#!/usr/bin/env php
<?php
// Helper script that removes jobs from the database
require_once '/usr/share/xdmod/configuration/linker.php';

use CCR\DB;
use CCR\Log;
use CCR\DB\MySQLHelper;
use OpenXdmod\DataWarehouseInitializer;

class DbPretender
{
    public function execute($stmt, $values) {
        global $logger;

        $logger->info("Would run " . $stmt . " with " . print_r($values, true));

        return 0;
    }
}

class Expunger
{
    private $shredderDb;
    private $hpcdbDb;
    private $dwDb;

    public function __construct($pretend = false) {

        $this->shredderDb = DB::factory('shredder');
        $this->shredderHelper = MySQLHelper::factory($this->shredderDb);

        $this->hpcdbDb    = DB::factory('hpcdb');
        $this->hpcdbHelper = MySQLHelper::factory($this->hpcdbDb);

        $this->dwDb       = DB::factory('datawarehouse');
    
        $this->knownShredderTables = array();
        $this->knownHpcdbTables = array();

        if ($pretend == true) {
            $this->shredderDb = new DbPretender();
            $this->hpcdbDb = new DbPretender();
            $this->dwDb = new DbPretender();
        }
    }

    private function shredderTableExists($table) {
        if( !isset($this->knownShredderTables[$table])) {
            $this->knownShredderTables[$table] = $this->shredderHelper->tableExists($table);
        }
        return $this->knownShredderTables[$table];
    }

    private function hpcdbTableExists($table) {
        if( !isset($this->knownHpcdbTables[$table])) {
            $this->knownHpcdbTables[$table] = $this->hpcdbHelper->tableExists($table);
        }
        return $this->knownHpcdbTables[$table];
    }

    public function dodelete($whereconds, $wherevals) {

        global $logger;

        if (count($whereconds) != 4) {
            throw new Exception("Need to supply 4 where conditions");
        }

        // Clean up the shredded tables
        $tables = array('shredded_job_pbs');

        foreach ($tables as $table) {
            if( $this->shredderTableExists($table) ) {
                $logger->info("Deleting jobs from $table");
                $ndeleted = $this->shredderDb->execute("DELETE FROM $table WHERE " . $whereconds[0], $wherevals);
                $logger->info("Deleted $ndeleted jobs from $table");
            }
        }

        $tables = array('shredded_job', 'staging_job');

        foreach ($tables as $table) {
            if ($this->shredderTableExists($table)) {
                $logger->info("Deleting jobs from $table");
                $ndeleted = $this->shredderDb->execute("DELETE FROM $table WHERE " .  $whereconds[1], $wherevals);
                $logger->info("Deleted $ndeleted jobs from $table");
            }
        }

        $tables = array('hpcdb_jobs');

        foreach ($tables as $table) {
            if ($this->hpcdbTableExists($table)) {
                $logger->info("Deleting jobs from $table");
                $ndeleted = $this->hpcdbDb->execute("DELETE FROM $table WHERE " .  $whereconds[2], $wherevals);
                $logger->info("Deleted $ndeleted jobs from $table");
            }
        }

        $logger->info("Removing jobs from modw database");
        $ndeleted = $this->dwDb->execute("
DELETE modw.jobfact, modw.jobhosts, modw_supremm.process FROM modw.jobfact 
    LEFT JOIN modw.jobhosts ON modw.jobhosts.job_id = modw.jobfact.job_id 
    LEFT JOIN modw_supremm.process ON modw_supremm.process.jobid = modw.jobfact.job_id
    WHERE " . $whereconds[3], $wherevals);
        $logger->info("Deleted $ndeleted rows from modw");
        
    }
}

function deletebyfile($filename)
{
    $deleter = new Expunger();

    $fp = fopen($filename, "r");

    $whereconds = array(
        "(job_id = ? AND end = ?)",
        "(job_id = ? AND end_time = ?)",
        "(local_jobid = ? AND end_time = ?)",
        "(local_jobid = ? AND end_time_ts = ?)"
    );

    $allwheres = array(array(), array(), array(), array());
    $alldata = array();

    while (($data = fgetcsv($fp, 80)) !== false) {

        for ($i=0; $i<4; $i++) {
            $allwheres[$i][] = $whereconds[$i];
        }
        $alldata = array_merge($alldata, $data);

        if (count($alldata) > 500) {
            for($i=0; $i<4; $i++) {
                $allwheres[$i] = implode(' OR ', $allwheres[$i]);
            }
            $deleter->dodelete($allwheres, $alldata);
            $allwheres = array(array(), array(), array(), array());
            $alldata = array();
        }
    }

    if (count($alldata) > 0) {
        for($i=0; $i<4; $i++) {
            $allwheres[$i] = implode(' OR ', $allwheres[$i]);
        }
        $deleter->dodelete($allwheres, $alldata);
    }

}

function deletebytime($startDate, $endDate)
{
    $deleter = new Expunger();

    $whereconds = array(
        "end BETWEEN UNIX_TIMESTAMP(?) AND UNIX_TIMESTAMP(?)",
        "end_time BETWEEN UNIX_TIMESTAMP(?) AND UNIX_TIMESTAMP(?)",
        "end_time BETWEEN UNIX_TIMESTAMP(?) AND UNIX_TIMESTAMP(?)",
        "modw.jobfact.end_time_ts BETWEEN UNIX_TIMESTAMP(?) AND UNIX_TIMESTAMP(?)"
    );
    $deleter->dodelete($whereconds, array($startDate, $endDate));
    return;
}

function main()
{
    global $argv, $logger;

    $opts = array(
        array('h', 'help'),

        // Logging levels.
        array('v', 'verbose'),
        array('d',  'debug'),
        array('q', 'quiet'),

        array('s:', 'start-date:'),
        array('e:', 'end-date:'),
        array('j:', 'jobid:'),
        array('f:', 'file:'),
    );

    $shortOptions = implode('', array_map(function ($opt) {
        return $opt[0];
    }, $opts));
    $longOptions = array_map(function ($opt) {
        return $opt[1];
    }, $opts);

    $args = getopt($shortOptions, $longOptions);

    if ($args === false) {
        fwrite(STDERR, "Failed to parse arguments\n");
        exit(1);
    }

    $startDate = null;
    $endDate = null;
    $jobid = null;
    $file = null;

    $logLevel = -1;

    foreach ($args as $key => $value) {
        if (is_array($value)) {
            fwrite(STDERR, "Multiple values not allowed for '$key'\n");
            exit(1);
        }

        switch ($key) {
            case 'h':
            case 'help':
                $help = true;
                break;
            case 'q':
            case 'quiet':
                $logLevel = max($logLevel, Log::WARNING);
                break;
            case 'v':
            case 'verbose':
                $logLevel = max($logLevel, Log::INFO);
                break;
            case 'd':
            case 'debug':
                $logLevel = max($logLevel, Log::DEBUG);
                break;
            case 's':
            case 'start-date':
                $startDate = $value;
                break;
            case 'e':
            case 'end-date':
                $endDate = $value;
                break;
            case 'j':
            case 'jobid':
                $jobid = $value;
                break;
            case 'f':
            case 'file':
                $file = $value;
                break;
            default:
                fwrite(STDERR, "Unexpected option '$key'\n");
                exit(1);
                break;
        }
    }

    if ($logLevel === -1) {
        $logLevel = Log::NOTICE;
    }

    $conf = array(
        'file'            => false,
        'mail'            => false,
        'db'              => false,
        'consoleLogLevel' => $logLevel,
    );

    $logger = Log::factory('deletejobs', $conf);

    if ($startDate !== null && !preg_match('/^\d{4}-\d{2}-\d{2}$/', $startDate)) {
        $logger->crit("Invalid start date '$startDate'");
        exit(1);
    }

    if ($endDate !== null && !preg_match('/^\d{4}-\d{2}-\d{2}$/', $endDate)) {
        $logger->crit("Invalid end date '$endDate'");
        exit(1);
    }

    if ($startDate !== null && $endDate !== null) {
        deletebytime($startDate, $endDate);
    }

    if ($file !== null) {
        deletebyfile($file);
    }
}


try {
    main();
} catch (Exception $e) {
    do {
        $logger->crit(array(
            'message'    => $e->getMessage(),
            'stacktrace' => $e->getTraceAsString(),
        ));
    } while ($e = $e->getPrevious());
    exit(1);
}
